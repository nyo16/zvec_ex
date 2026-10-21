# Makefile for zvec NIF
#
# Two stages:
#   1. Clone & build zvec from source (if not already done)
#   2. Compile c_src/zvec_nif.cpp and link against zvec static libs

# elixir_make provides these:
#   MIX_APP_PATH, ERTS_INCLUDE_DIR, FINE_INCLUDE_DIR
PRIV_DIR = $(MIX_APP_PATH)/priv
NIF_SO   = $(PRIV_DIR)/libzvec_nif.so

# zvec paths
ZVEC_VERSION = v0.8.0
ZVEC_SRC     = c_src/zvec_vendor
# Stamp recording which zvec release is checked out; bumping ZVEC_VERSION
# forces a fresh clone instead of silently reusing a stale vendor tree.
ZVEC_STAMP   = $(ZVEC_SRC)/.zvec-$(ZVEC_VERSION)
ZVEC_BUILD   = $(ZVEC_SRC)/build
ZVEC_LIB     = $(ZVEC_BUILD)/lib
ZVEC_EXT_LIB = $(ZVEC_BUILD)/external/usr/local/lib
ZVEC_INC     = $(ZVEC_SRC)/src/include
ZVEC_SRC_INC = $(ZVEC_SRC)/src
ZVEC_EXT_INC = $(ZVEC_BUILD)/external/usr/local/include

# Compiler
CXX ?= c++
CXXFLAGS = -std=c++17 -O2 -fPIC -fvisibility=hidden -Wall -Wextra -Wno-unused-parameter

# Include paths
CXXFLAGS += -I$(ERTS_INCLUDE_DIR) -I$(FINE_INCLUDE_DIR)
CXXFLAGS += -I$(ZVEC_INC) -I$(ZVEC_SRC_INC) -I$(ZVEC_EXT_INC)
# antlr4 runtime headers
CXXFLAGS += -I$(ZVEC_SRC)/thirdparty/antlr/antlr4/runtime/Cpp/runtime/src
# sparsehash headers
CXXFLAGS += -I$(ZVEC_SRC)/thirdparty/sparsehash/sparsehash-2.0.4/src
# magic_enum headers
CXXFLAGS += -I$(ZVEC_SRC)/thirdparty/magic_enum/magic_enum-0.9.7/include

# First-party static libs (force-load to preserve static constructors for algorithm registration)
# NOTE: libzvec.a (the db layer) and libzvec_core.a are built with GLOB_RECURSE and
# already contain all sub-library objects (zvec_common, zvec_index, zvec_sqlengine,
# core_knn_*, etc.), so we only force-load the top-level aggregates to avoid
# duplicate symbols.
ZVEC_FORCE_LIBS = \
	$(ZVEC_LIB)/libzvec.a \
	$(ZVEC_LIB)/libzvec_core.a \
	$(ZVEC_LIB)/libzvec_turbo.a \
	$(ZVEC_LIB)/libzvec_ailego.a

# Third-party static libs (FTS: FastPFOR, snowball, utf8proc; storage: rocksdb, arrow)
THIRDPARTY_LIBS = \
	$(ZVEC_LIB)/libFastPFOR.a \
	$(ZVEC_LIB)/libsnowball.a \
	$(ZVEC_LIB)/libutf8proc.a \
	$(ZVEC_EXT_LIB)/librocksdb.a \
	$(ZVEC_EXT_LIB)/libparquet.a \
	$(ZVEC_EXT_LIB)/libarrow_acero.a \
	$(ZVEC_EXT_LIB)/libarrow_compute.a \
	$(ZVEC_EXT_LIB)/libarrow.a \
	$(ZVEC_EXT_LIB)/libarrow_bundled_dependencies.a \
	$(ZVEC_EXT_LIB)/libantlr4-runtime.a \
	$(ZVEC_EXT_LIB)/libglog.a \
	$(ZVEC_EXT_LIB)/libgflags_nothreads.a \
	$(ZVEC_EXT_LIB)/libroaring.a \
	$(ZVEC_EXT_LIB)/liblz4.a

# Don't let the user's shell LDFLAGS / CXXFLAGS / CFLAGS leak into the
# CMake sub-build (cmake reads them at configure time and bakes them into
# every link command — e.g. a shell `LDFLAGS=-L...` plus our reassignment
# below would produce `-shared` link lines for protoc and friends).
unexport LDFLAGS
unexport CXXFLAGS
unexport CFLAGS
unexport CPPFLAGS

# Platform-specific linking for the FINAL NIF .so link only (intentionally
# not named LDFLAGS so it can never be exported to the cmake sub-build).
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
  # macOS: -force_load for static constructor registration, -undefined dynamic_lookup for BEAM symbols
  NIF_LDFLAGS = -shared -flat_namespace -undefined dynamic_lookup
  FORCE_LOAD_FLAGS = $(foreach lib,$(ZVEC_FORCE_LIBS),-Wl,-force_load,$(lib))
  SYS_LIBS = -lc++ -lz -lbz2
else
  # Linux: --whole-archive for static constructors
  NIF_LDFLAGS = -shared
  FORCE_LOAD_FLAGS = -Wl,--whole-archive $(ZVEC_FORCE_LIBS) -Wl,--no-whole-archive
  SYS_LIBS = -lstdc++ -lz -lbz2 -lpthread -ldl -lrt
endif

# CMake flags for building zvec
CMAKE_FLAGS = \
	-DCMAKE_POSITION_INDEPENDENT_CODE=ON \
	-DBUILD_PYTHON_BINDINGS=OFF \
	-DBUILD_C_BINDINGS=OFF \
	-DBUILD_TOOLS=OFF \
	-DBUILD_ZVEC_SHARED=OFF \
	-DBUILD_ZVEC_AILEGO_SHARED=OFF \
	-DBUILD_ZVEC_CORE_SHARED=OFF \
	-DCMAKE_BUILD_TYPE=Release \
	-DCMAKE_POLICY_VERSION_MINIMUM=3.5

.PHONY: all clean

all: $(NIF_SO)

# Stage 1: Clone the pinned zvec release (re-clones when ZVEC_VERSION changes)
$(ZVEC_STAMP):
	rm -rf $(ZVEC_SRC)
	git clone --branch $(ZVEC_VERSION) --recurse-submodules --depth 1 \
		https://github.com/alibaba/zvec.git $(ZVEC_SRC)
	touch $@

# Stage 1b: CMake configure (upstream applies its own antlr4 CMake-policy patch)
$(ZVEC_BUILD)/Makefile: $(ZVEC_STAMP)
	mkdir -p $(ZVEC_BUILD) && \
	cd $(ZVEC_BUILD) && \
	cmake .. $(CMAKE_FLAGS)

# Stage 1c: CMake build (only runs once, stamp file tracks completion).
# Only the static libraries the NIF links are built; zvec's own test suite is skipped.
$(ZVEC_BUILD)/.built: $(ZVEC_BUILD)/Makefile
	cmake --build $(ZVEC_BUILD) --target zvec zvec_core zvec_turbo zvec_ailego \
		-j$$(sysctl -n hw.ncpu 2>/dev/null || nproc)
	touch $@

# Stage 2: Compile and link NIF
$(NIF_SO): c_src/zvec_nif.cpp $(ZVEC_BUILD)/.built
	@mkdir -p $(PRIV_DIR)
	$(CXX) $(CXXFLAGS) $(NIF_LDFLAGS) \
		c_src/zvec_nif.cpp \
		$(FORCE_LOAD_FLAGS) \
		$(THIRDPARTY_LIBS) \
		$(SYS_LIBS) \
		-o $(NIF_SO)

clean:
	rm -f $(NIF_SO)
	rm -rf $(ZVEC_BUILD)
