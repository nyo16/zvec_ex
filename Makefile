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
ZVEC_SRC     = c_src/zvec_vendor
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
# NOTE: zvec_db.a and zvec_core.a are built with GLOB_RECURSE and already contain
# all sub-library objects (zvec_common, zvec_index, zvec_sqlengine, core_knn_*, etc.)
# so we only force-load the top-level aggregates to avoid duplicate symbols.
ZVEC_FORCE_LIBS = \
	$(ZVEC_LIB)/libzvec_db.a \
	$(ZVEC_LIB)/libzvec_core.a \
	$(ZVEC_LIB)/libzvec_ailego.a

# Third-party static libs
THIRDPARTY_LIBS = \
	$(ZVEC_EXT_LIB)/librocksdb.a \
	$(ZVEC_EXT_LIB)/libarrow.a \
	$(ZVEC_EXT_LIB)/libarrow_compute.a \
	$(ZVEC_EXT_LIB)/libarrow_acero.a \
	$(ZVEC_EXT_LIB)/libarrow_dataset.a \
	$(ZVEC_EXT_LIB)/libparquet.a \
	$(ZVEC_EXT_LIB)/libarrow_bundled_dependencies.a \
	$(ZVEC_EXT_LIB)/libantlr4-runtime.a \
	$(ZVEC_EXT_LIB)/libprotobuf.a \
	$(ZVEC_EXT_LIB)/libglog.a \
	$(ZVEC_EXT_LIB)/libgflags_nothreads.a \
	$(ZVEC_EXT_LIB)/libroaring.a \
	$(ZVEC_EXT_LIB)/liblz4.a

# Platform-specific linking
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
  # macOS: -force_load for static constructor registration, -undefined dynamic_lookup for BEAM symbols
  LDFLAGS = -shared -flat_namespace -undefined dynamic_lookup
  FORCE_LOAD_FLAGS = $(foreach lib,$(ZVEC_FORCE_LIBS),-Wl,-force_load,$(lib))
  SYS_LIBS = -lc++ -lz -lbz2
else
  # Linux: --whole-archive for static constructors
  LDFLAGS = -shared
  FORCE_LOAD_FLAGS = -Wl,--whole-archive $(ZVEC_FORCE_LIBS) -Wl,--no-whole-archive
  SYS_LIBS = -lstdc++ -lz -lbz2 -lpthread -ldl -lrt
endif

# CMake flags for building zvec
CMAKE_FLAGS = \
	-DCMAKE_POSITION_INDEPENDENT_CODE=ON \
	-DBUILD_PYTHON_BINDINGS=OFF \
	-DBUILD_TOOLS=OFF \
	-DCMAKE_BUILD_TYPE=Release \
	-DCMAKE_POLICY_VERSION_MINIMUM=3.5

.PHONY: all clean

all: $(NIF_SO)

# Stage 1: Clone zvec if needed
$(ZVEC_SRC)/CMakeLists.txt:
	git clone --recurse-submodules --depth 1 \
		https://github.com/alibaba/zvec.git $(ZVEC_SRC)

# Stage 1b: Patch antlr4 CMakeLists for modern CMake compatibility
$(ZVEC_SRC)/.patched: $(ZVEC_SRC)/CMakeLists.txt
	@if grep -q "CMP0054 OLD" $(ZVEC_SRC)/thirdparty/antlr/antlr4/runtime/Cpp/CMakeLists.txt 2>/dev/null; then \
		sed -i.bak \
			-e '/CMP0045 OLD/d' \
			-e 's/CMP0054 OLD/CMP0054 NEW/' \
			-e '/CMP0059 OLD/d' \
			-e '/CMP0042 OLD/d' \
			-e 's/CMP0042 OLD/CMP0042 NEW/' \
			$(ZVEC_SRC)/thirdparty/antlr/antlr4/runtime/Cpp/CMakeLists.txt; \
	fi
	touch $@

# Stage 1c: CMake configure
$(ZVEC_BUILD)/Makefile: $(ZVEC_SRC)/.patched
	mkdir -p $(ZVEC_BUILD) && \
	cd $(ZVEC_BUILD) && \
	cmake .. $(CMAKE_FLAGS)

# Stage 1d: CMake build (only runs once, stamp file tracks completion)
$(ZVEC_BUILD)/.built: $(ZVEC_BUILD)/Makefile
	cmake --build $(ZVEC_BUILD) -j$$(sysctl -n hw.ncpu 2>/dev/null || nproc)
	touch $@

# Stage 2: Compile and link NIF
$(NIF_SO): c_src/zvec_nif.cpp $(ZVEC_BUILD)/.built
	@mkdir -p $(PRIV_DIR)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) \
		c_src/zvec_nif.cpp \
		$(FORCE_LOAD_FLAGS) \
		$(THIRDPARTY_LIBS) \
		$(SYS_LIBS) \
		-o $(NIF_SO)

clean:
	rm -f $(NIF_SO)
	rm -rf $(ZVEC_BUILD)
