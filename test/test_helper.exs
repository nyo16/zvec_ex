# RaBitQ indexes only exist in Linux x86_64 builds of zvec; the Linux CI job
# runs them with `mix test --include rabitq`.
ExUnit.start(exclude: [:rabitq])
