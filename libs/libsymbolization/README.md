# Symbolization
Symbolize [AVMAs (actual virtual memory addresses)](https://docs.rs/findshlibs/latest/findshlibs/#addresses).

## Why this library?
There are existing ways to get backtrace symbols:

- `backtrace_symbols(...)`
- `absl::Symbolize(...)`

However, they do not directly provide debug info. Using these libs, we need to manually implement address-to-line utility.
Or we can port CH's `Stacktrace`, but we also want to support MACH-O in the future as there are on-going works at `pprof-rs`.
With all these requirements, `backtrace-rs` stands out. So we implement this library to port functions to
C++ to help us symbolize avma for backtrace purpose (e.g. program counters).