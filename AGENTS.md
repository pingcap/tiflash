# Repo Notes

## Local Build/Test

- Prefer `./cmake-build-debug/dbms/gtests_dbms` for targeted local gtest runs.
- When building `gtests_dbms` locally, reuse a Codex-owned ccache directory to avoid permission issues and keep later incremental builds fast:
  `env CCACHE_DIR=$HOME/.codex/memories/ccache CCACHE_TEMPDIR=/tmp/ccache-tmp cmake --build cmake-build-debug --target gtests_dbms --parallel 16`
- Reuse the same `CCACHE_DIR` for later incremental `gtests_dbms` builds.
