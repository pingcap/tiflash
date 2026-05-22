// Local replacement for cloud-storage-engine's generated workspace-hack.
//
// The upstream hakari crate intentionally aggregates a large feature set for
// standalone cloud-storage-engine binaries, including tikv-jemalloc-* with
// unprefixed malloc/free symbols. tiflash-columnar-hub is built as a cdylib
// and loaded into the TiFlash process, so inheriting that behavior causes the
// shared object to embed another allocator implementation and crash at runtime.
