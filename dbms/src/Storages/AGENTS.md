# TiFlash Storage Engine Guide

This directory contains the core storage engine implementations of TiFlash.

## 📂 Key Components

- **`DeltaMerge/`**: The main columnar storage engine.
  - `DeltaMergeStore`: Entry point for reading and writing data.
  - `Segment`: Data is divided into segments for management.
  - `StableValueSpace` & `DeltaValueSpace`: Storage for stable data and delta updates.
- **`KVStore/`**: Manages Raft log synchronization and TiKV interactions.
  - `KVStore`: Manages regions and Raft peers.
  - `Region`: Represents a Raft region.
  - `TMTContext`: Context for TiFlash Multi-Raft.
- **`Page/`**: `PageStorage` provides a persistent KV-like store for WAL and metadata.
- **`S3/`**: Integration with S3-compatible object storage for disaggregated architecture.

## 🧪 Testing

Storage engine tests are critical and often use `gtests_dbms`.

### Running Storage Tests
- **DeltaMerge Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="*DeltaMerge*"
  ```
- **PageStorage Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="*PageStorage*"
  ```
- **KVStore Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="*KVStore*"
  ```

### Failpoints & Syncpoints
Storage tests heavily rely on failpoints to simulate crashes or specific race conditions.
- Search for `FAIL_POINT_TRIGGER_EXCEPTION` or `FAIL_POINT_PAUSE` in the code.
- Use `SyncPointCtl` to coordinate threads in tests.

## 🛠 Coding Patterns

- **Shared Pointers**: Use `std::shared_ptr` for `StorageDeltaMerge`, `DeltaMergeStore`, `Region`, and `Context`.
- **Concurrency**:
  - `BackgroundProcessingPool`: Used for background tasks like merge/compaction/GC.
  - Always consider thread safety when modifying `Segment` or `KVStore` state.
- **Logging**: Use `LoggerPtr` and `LOG_INFO(log, ...)` with relevant context (e.g., `region_id`, `table_id`).
- **Error Handling**: Use `DB::Exception` with appropriate error codes from `ErrorCodes.cpp`.

## 📖 Recommended Reading
- `dbms/src/Storages/DeltaMerge/README.md` (if exists)
- Design docs in `docs/design/` related to DeltaMerge and Disaggregated architecture.
