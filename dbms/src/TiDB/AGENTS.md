# TiFlash TiDB Integration Guide

This directory handles TiFlash's integration with the TiDB ecosystem, specifically schema synchronization, data decoding, and TiDB-compatible collations.

## 📂 Key Components

- **`Schema/`**: The most critical part of this directory. It manages how TiFlash stays in sync with TiDB's schema changes.
  - `TiDBSchemaSyncer`: Logic for fetching schema diffs from TiKV/PD.
  - `SchemaSyncService`: Background service that triggers periodic schema syncs.
  - `SchemaBuilder`: Constructs TiFlash-internal storage structures based on TiDB schema info.
- **`Decode/`**: Logic for decoding TiDB's datum and row formats into TiFlash's columnar format.
- **`Collation/`**: Implementation of TiDB-compatible collations to ensure consistent sorting and filtering behavior.
- **`Etcd/`**: Utilities for interacting with Etcd, primarily for leader election (Owner) and service discovery.

## 🧪 Testing

TiDB integration tests focus on schema synchronization and decoding accuracy.

### Running TiDB Integration Tests
- **Schema Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="*Schema*"
  ```
- **Decoding Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="*Decode*"
  ```
- **Collation Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="*Collation*"
  ```

## 🛠 Coding Patterns

- **TiDB Types**: Be aware of TiDB-specific types defined in `TiDBTypes.h` and handled in `dbms/src/Core/Types.h`.
- **Protobufs**: Extensive use of `tipb` (TiDB Protocol Buffers) for schema and metadata.
- **Error Handling**: Schema sync errors are critical. Use `DB::Exception` and ensure proper logging for troubleshooting "stale schema" issues.
- **Logging**: Use `LoggerPtr` with context. Schema sync logs should include `table_id` or `schema_version` where relevant.

## 💡 Key Concepts
- **Schema Versioning**: TiDB uses a global schema version. TiFlash must track this version to ensure query consistency.
- **Mapped Names**: TiFlash often stores data with modified names/structures compared to TiDB. Refer to `SchemaNameMapper.h` for how these mappings are handled.
