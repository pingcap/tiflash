## A known design flaw between TiFlash DDL sync and RaftLog decoding

- Unlike TiKV, TiFlash must decode row values into columns before writing to its columnar engine, so decoding depends on correct schema.
- TiFlash does **not** enforce strong DDL consistency between TiDB and TiFlash. Instead, it relies on **schema mismatch detection** during RaftLog decode or query execution.
- On mismatch, TiFlash **syncs the latest schema** for the table and retries decode/query. A background task also syncs schema on a fixed interval.
- When decoding with its current schema, TiFlash uses a **best‑effort** strategy; if it still cannot decode after schema sync, it may fail fast(TiFlash panic).

## Why did TiFlash choose weak DDL sync design?

A "strong consistency" mechanism for DDL sync is expensive. SchemaDiffs (TableInfo changes) and DML Raft logs live in **different Regions**, and their arrival order is not guaranteed for a Raft learner. Validating SchemaDiffs for every Raft entry would introduce prohibitive latency and scalability costs. We must leverage additional information beside the RaftLog to mitigate this overhead.
Having TiDB push DDL changes to TiFlash with strong consistency is also problematic: if TiFlash is unavailable or network partitioned, TiDB latency would increase and violate the design principle that TiFlash failures should not impact the OLTP path. Having a weak push between TiDB to TiFlash, the SchemaDiffs can arrive after Raft entries encoded with the new schema.
Unlike TiCDC that only need to handle RaftLog after a specified commit_ts. TiFlash also has to **decode old data with the latest schema** when it first builds a replica (e.g. tiflash/issues/8419) or Region transferred between instances. SchemaDiff history may be GC‑ed, so decoding cannot rely on a complete diff chain.
Given these constraints, TiFlash chose: **decode with current schema → detect mismatch → sync → retry**. This has been workable, but only if mismatch detection is conservative enough.
