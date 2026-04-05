# Mark and MinMax Cache Eviction HTTP API

Purpose: propose a small operational API that clears the node-local `MarkCache`
and `MinMaxIndexCache` on demand, primarily for controlled experiments,
diagnostics, and cache-state reset during disaggregated read investigations.

Date: 2026-04-05

## Summary

Add two TiFlash HTTP endpoints under `ProxyFFIStatusService`:

- `/tiflash/cache/evict/mark`
- `/tiflash/cache/evict/minmax`

Each endpoint clears one node-local cache by calling the existing `Context`
cache-drop methods:

- `Context::dropMarkCache()`
- `Context::dropMinMaxIndexCache()`

The proposal intentionally keeps the first version narrow:

- node-local scope only
- no table-level or key-level eviction
- no extra query parameters
- no new cache implementation or refactor

This is the simplest production-safe design because both caches are already
owned by `Context`, already support `reset()`, and the existing `LRUCache`
semantics are compatible with concurrent readers that still hold cached values.

## Context

### Current state

TiFlash already exposes several operational HTTP endpoints from
`ProxyFFIStatusService`, including readiness probes and remote cache eviction:

- `dbms/src/Storages/KVStore/FFI/ProxyFFIStatusService.cpp:839`

The service has direct access to the global TiFlash context through
`EngineStoreServerWrap` / `TMTContext`, so it is a natural location for
small node-local management actions.

The two caches in scope are already globally owned by `Context`:

- `MarkCache`
  - definition: `dbms/src/Storages/MarkCache.h:64`
  - `Context` API: `dbms/src/Interpreters/Context.h:394`
  - getter implementation: `dbms/src/Interpreters/Context.cpp:1208`
  - drop implementation: `dbms/src/Interpreters/Context.cpp:1215`
- `MinMaxIndexCache`
  - definition: `dbms/src/Storages/DeltaMerge/Index/MinMaxIndex.h:158`
  - `Context` API: `dbms/src/Interpreters/Context.h:398`
  - getter implementation: `dbms/src/Interpreters/Context.cpp:1233`
  - drop implementation: `dbms/src/Interpreters/Context.cpp:1239`

Both drop methods already reset the corresponding cache instance rather than
destroying and recreating the whole cache object.

### Why this API is needed

Remote-cache eviction alone is not sufficient for disaggregated read
experiments. Mark and min-max metadata may stay warm even after remote cache is
cleared, which makes it difficult to reproduce a fully cold metadata path.

Today there is no dedicated operational endpoint to clear these caches without
restarting TiFlash or relying on more invasive test-only machinery.

### Verified cache reset semantics

The proposal depends on `LRUCache::reset()` being safe while concurrent queries
still hold cached values.

Relevant implementation details:

- cached values are stored as `std::shared_ptr<Mapped>`
  - `dbms/src/Common/LRUCache.h:52`
- `reset()` clears internal registry state under the cache mutex
  - `dbms/src/Common/LRUCache.h:186`
- `getOrSet()` already explicitly handles concurrent `reset()` on
  `insert_tokens`
  - `dbms/src/Common/LRUCache.h:138`

This means:

- in-flight readers that already obtained a cached value keep owning their
  `shared_ptr`
- new readers observe a cold-cache view after reset
- internal cache structures remain synchronized by the same mutex used for
  `get`, `set`, `getOrSet`, `remove`, and `reset`

Therefore node-local cache eviction is compatible with concurrent query
execution and does not require stopping traffic first.

## Goals

- Provide a simple node-local API to clear `MarkCache` on demand.
- Provide a simple node-local API to clear `MinMaxIndexCache` on demand.
- Keep the semantics safe for concurrent readers that already hold cached
  values.
- Reuse existing `Context` and cache-reset logic instead of inventing a new
  eviction mechanism.
- Make the API shape extensible for future cache types.

## Non-Goals

- No table-level, region-level, or key-level eviction.
- No cluster-wide fan-out or cross-node coordination.
- No new cache implementation, ownership redesign, or lifecycle refactor.
- No authentication or authorization redesign in this proposal.
- No automatic background eviction or policy-based eviction changes.
- No attempt to preserve internal cache hit/miss statistics across reset.

## Compatibility and Invariants

- The API is node-local and only affects the TiFlash instance that receives the
  HTTP request.
- Existing queries that already obtained cached values continue using those
  values safely.
- Queries that look up the cache after eviction observe a miss and rebuild the
  cache entry through the normal read path.
- The API must not destroy or replace `Context` ownership of the cache object;
  it should only call the existing drop/reset path.
- The API returns success even if the target cache is not enabled; in that
  case the operation is a no-op.

## Design

### API shape

Add two new endpoints:

- `/tiflash/cache/evict/mark`
- `/tiflash/cache/evict/minmax`

The `cache/evict/<type>` pattern is preferred over ad-hoc endpoint names
because it leaves a clean extension path for future node-local cache eviction
APIs such as `remote`, `column`, or `vector`.

### Request and response behavior

Requests are simple HTTP calls with no body and no query parameters.

Suggested response body for the first version:

```json
{"status":"ok","cache":"mark"}
```

and:

```json
{"status":"ok","cache":"minmax"}
```

If the target cache is not enabled, still return success with a small message,
for example:

```json
{"status":"ok","cache":"mark","message":"cache not enabled"}
```

This keeps the API operationally convenient and idempotent.

### Logging and response conventions

The first version should emit an INFO audit log for every manual cache-evict
request. The log should at least include:

- action: `evict`
- cache: `mark` or `minmax`
- result: `ok`, `noop`, or `error`

This is sufficient for operational traceability and experiment timeline
correlation. The first version does not need a dedicated Prometheus metric for
manual cache eviction.

Response bodies should use a consistent small JSON shape such as:

```json
{"status":"ok","cache":"mark"}
```

or:

```json
{"status":"ok","cache":"minmax","message":"cache not enabled"}
```

The proposal intentionally does not introduce a shared helper for cache-evict
responses yet. With only two endpoints, keeping the JSON shape consistent in
the handlers is simpler than adding a new abstraction layer.

### Handler implementation

Implement the new handlers in `ProxyFFIStatusService.cpp` and route them from
the same path-dispatch table that already owns remote-cache eviction.

The handler logic should reuse the existing `Context` methods directly:

- `global_ctx.dropMarkCache()`
- `global_ctx.dropMinMaxIndexCache()`

This is preferable to calling `reset()` on the cache object directly because it
keeps all cache-management logic behind the `Context` API boundary.

### Execution model

The operation is synchronous and lightweight:

1. parse the path
2. resolve the cache type
3. obtain `Context` from `server->tmt`
4. call the corresponding `drop*Cache()` method
5. return a small success response

No asynchronous job or background task is required.

### ASCII flow

```text
HTTP request
  -> ProxyFFIStatusService
     -> parse /tiflash/cache/evict/<type>
     -> resolve global Context
     -> Context::dropMarkCache() or dropMinMaxIndexCache()
        -> LRUCache::reset()
           -> clear cache registry under mutex
           -> preserve already-held shared_ptr values in running queries
     -> return {"status":"ok", ...}
```

## Alternatives Considered

### 1. Add per-table or per-key eviction

Rejected for the first version.

That would require new cache key parsing and more precise ownership semantics,
while the immediate operational need is only to force a node-local cold path.

### 2. Recreate the cache object instead of resetting it

Rejected.

`Context` already exposes `dropMarkCache()` and `dropMinMaxIndexCache()` with
 the current reset semantics. Replacing the object instance adds risk without
clear benefit.

### 3. Put the API somewhere outside `ProxyFFIStatusService`

Rejected.

`ProxyFFIStatusService` already hosts similar operational endpoints and already
has the right access to `Context` and `TMTContext`.

## Incremental Plan

1. Add two new handlers in `ProxyFFIStatusService.cpp`.
2. Register both endpoints in the existing route table.
3. Return small JSON success responses.
4. Add unit tests for path dispatch and behavior when the cache is enabled or
   absent.
5. Validate manually on a dev cluster by evicting the cache and confirming the
   next query rebuilds the corresponding cache entries.

## Validation Strategy

### Unit tests

- request path dispatch resolves `/tiflash/cache/evict/mark`
- request path dispatch resolves `/tiflash/cache/evict/minmax`
- handler returns success when the cache exists
- handler returns success when the cache is absent
- repeated calls remain safe and idempotent

### Manual validation

Suggested manual checks:

1. Warm `MarkCache` / `MinMaxIndexCache` with one query.
2. Confirm cache bytes / files through existing asynchronous metrics.
3. Call the new eviction endpoint.
4. Confirm cache size drops to zero or near zero.
5. Re-run the same query and confirm cache entries are rebuilt.

## Risks and Mitigations

- **Risk:** cache reset causes unexpected failures for in-flight queries.
  - **Mitigation:** current `LRUCache` implementation stores values as
    `shared_ptr` and already tolerates concurrent `reset()` during `getOrSet()`.
- **Risk:** operators use the API in production at the wrong time and cause
  avoidable cold-path latency spikes.
  - **Mitigation:** document the API as an operational/debug endpoint rather
    than a routine user-facing action.
- **Risk:** internal hit/miss counters reset together with cache state.
  - **Mitigation:** document that cache reset also clears internal cache stats.

## Open Questions

- If more node-local cache-evict endpoints are added later, at what point does
  it become worthwhile to introduce a shared response/log helper instead of
  keeping the format duplicated in a few handlers?
