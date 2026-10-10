# TiFlash MATCH AGAINST pushdown

## Implementation status

This document describes the implementation currently on the `match_against`
branch. Validation was performed with the release-8.5 TiDB/TiFlash branches
and TiUP playground v8.5.8. It includes Boolean pushdown, STANDARD_V1 and
NGRAM_V1 analysis, nullable MATCH-column handling, and collation-aware
fallback behavior.

The delivery target is the release-8.5 family. The supported product surface
is single- or multi-column MATCH in BOOLEAN MODE, provided the TiDB planner can
resolve one matching public FULLTEXT index:

```sql
MATCH(col) AGAINST('+tidb -mysql' IN BOOLEAN MODE)
MATCH(title, body) AGAINST('+tidb -mysql' IN BOOLEAN MODE)
```

For multi-column queries, the ordered MATCH column list must exactly match the
columns of one public composite FULLTEXT index on the same table. Independent
single-column indexes do not qualify for native TiFlash pushdown.

The implementation is a snapshot-local scan evaluator. TiFlash does not read
or maintain a physical FULLTEXT inverted index in this phase. TiDB still
requires a public FULLTEXT index as planner metadata and as the source of the
parser configuration. TiFlash is used when a table has an available replica;
without a TiFlash replica, TiDB uses its local evaluator through the normal
TiKV read path.

## Introduction

This document describes the TiFlash-side implementation for evaluating TiDB
`MATCH ... AGAINST` and `FTS_MATCH_WORD` expressions on the read snapshot.
The implementation is intentionally limited to the existing release-8.5
TiDB-to-TiFlash protocol. TiKV, TICI, and TiDB-side changes are out of scope
for this TiFlash change.

## Motivation or Background

The TiDB planner can serialize full-text scalar signatures and the
`FTSQueryTypeWithScore` table-scan metadata into tipb, but the release-8.5
TiFlash branch did not recognize those signatures. A snapshot-local evaluator
allows TiFlash to execute the predicate and materialize `_FTS_SCORE` after
reading the row, without depending on an asynchronously maintained external
index.

## Detailed Design

### Protocol and planner path

The `contrib/tipb` submodule is pinned to the FTS protocol commit
`26695da26e` on the `release-8.5-match-against` line. The protocol carries
`ScalarFuncSig_FTSMatchWord`, `ScalarFuncSig_FTSMatchExpression`,
`FTSQueryInfo`, `FTSBooleanQuery`, and `used_columnar_indexes`. `FTSBooleanQuery`
also carries `query_tokenizer` and `ngram_token_size`, so TiFlash uses the
same analyzer selected by the FULLTEXT index and TiDB session configuration.

TiFlash maps the two scalar signatures as follows:

| tipb signature | TiFlash function | semantics |
| --- | --- | --- |
| `FTSMatchWord` | `fts_match_word` | Legacy one-column full-text evaluator |
| `FTSMatchExpression` | `fts_match_expression` | Boolean-aware evaluator over MATCH columns |

For the #70484/#70485 boolean predicate, TiDB serializes a `FTSQueryInfo`
into the table scan. For native pushdown, the MATCH columns must resolve in
order to one public composite FULLTEXT index on the same table; this prevents
the planner from substituting unrelated per-column indexes. `TiDBTableScan`
extracts the query and `PhysicalTableScan` builds an `fts_match_expression`
filter containing:

1. the original query text;
2. MATCH column references and their TiDB field types;
3. the encoded `FTSBooleanQuery` containing required and prohibited terms.

The filter is attached to the TiFlash table-scan pipeline rather than added
as a root TiDB Selection. The expression carries the first MATCH column's
protocol collation to the TiFlash function. The score path still supports the
existing `_FTS_SCORE` placeholder, but score semantics are outside the
boolean-mode delivery target.

The expression pipeline runs on the same read snapshot as the row data. The
FTS query is evaluated by the scan executor after rows are read; it is not
used by the DeltaMerge rough-set index. Consequently, current logs may report
`FTSMatchExpression is not supported` for rough-set pruning while the MATCH
predicate itself is still evaluated successfully by the TiFlash scan.

### Analyzer and matcher

The evaluator implements the STANDARD_V1 and NGRAM_V1 Boolean matching subset
used by #70484/#70485:

- Both analyzers recognize Unicode letter/number token runs with `_` preserved;
- STANDARD_V1 uses Unicode lower-casing when no protocol collator is available,
  a default token length range of 3..84, and the default InnoDB stopword set;
- NGRAM_V1 emits fixed-size Unicode code-point ngrams. The token size is read
  from `ngram_token_size`, whose release-8.5 default is 2;
- Boolean `+` required terms, `-` prohibited terms, quoted phrases, and a
  trailing `*` prefix;
- phrase positions are retained across analyzer filtering, so removed
  stopwords do not close phrase gaps;
- NULL MATCH columns contribute no tokens and do not nullify the whole row.

The predicate path consumes the numeric result as a Boolean value (`0` means
no match, a positive value means match). Queries containing Boolean syntax
use the Boolean matcher; plain queries retain the existing token-overlap
behavior for compatibility with the current scalar signature. The
implementation still returns a deterministic positive term-frequency value
for the existing `_FTS_SCORE` placeholder path, but that score is not part of
the #70484/#70485 acceptance scope and is not claimed to be MySQL/InnoDB
relevance. Nullable query arguments preserve NULL.

### Collation handling

When a protocol collation is available, TiFlash preserves the source spelling
of tokens and compares terms through the TiDB collator. This makes matching
case and accent behavior follow the column collation. Prefix matching reuses
the same collator-aware comparison path.

The local TiDB fallback is a separate implementation, but it now receives the
MATCH column collation in its analyzer configuration and uses the same
collator-aware token, prefix, and phrase comparisons. The fallback therefore
preserves the tested `utf8mb4_bin`, `utf8mb4_general_ci`, and
`utf8mb4_0900_ai_ci` behavior when TiFlash is unavailable.

### Nullable columns

`FunctionsFullText.cpp` handles nullable string columns by treating a NULL
MATCH column as an empty document. The table-scan expression must nevertheless
declare a nullable result type: otherwise the DAG analyzer inserts a cast from
`Nullable(Float64)` to `Float64` and a NULL row fails with
`Cannot convert NULL value to non-Nullable type`.

`PhysicalTableScan` now clears `ColumnFlagNotNull` when any MATCH column is
nullable. This keeps the planner metadata aligned with the function return
type and lets a nullable column be evaluated as a normal no-match row in a
WHERE predicate.

### Consistency boundary

This is a scan evaluator, not a DeltaMerge native full-text index. It therefore
does not introduce a second mutable index and cannot return rows that are
invisible at the read timestamp. The index id in `FTSQueryInfo` is used only to
identify the query contract in this phase; it is not read by a separate index
reader. The score is calculated from the query and row text, so it does not
provide corpus-wide IDF normalization.

## Test Design

### Functional Tests

`dbms/src/Functions/tests/gtest_fulltext.cpp` covers required/prohibited terms,
phrases with stopword gaps, prefixes, word boundaries, nullable input, NULL
MATCH columns, collation behavior, and unsupported score modifiers. The
`MatchExpressionCollationMatrix` case covers `utf8mb4_bin`,
`utf8mb4_0900_bin`, `utf8mb4_general_ci`, `utf8mb4_unicode_ci`, and
`utf8mb4_0900_ai_ci` for both ordinary terms and prefix terms.

On 2026-09-23, the `gtests_dbms --gtest_filter='TestFullText.*'` suite passed
all 14 tests. This includes regressions for parsing `run*` as a prefix query
and matching `+run*` against `RUNNER` under a case-insensitive collation. The
fix was in Boolean query parsing: the scanner stopped before `*` but did not
attach it to the preceding term. TiUP SQL E2E was not rerun after this parser
change; the E2E results below are from the earlier validation run. The
multi-column planner resolver and TiFlash evaluator have targeted unit
coverage, but multi-column MATCH has not yet been verified through TiUP SQL.

### Local E2E validation

The local E2E test used TiUP playground v8.5.8 with locally built TiDB and
TiFlash binaries. The TiFlash replica was waited to `AVAILABLE=1` and
`PROGRESS=1` before querying.

The main query:

```sql
SELECT id, body
FROM articles
WHERE MATCH(body) AGAINST('+tidb -mysql' IN BOOLEAN MODE)
ORDER BY id;
```

returned row `2` with the data set used for the release-8.5 verification. Its
plan was:

```text
TableReader
└─ExchangeSender  mpp[tiflash]
  └─TableFullScan   mpp[tiflash]
```

There was no root `Selection(match_against(...))`. `EXPLAIN ANALYZE` showed
five rows scanned and one row output by the TiFlash task.

The currently supported TiDB collations were tested on TiFlash:

- `ascii_bin`
- `latin1_bin`
- `utf8_bin`
- `utf8_general_ci`
- `utf8_unicode_ci`
- `utf8mb4_bin`
- `utf8mb4_general_ci`
- `utf8mb4_unicode_ci`
- `utf8mb4_0900_bin`
- `utf8mb4_0900_ai_ci`

The matrix covered exact terms, `*` prefixes, case behavior, and
`cafe`/`café` accent behavior. Binary collations matched only the exact case
and spelling; CI/AI collations matched the expected case or accent variants.

The nullable regression test used a `utf8mb4_bin NULL` column containing
`quick runner`, `NULL`, and `QUICK runner`. The query returned only
`quick runner`, used `mpp[tiflash]`, and no longer raised the NULL conversion
error.

The no-replica test returned successfully through:

```text
Selection
└─TableReader
  └─TableFullScan  cop[tikv]
```

This confirms the TiDB fallback path remains available when TiFlash cannot be
used and returns the same result as the native path after
`tidb_enable_local_match_against` is enabled. With the session variable left
`OFF`, TiDB intentionally returns the existing error requiring either a
TiFlash FULLTEXT replica or local MATCH AGAINST evaluation.

The NGRAM E2E used a public FULLTEXT index with `WITH PARSER NGRAM`,
`ngram_token_size = 2`, and the query `+数据库 -mysql`. TiFlash pushdown and
TiDB fallback returned identical rows under both `utf8mb4_bin` and
`utf8mb4_general_ci`; the binary collation remained case-sensitive while the
CI collation matched `mysql` and `MySQL` equivalently.

### Compatibility Tests

The normal local TiFlash read path and region-retry remote-read path use the
same expression evaluator. TICI/Disaggregated execution is intentionally not
part of this phase.

### Benchmark Tests

No benchmark is included in this phase. The scan evaluator is expected to be
slower than an inverted index and should be treated as compute pushdown, not
index-accelerated search.

## Impacts & Risks

The implementation adds per-row tokenization and matching CPU cost. Its score
is deterministic but is not a native MySQL relevance score. Custom stopword
lists and non-default `AGAINST` modifiers are not represented in the current
wire contract and must not be treated as supported by this phase. NGRAM_V1 is
supported for the configured `ngram_token_size`; it is still a scan evaluator,
not an inverted-index implementation.

The scan evaluator currently does not perform inverted-index lookup or
rough-set pruning for FTS. Large tables may therefore scan many rows even
when the final MATCH result is small. STANDARD_V1 Chinese/CJK behavior follows
its default minimum token length, while NGRAM_V1 handles CJK through fixed-size
code-point windows. This phase does not claim general linguistic segmentation.

## Investigation & Alternatives

An asynchronous external index was rejected because it cannot provide the
required snapshot consistency. A native DeltaMerge full-text index remains a
long-term performance design, but it requires a larger write-path, snapshot,
and index-reader change set.

## Remaining work

Before product release, multi-column MATCH still needs TiUP SQL E2E coverage;
the paired TiDB branch and TiFlash branch also need CI coverage, release-8.5
build verification, and code review. If exact
corpus-wide ranking or index-level performance is required, a future phase
must define a relevance-score contract and add a native DeltaMerge full-text
index.
