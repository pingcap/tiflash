# Generate TiFlash Grafana Dashboards with Python grafanalib

- Author(s): [JaySon-Huang](https://github.com/JaySon-Huang)

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This design migrates **TiFlash Summary** (and sets the pattern for other TiFlash Grafana dashboards) from hand-maintained to a **Python + [grafanalib](https://github.com/weaveworks/grafanalib)** generation pipeline, aligned with TiKV (`common.py` + `*.dashboard.py`).
Dashboard authors edit Python sources; checked-in `.json` is a generated artifact produced by `generate_dashboard.sh`.

## Motivation or Background

### Problems with the previous approach

1. **Hard to review and evolve**: TiFlash Summary lived as a very large Grafana
   JSON export. Small panel changes produced noisy diffs; PromQL strings were
   duplicated with inconsistent label selectors and rate windows.
2. **Weak abstraction**: Common patterns (OPS `sum(rate)`, duration histograms,
   Threads CPU + Limit, heatmaps) were copy-pasted instead of shared helpers.
3. **Tooling mismatch**: Intermediate jsonnet/grafonnet work improved structure
   but stayed farther from the TiKV Python tooling that the monitoring
   ecosystem already standardizes on. More importantly, we do not want to
   introduce a **Go/jsonnet toolchain dependency** into TiFlash’s developer
   workflow and CI; Python + `uv` / grafanalib fits the existing metrics
   authoring path without expanding the build/test tool surface.

### Goals

- Make **Python sources** the source of truth for TiFlash Summary.
- Provide layered helpers so new panels reuse PromQL builders and panel
  factories instead of raw JSON.
- Keep generated JSON importable by Grafana / Clinic / TiUP packaging with
  stable dashboard `uid` / datasource `__inputs`.
- Prefer intentional, documented semantic alignments (e.g. `$__rate_interval`)
  over silent regressions.

### Non-goals (this phase)

- Rewriting `tiflash_proxy_summary.json` / `tiflash_proxy_details.json` (remain
  hand-maintained for now).
- Changing TiFlash runtime metrics emission or Prometheus scrape config.
- Requiring CI to import dashboards into a live Grafana (optional follow-up).

## Detailed Design

### Directory layout

```text
metrics/grafana/
  common.py                      # shared PromQL + panel helpers
  tiflash_summary.dashboard.py   # TiFlash Summary source
  tiflash_summary.json           # generated (do not edit)
  tiflash_summary.json.sha256
  generate_dashboard.sh          # uv sync + format + generate
  pyproject.toml / uv.lock       # grafanalib==0.7.1
  README.md
```

During migration we also used a temporary `scripts/compare_dashboards.py` for
semantic JSON diffs against legacy baselines; it is not kept in tree afterward.

### Generation flow

Authors run:

```bash
cd metrics/grafana
./generate_dashboard.sh
```

The script syncs the `uv` env, runs `isort`/`black` on `*.py`, then
`generate-dashboard -o tiflash_summary.json tiflash_summary.dashboard.py`, and
updates the SHA256 sidecar.

### Layered helper model

The design mirrors CSE’s four-layer DSL:

```text
  Expr / OpExpr
  (expr_sum_rate, expr_histogram_*)
            │
            v
  target()
  (legend / hide / interval_factor)
            │
            v
  graph_panel / yaxes / Layout
            │
            v
  ops_panel / duration_panel /
  cpu_with_limit_panel / heatmap
```

1. **PromQL builders** (`Expr`, `expr_sum`, `expr_sum_rate`, histogram helpers):
   always attach cluster selectors (`k8s_cluster` / `tidb_cluster`) and choose
   instance selectors via `instance_selector`:
   - `CPP_LABEL_SELECTORS`: `$instance` + `$tiflash_role`
   - `PROXY_LABEL_SELECTORS`: `$proxy_instance` + `$tiflash_role`
2. **`target()`**: wraps PromQL into grafanalib `Target` with legend / hide.
3. **`graph_panel()` / `yaxes()` / `Layout`**: shared visual defaults (legend
   table, tooltip sort, single-axis `right_show=False`, IEC byte-unit assert).
4. **Domain panels**:
   - `ops_panel`: single `sum(rate(...))` OPS-style graph
   - `duration_panel`: S3-style histogram quantiles (max/9999/999/99/80/avg)
   - `cpu_with_limit_panel`: Threads CPU series + Limit override
   - heatmap / hit-ratio helpers for specialized rows

Dashboard rows are ordinary Python functions returning `RowPanel`, composed in the dashboard entrypoint.

### Compatibility

- **Grafana / Clinic / TiUP**: keep dashboard `uid` and `__inputs` datasource
  wiring so existing imports can overwrite the same dashboard identity when
  desired.
- **External components**: no change to TiDB / TiKV / PD metrics contracts;
  only how TiFlash Summary JSON is authored.

## Test Design

### Functional Tests

- Regenerate with `./generate_dashboard.sh`; confirm exit 0 and SHA256 update.
- Python sources format cleanly under repo `isort`/`black` settings.
- Spot-import generated JSON into a test Grafana and verify datasource binding
  (`DS_TEST-CLUSTER` → local Prometheus).

### Compatibility Tests

- Semantic compare against a known baseline JSON when available (migration
  phase used `scripts/compare_dashboards.py`; not retained after landing).

### Benchmark Tests

Not applicable: this change does not affect TiFlash query/storage runtime performance.

## Impacts & Risks

### Impacts

- **Positive**: smaller, reviewable panel diffs; reusable helpers; consistent
  PromQL selectors and units; same authoring model as TiKV.
- **Positive**: clearer ownership — edit `.dashboard.py`, never hand-edit
  generated `.json`.
- **Neutral / operational**: dashboard JSON shape may differ cosmetically
  (schema metadata, default legend flags) while queries remain equivalent aside
  from documented alignments.

### Risks

- **Silent PromQL drift** if helpers compose selectors incorrectly
  (mitigation: migration-time `compare_dashboards.py`, Grafana spot-check,
  code review).
- **grafanalib version skew** (`0.7.1` pinned in `uv.lock`); upgrading may
  change JSON defaults (mitigation: pin + regenerate in the same PR).

## Investigation & Alternatives

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Keep hand-edited JSON | Zero tooling | Unmaintainable diffs | Rejected |
| jsonnet + grafonnet-lib | Structured, used briefly in TiFlash | Diverges from TiKV Python; extra golang toolchain | Rejected as long-term SoT |
| **Python grafanalib** | Matches TiKV; rich helpers; `uv` workflow | Need Python env for regen | **Chosen** |
| Grafana UI only / provisioning CRDs | Nice for ops clusters | Poor fit for git-reviewed upstream metrics | Out of scope |

## Unresolved Questions

- Whether CI should fail if `tiflash_summary.json` is stale vs sources (SHA / regenerate check).
- Timeline / ownership for migrating `tiflash_proxy_summary.json` and
  `tiflash_proxy_details.json` to the same pipeline.
