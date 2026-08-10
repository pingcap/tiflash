# TiFlash Grafana dashboards

TiFlash Summary is generated as Grafana JSON from Python code using
[grafanalib](https://github.com/weaveworks/grafanalib), following the same
pattern as TiKV / Cloud Storage Engine (`common.py` + `*.dashboard.py`).

Please avoid manually modifying the generated `.json` files.

## Generate Dashboard JSON

```bash
cd metrics/grafana
./generate_dashboard.sh
```

This runs `uv sync`, formats Python sources with isort/black, regenerates
`tiflash_summary.json`, and updates `tiflash_summary.json.sha256`.

### Manual (uv)

```bash
cd metrics/grafana
uv sync
.venv/bin/isort --profile black *.py
.venv/bin/black *.py
.venv/bin/generate-dashboard -o tiflash_summary.json tiflash_summary.dashboard.py
```

## Files

| File | Description |
|------|-------------|
| `common.py` | Shared helpers: PromQL builders, `graph_panel`, L3 panels (`ops_panel`, `duration_panel`, `cpu_with_limit_panel`, …) |
| `tiflash_summary.dashboard.py` | TiFlash Summary dashboard source |
| `tiflash_summary.json` | Generated JSON — do not edit manually |
| `tiflash_summary.json.sha256` | SHA256 of the generated JSON |
| `generate_dashboard.sh` | Generate entrypoint |
| `pyproject.toml` / `uv.lock` | Python deps (`grafanalib==0.7.1`) |
| `scripts/compare_dashboards.py` | Semantic diff helper |
| `jsonnet_legacy/` | Archived jsonnet/grafonnet sources (not used for generation; cleanup later) |

## Authoring notes

- Prefer helpers in `common.py` for new panels (`ops_panel`, `duration_panel`,
  `tiflash_heatmap_panel`, `cpu_with_limit_panel`, `ops_hit_ratio_panel`,
  `graph_panel` + `expr_*`).
- Default PromQL labels always include `k8s_cluster` / `tidb_cluster`.
  Choose instance selectors with `instance_selector=CPP_LABEL_SELECTORS`
  (default: `$instance` + `$tiflash_role`) or
  `instance_selector=PROXY_LABEL_SELECTORS` (`$proxy_instance` +
  `$tiflash_role`). Put non-instance filters only in `label_selectors`.
  (`use_instance_selectors(...)` remains for shared-pool / legacy paths.)
- `tiflash_proxy_summary.json` / `tiflash_proxy_details.json` are still
  hand-maintained JSON.

## Validate

```bash
python3 scripts/compare_dashboards.py \
  scripts/tiflash_summary.pre_grafanalib.json \
  tiflash_summary.json
```
