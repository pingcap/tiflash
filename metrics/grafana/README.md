# TiFlash Grafana dashboards

## About

Use [go-jsonnet](https://github.com/google/go-jsonnet) and
[grafonnet-lib](https://github.com/grafana/grafonnet-lib) (TiDB-compatible
[nolouch fork](https://github.com/nolouch/grafonnet-lib)) to generate
`tiflash_summary.json`.

Why jsonnet?

1. The exported Grafana JSON is too large to maintain by hand (~27k lines).
2. Jsonnet + grafonnet-lib keep panel queries / layout reviewable in small files.

## Layout

```text
metrics/grafana/
  tiflash_summary.jsonnet          # dashboard entry (templates + row assembly)
  tiflash_summary/
    common.libsonnet               # shared datasource / gridPos helpers
    rows_*.libsonnet               # one file per dashboard row
  generate_json.sh                 # regenerate tiflash_summary.json
  scripts/compare_dashboards.py    # semantic diff vs previous JSON
  scripts/gen_jsonnet_from_dashboard.py  # one-shot / refresh codegen from JSON
```

## Usage

1. Edit the relevant `tiflash_summary/*.libsonnet` (or the entry jsonnet).
2. Run `./generate_json.sh` to regenerate `tiflash_summary.json`.
3. Optionally compare against the previous generated file (or git HEAD):  
   `python3 scripts/compare_dashboards.py scripts/tiflash_summary.original.json tiflash_summary.json`
4. Commit **both** the jsonnet sources and the generated JSON.

Do **not** hand-edit `tiflash_summary.json` — it is a generated artifact.

`scripts/gen_jsonnet_from_dashboard.py` is only for bootstrapping / refreshing row files from a dashboard JSON export. Day-to-day edits should be made in the `*.libsonnet` / `tiflash_summary.jsonnet` sources.

## Notes

- Panel types remain legacy `graph` / `heatmap` for clinic-grafana compatibility.
- Migrating to the newer [grafana/grafonnet](https://github.com/grafana/grafonnet)
  is intentionally deferred (same as TiDB's current TODO).
