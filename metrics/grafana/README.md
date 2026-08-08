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

## Layout helpers

Prefer `common.band` / `common.buildRow` instead of hand-written `x/y/w`:

```jsonnet
{
  row: common.buildRow(
    rowObj,
    [
      common.band([store_sizeP, available_sizeP, capacity_sizeP]),  // 3 equal columns
      common.band([uptimeP, regionP]),                              // 2 equal columns
      common.band([fullWidthP]),                                    // 1 full-width panel
      common.band([{ panel: a, w: 8 }, { panel: b, w: 16 }], h=7), // custom widths/height
    ],
  ),
}
```

N panels in a band are equally divided across width 24 unless you pass explicit `w`.

## PromQL helpers (L1)

Prefer `common.expr.*` instead of hand-written selector strings:

```jsonnet
common.expr.sumRate('tiflash_stale_read_count', common.selector, by=['instance'])
common.expr.histogramQuantile('0.99', 'tiflash_storage_s3_request_seconds', common.selector, by=['type'])
common.expr.histogramAvg('tiflash_storage_s3_request_seconds', common.selector, by=['type'])
```

`histogramQuantile` / `histogramAvg` take the metric **base name** (no `_bucket` / `_sum` / `_count`).
Extra matchers go in `labels=`, e.g. `labels='type="decode"'`. Default range is `$__rate_interval`.

## Graph style helpers (L2)

Prefer `common.graph` / `common.target` / `common.override` instead of repeating
`graphPanel.new` + legend/yaxes boilerplate:

```jsonnet
common.graph(
  'Example',
  [
    common.target(common.expr.sumRate('metric', common.selector, by=['instance']), '{{instance}}'),
  ],
  yLeft='ops',
  yRight='none',
  fill=0,
)
```

For single-metric OPS/QPS panels, prefer `common.opsPanel`:

```jsonnet
common.opsPanel('Stale Read OPS', 'tiflash_stale_read_count', by=['instance'])
common.opsPanel(
  'Small Internal Tasks OPS',
  'tiflash_storage_subtask_count',
  by=['type'],
  labels='type!~"(delta_merge|seg_merge|seg_split).*"',
  yRight='opm',
)
```

For histogram heatmaps, prefer `common.heatmap`:

```jsonnet
common.heatmap(
  'Region write Duration (decode)',
  'tiflash_raft_write_data_to_storage_duration_seconds_bucket',
  labels='type="decode"',
)
```

For Threads CPU panels with a Limit red line, prefer `common.cpuWithLimitPanel`:

```jsonnet
common.cpuWithLimitPanel('SST Apply', 'apply_low_.*', description='Involved when importing data.')
common.cpuWithLimitPanel('Region Task', 'region_task.*', legend='{{name}} {{instance}}')
```

For OPS + hit-ratio dual-axis panels, prefer `common.opsHitRatioPanel`:

```jsonnet
common.opsHitRatioPanel(
  'Remote Cache Operations',
  'tiflash_storage_remote_cache',
  [
    { hitLabels: 'type=~"dtfile_hit"', totalLabels: 'type=~"dtfile_hit|dtfile_miss"', legend: 'dtfile_cache_hit_ratio' },
  ],
  by=['type', '$additional_groupby'],
  legend='{{type}} {{$additional_groupby}}',
)
```

## Duration histogram helpers

For `*_seconds_bucket` latency panels (default show p9999/p99; hide max/p999/p80/avg), prefer:

```jsonnet
local s3_Request_DurationP = common.durationPanel(
  'S3 Request Duration',
  'tiflash_storage_s3_request_seconds_bucket',
  by=['type'],
  legend='{{type}}-%s {{$additional_groupby}}',
  description='S3 Request Duration',
);
```

`%s` in `legend` is replaced by `max` / `9999` / `999` / `99` / `80` / `avg`. Use `selector=common.proxySelector` for proxy metrics.
