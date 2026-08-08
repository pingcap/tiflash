#!/usr/bin/env python3
"""Generate grafonnet-lib jsonnet sources from tiflash_summary.json.

Re-run after intentional dashboard JSON edits to refresh row libsonnet files.
Source of truth after migration is the generated *.libsonnet / *.jsonnet.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_JSON = ROOT / "scripts" / "tiflash_summary.original.json"
if not DASHBOARD_JSON.exists():
    DASHBOARD_JSON = ROOT / "tiflash_summary.json"
OUT_DIR = ROOT / "tiflash_summary"
ENTRY = ROOT / "tiflash_summary.jsonnet"

ROW_FILE_NAMES = {
    "Server": "rows_server.libsonnet",
    "Threads CPU": "rows_threads_cpu.libsonnet",
    "Threads": "rows_threads.libsonnet",
    "Coprocessor": "rows_coprocessor.libsonnet",
    "Task Scheduler": "rows_task_scheduler.libsonnet",
    "DDL": "rows_ddl.libsonnet",
    "Imbalance read/write": "rows_imbalance.libsonnet",
    "Memory trace": "rows_memory_trace.libsonnet",
    "Columnar Storage": "rows_columnar_storage.libsonnet",
    "Storage": "rows_storage.libsonnet",
    "Storage Read Pool & Data Sharing": "rows_storage_read_pool.libsonnet",
    "PageStorage": "rows_pagestorage.libsonnet",
    "Rate Limiter": "rows_rate_limiter.libsonnet",
    "Storage Write Stall": "rows_storage_write_stall.libsonnet",
    "Raft": "rows_raft.libsonnet",
    "Raft Snapshot / IngestSST": "rows_raft_snapshot.libsonnet",
    "Rough Set Filter Rate Histogram": "rows_rough_set.libsonnet",
    "Disaggregated-Write": "rows_disagg_write.libsonnet",
    "Disaggregated-Compute": "rows_disagg_compute.libsonnet",
    "S3": "rows_s3.libsonnet",
    "Pipeline Model": "rows_pipeline_model.libsonnet",
    "TiFlash Resource Control": "rows_resource_control.libsonnet",
    "Status Server": "rows_status_server.libsonnet",
    "Vector Search": "rows_vector_search.libsonnet",
}


def jstr(s: str) -> str:
    """Emit a jsonnet single-quoted string literal."""
    out = (
        (s or "")
        .replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return "'" + out + "'"


def jstr_or_multiline(s: str) -> str:
    # Keep everything as a single-quoted string (with \\n escapes).
    return jstr(s)


def ident(title: str, used: set[str]) -> str:
    base = re.sub(r"[^0-9A-Za-z]+", "_", title).strip("_")
    if not base:
        base = "panel"
    if base[0].isdigit():
        base = "p_" + base
    base = base[0].lower() + base[1:] if base else "panel"
    # camelCase-ish local names used by TiDB style: keep snake for uniqueness
    name = base
    i = 2
    while name in used:
        name = f"{base}_{i}"
        i += 1
    used.add(name)
    return name


def collapse_ws(s: str) -> str:
    """Collapse whitespace so PromQL fits in a single-quoted jsonnet string."""
    return " ".join((s or "").split())


def emit_target(t: dict) -> str:
    expr = collapse_ws(t.get("expr") or "")
    legend = t.get("legendFormat") or ""
    fmt = t.get("format") or "time_series"
    args = [jstr(expr), f"legendFormat={jstr(legend)}"]
    if fmt and fmt != "time_series":
        args.insert(1, f"format={jstr(fmt)}")
    # keep intervalFactor if not default 2
    if t.get("intervalFactor") not in (None, 2):
        args.append(f"intervalFactor={t['intervalFactor']}")
    if t.get("interval"):
        args.append(f"interval={jstr(t['interval'])}")
    # Preserve explicit hide=true (common for optional / high-cardinality series).
    if t.get("hide") is True:
        args.append("hide=true")
    elif t.get("hide") is False:
        # Omit false; grafonnet default is unset/visible.
        pass
    if t.get("instant") is True:
        args.append("instant=true")
    return "prometheus.target(\n    " + ",\n    ".join(args) + ",\n  )"


def emit_series_override(o: dict) -> str:
    parts = []
    for k, v in o.items():
        key = k if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", k) else jstr(k)
        if isinstance(v, bool):
            parts.append(f"{key}: {str(v).lower()}")
        elif isinstance(v, (int, float)) and not isinstance(v, bool):
            parts.append(f"{key}: {v}")
        elif v is None:
            continue
        else:
            parts.append(f"{key}: {jstr(str(v))}")
    return "{ " + ", ".join(parts) + " }"


def emit_yaxis(axis: dict | None, default_format: str = "short") -> str:
    """Emit graphPanel.addYaxis(...) args from an original yaxes[] entry."""
    axis = axis or {}
    args = [f"format={jstr(axis.get('format') or default_format)}"]
    amin = axis.get("min")
    if amin is not None and amin != "":
        if isinstance(amin, (int, float)) and not isinstance(amin, bool):
            args.append(f"min={amin}")
        else:
            args.append(f"min={jstr(str(amin))}")
    amax = axis.get("max")
    if amax is not None and amax != "":
        if isinstance(amax, (int, float)) and not isinstance(amax, bool):
            args.append(f"max={amax}")
        else:
            args.append(f"max={jstr(str(amax))}")
    label = axis.get("label")
    if label:
        args.append(f"label={jstr(label)}")
    if axis.get("show") is False:
        args.append("show=false")
    log_base = axis.get("logBase")
    if log_base not in (None, 1):
        args.append(f"logBase={log_base}")
    decimals = axis.get("decimals")
    if decimals is not None:
        args.append(f"decimals={decimals}")
    return "addYaxis(\n  " + ",\n  ".join(args) + ",\n)"


def emit_graph(p: dict, var: str) -> str:
    yaxes = p.get("yaxes") or []
    y1 = yaxes[0] if yaxes else {}
    y2 = yaxes[1] if len(yaxes) > 1 else {}
    legend = p.get("legend") or {}

    kwargs = [
        f"title={jstr(p.get('title') or '')}",
        "datasource=common.datasource",
    ]
    if p.get("description"):
        kwargs.append(f"description={jstr_or_multiline(p['description'])}")

    fill = p.get("fill", 1)
    kwargs.append(f"fill={fill}")
    lw = p.get("linewidth", 1)
    if lw != 1:
        kwargs.append(f"linewidth={lw}")

    npm = p.get("nullPointMode") or "null"
    kwargs.append(f"nullPointMode={jstr(npm)}")

    if p.get("stack"):
        kwargs.append("stack=true")
    if p.get("percentage"):
        kwargs.append("percentage=true")
    if p.get("bars"):
        kwargs.append("bars=true")
    if p.get("points"):
        kwargs.append("points=true")
    if p.get("lines") is False:
        kwargs.append("lines=false")
    if p.get("pointradius") not in (None, 5):
        kwargs.append(f"pointradius={p['pointradius']}")
    if p.get("decimals") is not None:
        kwargs.append(f"decimals={p['decimals']}")

    # legend
    if legend.get("show") is False:
        kwargs.append("legend_show=false")
    if legend.get("alignAsTable"):
        kwargs.append("legend_alignAsTable=true")
    if legend.get("rightSide"):
        kwargs.append("legend_rightSide=true")
    if legend.get("values"):
        kwargs.append("legend_values=true")
    if legend.get("current"):
        kwargs.append("legend_current=true")
    if legend.get("max"):
        kwargs.append("legend_max=true")
    if legend.get("min"):
        kwargs.append("legend_min=true")
    if legend.get("avg"):
        kwargs.append("legend_avg=true")
    if legend.get("total"):
        kwargs.append("legend_total=true")
    if legend.get("hideEmpty"):
        kwargs.append("legend_hideEmpty=true")
    if legend.get("hideZero"):
        kwargs.append("legend_hideZero=true")
    if legend.get("sort"):
        kwargs.append(f"legend_sort={jstr(legend['sort'])}")
    if legend.get("sortDesc"):
        kwargs.append("legend_sortDesc=true")
    if legend.get("sideWidth") is not None:
        kwargs.append(f"legend_sideWidth={legend['sideWidth']}")

    body = "graphPanel.new(\n  " + ",\n  ".join(kwargs) + ",\n)"
    for t in p.get("targets") or []:
        body += "\n.addTarget(\n  " + emit_target(t) + "\n)"
    for o in p.get("seriesOverrides") or []:
        body += "\n.addSeriesOverride(" + emit_series_override(o) + ")"
    # Rebuild y-axes per original panel so label/show/logBase/min stay accurate.
    # graphPanel.new() shares one min across both axes and always show=true.
    body += "\n.resetYaxes()"
    body += "\n." + emit_yaxis(y1, default_format="short")
    body += "\n." + emit_yaxis(y2, default_format="short")
    return f"local {var} = {body};\n"


def emit_heatmap(p: dict, var: str) -> str:
    yaxis = p.get("yAxis") or {}
    color = p.get("color") or {}
    kwargs = [
        f"title={jstr(p.get('title') or '')}",
        "datasource=common.datasource",
        f"dataFormat={jstr(p.get('dataFormat') or 'tsbuckets')}",
        f"yAxis_format={jstr(yaxis.get('format') or 'short')}",
        f"hideZeroBuckets={'true' if p.get('hideZeroBuckets') else 'false'}",
        f"color_mode={jstr(color.get('mode') or p.get('colorMode') or 'spectrum')}",
    ]
    if color.get("colorScheme"):
        kwargs.append(f"color_colorScheme={jstr(color['colorScheme'])}")
    if p.get("description"):
        kwargs.append(f"description={jstr_or_multiline(p['description'])}")
    legend = p.get("legend") or {}
    if legend.get("show"):
        kwargs.append("legend_show=true")

    body = "heatmapPanel.new(\n  " + ",\n  ".join(kwargs) + ",\n)"
    for t in p.get("targets") or []:
        # heatmap targets usually need format='heatmap'
        tt = dict(t)
        if not tt.get("format"):
            tt["format"] = "heatmap"
        body += "\n.addTarget(\n  " + emit_target(tt) + "\n)"
    return f"local {var} = {body};\n"


def emit_row_file(row: dict) -> str:
    title = row["title"]
    used: set[str] = set()
    panels_code = []
    # (var, w, h, x, y) in original order
    panel_vars: list[tuple[str, int, int, int, int]] = []
    for p in row.get("panels") or []:
        var = ident(p.get("title") or "panel", used) + "P"
        if p.get("type") == "heatmap":
            panels_code.append(emit_heatmap(p, var))
        else:
            panels_code.append(emit_graph(p, var))
        gp = p.get("gridPos") or {}
        panel_vars.append(
            (
                var,
                int(gp.get("w", 12)),
                int(gp.get("h", 8)),
                int(gp.get("x", 0)),
                int(gp.get("y", 0)),
            )
        )

    # Group into horizontal bands by original y; sort each band by x.
    by_y: dict[int, list[tuple[str, int, int, int]]] = {}
    for var, w, h, x, y in panel_vars:
        by_y.setdefault(y, []).append((var, w, h, x))
    bands = []
    for y in sorted(by_y):
        items = sorted(by_y[y], key=lambda t: t[3])  # by x
        h = items[0][2]
        widths = [it[1] for it in items]
        n = len(items)
        band_h = "" if h == 8 else f", h={h}"
        # Only auto-equalize when the band spans the full 24-wide grid with
        # identical widths (1/2/3/4 columns). Otherwise keep explicit w.
        equal_full_row = sum(widths) == 24 and len(set(widths)) == 1
        if equal_full_row:
            names = ", ".join(it[0] for it in items)
            bands.append(f"      common.band([{names}]{band_h})")
        else:
            parts = ", ".join(
                f"{{ panel: {it[0]}, w: {it[1]} }}" for it in items
            )
            bands.append(f"      common.band([{parts}]{band_h})")

    bands_block = ",\n".join(bands)

    lines = [
        "// Generated from tiflash_summary.json — edit carefully or regenerate.",
        "// Layout: use common.band / common.buildRow (do not hand-write x/y/w).",
        "local grafana = import 'grafonnet/grafana.libsonnet';",
        "local row = grafana.row;",
        "local graphPanel = grafana.graphPanel;",
        "local heatmapPanel = grafana.heatmapPanel;",
        "local prometheus = grafana.prometheus;",
        "local common = import 'common.libsonnet';",
        "",
        f"local rowObj = row.new(collapse=true, title={jstr(title)});",
        "",
    ]
    lines.extend(panels_code)
    lines.append("")
    lines.append("{")
    lines.append("  row: common.buildRow(")
    lines.append("    rowObj,")
    lines.append("    [")
    lines.append(bands_block)
    lines.append("    ],")
    lines.append("  ),")
    lines.append("}")
    lines.append("")
    return "\n".join(lines)


def slug(title: str) -> str:
    return ROW_FILE_NAMES.get(title) or (
        "rows_" + re.sub(r"[^0-9a-z]+", "_", title.lower()).strip("_") + ".libsonnet"
    )


def emit_entry(row_files: list[tuple[str, str]]) -> str:
    imports = "\n".join(
        f"local {safe_import_name(title)} = import 'tiflash_summary/{fname}';"
        for title, fname in row_files
    )
    add_panels = "\n".join(
        f".addPanel({safe_import_name(title)}.row, gridPos=common.rowPos)"
        for title, _ in row_files
    )
    return f"""// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

local grafana = import 'grafonnet/grafana.libsonnet';
local dashboard = grafana.dashboard;
local template = grafana.template;
local common = import 'tiflash_summary/common.libsonnet';

{imports}

local myNameFlag = 'DS_TEST-CLUSTER';

dashboard.new(
  title='Test-Cluster-TiFlash-Summary',
  uid='SVbh2xUWk',
  editable=true,
  graphTooltip='shared_crosshair',
  refresh='1m',
  time_from='now-1h',
  schemaVersion=27,
  style='dark',
)
.addInput(
  name=myNameFlag,
  label='Test-Cluster',
  type='datasource',
  pluginId='prometheus',
  pluginName='Prometheus',
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    hide='all',
    label='K8s-cluster',
    name='k8s_cluster',
    query='label_values(tiflash_system_profile_event_Query, k8s_cluster)',
    refresh='time',
    sort=1,
  )
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    hide='all',
    includeAll=false,
    label='tidb_cluster',
    multi=false,
    name='tidb_cluster',
    query='label_values(tiflash_system_profile_event_Query{{k8s_cluster="$k8s_cluster"}}, tidb_cluster)',
    refresh='time',
    sort=1,
  )
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    includeAll=true,
    label='Instance',
    multi=true,
    name='instance',
    query='label_values(tiflash_system_profile_event_Query{{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}}, instance)',
    refresh='load',
    sort=1,
  )
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    includeAll=true,
    label='Proxy Instance',
    multi=true,
    name='proxy_instance',
    query='label_values(tiflash_proxy_process_cpu_seconds_total{{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}}, instance)',
    refresh='load',
    sort=1,
  )
)
.addTemplate(
  template.custom(
    name='additional_groupby',
    query='none,instance',
    current='none',
    label='additional_groupby',
  )
)
.addTemplate(
  template.custom(
    name='tiflash_role',
    query='.*,.*write-tiflash.*,.*compute-tiflash.*',
    current='.*',
    label='Role',
    valuelabels={{
      '.*': 'All',
      '.*write-tiflash.*': 'Write',
      '.*compute-tiflash.*': 'Compute',
    }},
  )
)
{add_panels}
"""


def safe_import_name(title: str) -> str:
    name = re.sub(r"[^0-9A-Za-z]+", "_", title).strip("_").lower()
    if name[0].isdigit():
        name = "r_" + name
    return "row_" + name


def main() -> int:
    with DASHBOARD_JSON.open() as f:
        dash = json.load(f)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    row_files: list[tuple[str, str]] = []
    for row in dash.get("panels") or []:
        if row.get("type") != "row":
            continue
        title = row["title"]
        fname = slug(title)
        path = OUT_DIR / fname
        path.write_text(emit_row_file(row))
        row_files.append((title, fname))
        print(f"wrote {path.relative_to(ROOT)} ({len(row.get('panels') or [])} panels)")

    ENTRY.write_text(emit_entry(row_files))
    print(f"wrote {ENTRY.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
