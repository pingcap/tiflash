#!/usr/bin/env python3
"""Bootstrap tiflash_summary.dashboard.py from existing Grafana JSON.

Preserves PromQL / panel titles / legends for semantic parity. Authors should
prefer common.py helpers for new panels; this script is a one-shot migrator.
"""

from __future__ import annotations

import json
import re
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "tiflash_summary.json"
OUT = ROOT / "tiflash_summary.dashboard.py"


def py_str(s: str | None) -> str:
    if s is None:
        return "None"
    return repr(s)


def py_ident(title: str) -> str:
    s = re.sub(r"[^0-9A-Za-z]+", "_", title).strip("_")
    if not s:
        s = "Row"
    if s[0].isdigit():
        s = "R_" + s
    return s


def emit_target(t: dict, indent: str) -> str:
    expr = t.get("expr") or ""
    legend = t.get("legendFormat")
    hide = bool(t.get("hide"))
    interval = t.get("intervalFactor")
    args = [f"expr={py_str(expr)}"]
    if legend is not None:
        args.append(f"legend_format={py_str(legend)}")
    if hide:
        args.append("hide=True")
    if interval is None:
        args.append("interval_factor=1")
    elif interval != 2:
        args.append(f"interval_factor={interval}")
    return f"{indent}target({', '.join(args)})"




def yaxes_from_panel(p: dict) -> str:
    yaxes = p.get("yaxes") or []
    left = yaxes[0] if yaxes else {}
    right = yaxes[1] if len(yaxes) > 1 else {}
    left_fmt = left.get("format") or "short"
    right_fmt = right.get("format") or "short"
    left_min = left.get("min")
    left_max = left.get("max")
    left_log = left.get("logBase") or 1
    right_show = right.get("show", True)
    args = [f"left_format={py_str(left_fmt)}"]
    # Always pass right format; visibility controlled separately.
    args.append(f"right_format={py_str(right_fmt)}")
    call = f"yaxes({', '.join(args)})"
    # Apply min/max/log via mutating after — keep simple: encode in extra_json below.
    return call, right_show, left, right


def legend_from_panel(p: dict) -> str | None:
    leg = p.get("legend") or {}
    # Only emit custom legend when it differs meaningfully from defaults.
    kwargs = []
    mapping = {
        "avg": "avg",
        "current": "current",
        "max": "max",
        "min": "min",
        "total": "total",
        "show": "show",
        "alignAsTable": "align_as_table",
        "hideEmpty": "hide_empty",
        "hideZero": "hide_zero",
        "rightSide": "right_side",
        "sideWidth": "side_width",
        "sortDesc": "sort_desc",
    }
    defaults = {
        "avg": False,
        "current": True,
        "max": True,
        "min": False,
        "total": False,
        "show": True,
        "alignAsTable": True,
        "hideEmpty": True,
        "hideZero": True,
        "rightSide": True,
        "sideWidth": None,
        "sortDesc": True,
    }
    changed = False
    for src, dst in mapping.items():
        if src not in leg:
            continue
        val = leg[src]
        if val != defaults.get(src):
            changed = True
            kwargs.append(f"{dst}={py_str(val) if not isinstance(val, bool) else val}")
    if not changed:
        return None
    return f"graph_legend({', '.join(kwargs)})"


def emit_graph(p: dict, indent: str) -> str:
    yaxes_call, right_show, left, right = yaxes_from_panel(p)
    legend_call = legend_from_panel(p)
    targets = p.get("targets") or []
    target_lines = ",\n".join(emit_target(t, indent + "        ") for t in targets)
    desc = p.get("description")
    fill = p.get("fill", 0)
    linewidth = p.get("linewidth", p.get("lineWidth", 1))
    stack = bool(p.get("stack"))
    null_mode = p.get("nullPointMode") or "null as zero"
    decimals = p.get("decimals")
    points = bool(p.get("points"))
    pointradius = p.get("pointradius", 5)
    tooltip = p.get("tooltip") or {}
    tooltip_sort = tooltip.get("sort", 0) or 0
    overrides = p.get("seriesOverrides") or []
    extra = {}
    # Preserve y-axis min/max/log/label when present.
    if any(
        left.get(k) not in (None, "", 1, False)
        for k in ("min", "max", "label", "logBase", "decimals")
    ) or any(
        right.get(k) not in (None, "", 1, False)
        for k in ("min", "max", "label", "logBase", "decimals")
    ):
        extra["yaxes"] = [
            {
                "format": left.get("format") or "short",
                "label": left.get("label"),
                "logBase": left.get("logBase") or 1,
                "max": left.get("max"),
                "min": left.get("min"),
                "show": left.get("show", True),
                "decimals": left.get("decimals"),
            },
            {
                "format": right.get("format") or "short",
                "label": right.get("label"),
                "logBase": right.get("logBase") or 1,
                "max": right.get("max"),
                "min": right.get("min"),
                "show": right.get("show", True),
                "decimals": right.get("decimals"),
            },
        ]
        right_show = bool(right.get("show", True))

    args = [
        f"title={py_str(p.get('title'))}",
        f"targets=[\n{target_lines},\n{indent}    ]" if targets else "targets=[]",
        f"yaxes={yaxes_call}",
    ]
    if desc:
        args.append(f"description={py_str(desc)}")
    if legend_call:
        args.append(f"legend={legend_call}")
    if fill != 0:
        args.append(f"fill={fill}")
    if linewidth != 1:
        args.append(f"line_width={linewidth}")
    if stack:
        args.append("stack=True")
    if null_mode != "null as zero":
        args.append(f"null_point_mode={py_str(null_mode)}")
    if decimals is not None:
        args.append(f"decimals={decimals}")
    if points:
        args.append("points=True")
        args.append(f"pointradius={pointradius}")
    if tooltip_sort:
        args.append(f"tooltip_sort={tooltip_sort}")
    if overrides:
        args.append(f"series_overrides={py_str(overrides)}")
    # Always set y_right_show explicitly from JSON when we didn't embed yaxes.
    args.append(f"y_right_show={right_show}")
    if extra:
        args.append(f"extra_json={py_str(extra)}")
    # Prefer no fill_gradient unless JSON had fillGradient
    fg = p.get("fillGradient", 0) or 0
    if fg:
        args.append(f"fill_gradient={fg}")
    else:
        args.append("fill_gradient=0")

    joined = f",\n{indent}    ".join(args)
    return f"{indent}graph_panel(\n{indent}    {joined},\n{indent})"


def emit_heatmap(p: dict, indent: str) -> str:
    targets = p.get("targets") or []
    yax = p.get("yAxis") or p.get("yaxis") or {}
    y_format = yax.get("format") or "short"
    desc = p.get("description")
    t_lines = []
    for t in targets:
        expr = t.get("expr") or ""
        legend = t.get("legendFormat") or "{{le}}"
        interval = t.get("intervalFactor")
        iv = f", interval_factor={interval}" if interval is not None else ", interval_factor=1"
        hide = ", hide=True" if t.get("hide") else ""
        t_lines.append(
            f"{indent}    target(expr={py_str(expr)}, legend_format={py_str(legend)}{iv}{hide})"
        )
    # Use a helper expression that builds Heatmap with mutated target formats.
    inner_indent = indent + "    "
    targets_block = ",\n".join(t_lines)
    desc_arg = f",\n{inner_indent}description={py_str(desc)}" if desc else ""
    return (
        f"{indent}make_heatmap(\n"
        f"{inner_indent}title={py_str(p.get('title'))}{desc_arg},\n"
        f"{inner_indent}y_format={py_str(y_format)},\n"
        f"{inner_indent}log_base={yax.get('logBase') or 1},\n"
        f"{inner_indent}hide_zero_buckets={bool(p.get('hideZeroBuckets', True))},\n"
        f"{inner_indent}max_data_points={p.get('maxDataPoints') or 512},\n"
        f"{inner_indent}targets=[\n{targets_block},\n"
        f"{inner_indent}],\n"
        f"{indent})"
    )


def emit_panel(p: dict, indent: str) -> str:
    ptype = p.get("type")
    if ptype == "heatmap":
        return emit_heatmap(p, indent)
    # Treat everything else as graph (legacy graph panels).
    return emit_graph(p, indent)


def emit_row(row: dict) -> str:
    title = row.get("title") or "Row"
    fn = py_ident(title)
    panels = row.get("panels") or []
    # Group into bands by shared y coordinate.
    bands: list[list[dict]] = []
    cur_y = None
    for p in panels:
        y = (p.get("gridPos") or {}).get("y")
        if cur_y is None or y != cur_y:
            bands.append([p])
            cur_y = y
        else:
            bands[-1].append(p)

    parts = [
        f"def {fn}() -> RowPanel:",
        f"    layout = Layout(title={py_str(title)})",
    ]
    for band in bands:
        widths = [(p.get("gridPos") or {}).get("w", 24) for p in band]
        heights = [(p.get("gridPos") or {}).get("h", 8) for p in band]
        h = max(heights) if heights else 8
        parts.append("    layout.row([")
        for p in band:
            parts.append(emit_panel(p, "        ") + ",")
        parts.append(f"    ], height={h}, widths={widths})")
    parts.append("    return layout.row_panel")
    parts.append("")
    return "\n".join(parts)



def emit_templates(templating: dict) -> str:
    items = (templating or {}).get("list") or []
    lines = ["def Templates() -> Templating:", "    return Templating(list=["]
    for t in items:
        name = t.get("name")
        ttype = t.get("type")
        hide = t.get("hide", 0)
        # grafanalib: hide 0=show, 1=label, 2=variable — CSE uses HIDE_VARIABLE/SHOW
        hide_const = "SHOW" if hide == 0 else ("HIDE_VARIABLE" if hide == 2 else hide)
        if ttype == "custom":
            # Custom vars need explicit options: grafanalib comma-split does not
            # understand Grafana "All : .*" text:value syntax.
            query = t.get("query") or ""
            current = t.get("current") or {}
            default = current.get("value")
            options = t.get("options") or []
            lines.append("        Template(")
            lines.append(f"            name={py_str(name)},")
            lines.append("            type='custom',")
            lines.append(f"            query={py_str(query)},")
            lines.append("            dataSource=None,")
            lines.append(f"            hide={hide_const},")
            if t.get("label"):
                lines.append(f"            label={py_str(t.get('label'))},")
            if default is not None:
                lines.append(f"            default={py_str(default)},")
            if options:
                lines.append(f"            options={py_str(options)},")
            lines.append("        ),")
        else:
            lines.append(
                "        template("
                f"name={py_str(name)}, "
                f"type={py_str(ttype)}, "
                f"query={py_str(t.get('query'))}, "
                f"data_source=DATASOURCE, "
                f"hide={hide_const}, "
                f"multi={bool(t.get('multi'))}, "
                f"include_all={bool(t.get('includeAll'))}, "
                f"all_value={py_str(t.get('allValue'))}, "
                f"label={py_str(t.get('label'))}, "
                f"refresh={t.get('refresh') or 1}"
                "),"
            )
    lines.append("    ])")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    dash = json.loads(SRC.read_text())
    rows = [p for p in dash.get("panels") or [] if p.get("type") == "row"]
    row_fns = [py_ident(r.get("title") or f"Row{i}") for i, r in enumerate(rows)]

    header = textwrap.dedent(
        '''\
        # Generated from tiflash_summary.json — prefer editing with common.py helpers.
        import os
        import sys

        sys.path.append(os.path.dirname(__file__))

        from common import (
            DATASOURCE,
            DATASOURCE_INPUT,
            Layout,
            graph_legend,
            graph_panel,
            make_heatmap,
            target,
            template,
            yaxes,
        )
        from grafanalib.core import (
            GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
            HIDE_VARIABLE,
            SHOW,
            Dashboard,
            RowPanel,
            Template,
            Templating,
        )

        '''
    )

    body_parts = [emit_templates(dash.get("templating") or {})]
    for row in rows:
        body_parts.append(emit_row(row))

    panels_list = ",\n        ".join(f"{fn}()" for fn in row_fns)
    footer = textwrap.dedent(
        f'''\
        dashboard = Dashboard(
            title={py_str(dash.get("title"))},
            uid={py_str(dash.get("uid"))},
            timezone={py_str(dash.get("timezone") or "browser")},
            refresh={py_str(dash.get("refresh") or "1m")},
            inputs=[DATASOURCE_INPUT],
            editable=True,
            templating=Templates(),
            panels=[
                {panels_list},
            ],
            schemaVersion={dash.get("schemaVersion") or 14},
            graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
            time={py_str((dash.get("time") or {}).get("from") or "now-1h")},
        ).auto_panel_ids()
        '''
    )
    # Fix time= — Dashboard expects Time object; use shared time from grafanalib if needed.
    # grafanalib Dashboard(time=) often accepts dict-like; keep simple without time kw if unsure.
    footer = textwrap.dedent(
        f'''\
        dashboard = Dashboard(
            title={py_str(dash.get("title"))},
            uid={py_str(dash.get("uid"))},
            timezone={py_str(dash.get("timezone") or "browser")},
            refresh={py_str(dash.get("refresh") or "1m")},
            inputs=[DATASOURCE_INPUT],
            editable=True,
            templating=Templates(),
            panels=[
                {panels_list},
            ],
            schemaVersion=14,
            graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
        ).auto_panel_ids()
        '''
    )

    OUT.write_text(header + "\n".join(body_parts) + "\n" + footer)
    print(f"Wrote {OUT} ({OUT.stat().st_size} bytes), rows={len(rows)}")


if __name__ == "__main__":
    main()
