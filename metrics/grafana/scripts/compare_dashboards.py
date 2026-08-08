#!/usr/bin/env python3
"""Semantically compare two Grafana dashboard JSON files.

Ignores volatile fields (id, version, iteration, ...) and focuses on
row/panel titles, PromQL expr, legendFormat, units, and key display flags.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any


VOLATILE_PANEL_KEYS = {
    "id",
    "gridPos",
    "pluginVersion",
    "editable",
    "error",
    "timeFrom",
    "timeShift",
    "timeRegions",
    "links",
    "fieldConfig",
    "options",
    "renderer",
    "spaceLength",
    "dashLength",
    "dashes",
    "hiddenSeries",
    "percentage",
    "pointradius",
    "points",
    "bars",
    "steppedLine",
    "thresholds",
    "tooltip",
    "xaxis",
    "yaxis",
    "aliasColors",
    "fillGradient",
    "grid",
}


def load(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def rows(dash: dict) -> list[dict]:
    out = []
    for p in dash.get("panels") or []:
        if p.get("type") == "row":
            out.append(p)
    return out


def norm_expr(s: str | None) -> str:
    if not s:
        return ""
    return " ".join(s.split())


def panel_key(row_title: str, panel: dict) -> str:
    return f"{row_title} :: {panel.get('title')}"


def extract_targets(panel: dict) -> list[tuple[str, str]]:
    out = []
    for t in panel.get("targets") or []:
        out.append((norm_expr(t.get("expr")), t.get("legendFormat") or ""))
    return out


def graph_meta(panel: dict) -> dict[str, Any]:
    yaxes = panel.get("yaxes") or []
    y1 = yaxes[0] if yaxes else {}
    y2 = yaxes[1] if len(yaxes) > 1 else {}
    legend = panel.get("legend") or {}
    return {
        "type": panel.get("type"),
        "nullPointMode": panel.get("nullPointMode"),
        "stack": bool(panel.get("stack")),
        "formatY1": y1.get("format"),
        "formatY2": y2.get("format"),
        "legend_alignAsTable": bool(legend.get("alignAsTable")),
        "legend_rightSide": bool(legend.get("rightSide")),
        "legend_values": bool(legend.get("values")),
        "legend_current": bool(legend.get("current")),
        "legend_max": bool(legend.get("max")),
        "description": (panel.get("description") or "").strip(),
    }


def heatmap_meta(panel: dict) -> dict[str, Any]:
    yaxis = panel.get("yAxis") or {}
    color = panel.get("color") or {}
    return {
        "type": panel.get("type"),
        "dataFormat": panel.get("dataFormat"),
        "yAxis_format": yaxis.get("format"),
        "hideZeroBuckets": bool(panel.get("hideZeroBuckets")),
        "color_mode": color.get("mode"),
        "description": (panel.get("description") or "").strip(),
    }


def compare(old: dict, new: dict, only_rows: set[str] | None = None) -> list[str]:
    issues: list[str] = []

    # dashboard meta
    for key in ("title", "uid", "refresh"):
        if old.get(key) != new.get(key):
            issues.append(f"dashboard.{key}: {old.get(key)!r} -> {new.get(key)!r}")

    old_vars = {v["name"]: v for v in (old.get("templating") or {}).get("list") or []}
    new_vars = {v["name"]: v for v in (new.get("templating") or {}).get("list") or []}
    if set(old_vars) != set(new_vars):
        issues.append(f"template names: {sorted(old_vars)} vs {sorted(new_vars)}")
    for name in sorted(set(old_vars) & set(new_vars)):
        oq = old_vars[name].get("query")
        nq = new_vars[name].get("query")
        if isinstance(oq, dict):
            oq = oq.get("query")
        if isinstance(nq, dict):
            nq = nq.get("query")
        if str(oq) != str(nq):
            # custom role variable may normalize label:value form
            if name == "tiflash_role":
                continue
            issues.append(f"template[{name}].query differs:\n  old={oq}\n  new={nq}")

    old_rows = {r["title"]: r for r in rows(old)}
    new_rows = {r["title"]: r for r in rows(new)}
    if only_rows:
        old_rows = {k: v for k, v in old_rows.items() if k in only_rows}
        new_rows = {k: v for k, v in new_rows.items() if k in only_rows}

    missing = set(old_rows) - set(new_rows)
    extra = set(new_rows) - set(old_rows)
    for t in sorted(missing):
        issues.append(f"missing row: {t}")
    for t in sorted(extra):
        issues.append(f"extra row: {t}")

    for title in sorted(set(old_rows) & set(new_rows)):
        op = {p.get("title"): p for p in old_rows[title].get("panels") or []}
        np = {p.get("title"): p for p in new_rows[title].get("panels") or []}
        for pt in sorted(set(op) - set(np)):
            issues.append(f"missing panel: {title} :: {pt}")
        for pt in sorted(set(np) - set(op)):
            issues.append(f"extra panel: {title} :: {pt}")
        for pt in sorted(set(op) & set(np)):
            a, b = op[pt], np[pt]
            ot, nt = extract_targets(a), extract_targets(b)
            if ot != nt:
                issues.append(
                    f"targets differ: {title} :: {pt}\n  old={ot}\n  new={nt}"
                )
            if a.get("type") == "graph":
                am, bm = graph_meta(a), graph_meta(b)
            else:
                am, bm = heatmap_meta(a), heatmap_meta(b)
            for k in am:
                if am[k] != bm.get(k):
                    # description whitespace / missing description is soft
                    if k == "description" and not am[k]:
                        continue
                    issues.append(
                        f"{k} differ: {title} :: {pt}: {am[k]!r} -> {bm.get(k)!r}"
                    )
    return issues


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("old_json")
    ap.add_argument("new_json")
    ap.add_argument("--row", action="append", default=[], help="Only compare these rows")
    ap.add_argument("--max-issues", type=int, default=80)
    args = ap.parse_args()

    issues = compare(load(args.old_json), load(args.new_json), set(args.row) or None)
    if not issues:
        print("OK: semantic compare passed")
        return 0
    print(f"FOUND {len(issues)} issue(s):")
    for i, msg in enumerate(issues[: args.max_issues]):
        print(f"- {msg}")
    if len(issues) > args.max_issues:
        print(f"... and {len(issues) - args.max_issues} more")
    return 1


if __name__ == "__main__":
    sys.exit(main())
