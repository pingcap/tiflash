#!/usr/bin/env python3
"""Semantically compare two Grafana dashboard JSON files.

Ignores volatile fields (id, version, iteration, ...) and focuses on
row/panel titles, PromQL expr, legendFormat, units, and key display flags.
"""

from __future__ import annotations

import argparse
import json
import re
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


_AGGR_OPS = ("sum", "min", "max", "avg", "count", "group", "stddev", "stdvar")


def _matching_paren(s: str, open_idx: int) -> int:
    """Return index of ')' matching s[open_idx]=='(', or -1."""
    depth = 0
    for i in range(open_idx, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _sort_by_labels(labels: str) -> str:
    parts = [p for p in (x.strip() for x in labels.split(",")) if p]
    return ",".join(sorted(parts))


def _peel_double_parens(s: str) -> str:
    """Replace ((inner)) with (inner) at any nesting level (one pass)."""
    out: list[str] = []
    i = 0
    while i < len(s):
        if s[i] == "(":
            j = _matching_paren(s, i)
            if j > i + 1 and s[i + 1] == "(" and _matching_paren(s, i + 1) == j - 1:
                out.append("(")
                out.append(s[i + 2 : j - 1])
                out.append(")")
                i = j + 1
                continue
            out.append(s[i : j + 1] if j >= 0 else s[i])
            i = j + 1 if j >= 0 else i + 1
            continue
        out.append(s[i])
        i += 1
    return "".join(out)


def norm_expr(s: str | None) -> str:
    """Normalize PromQL for semantic compare (whitespace / by-label order / parens)."""
    if not s:
        return ""
    # PromQL is whitespace-insensitive for our purposes.
    s = re.sub(r"\s+", "", s)

    # op by (labels) (expr)  ->  op(expr) by (labels)
    changed = True
    while changed:
        changed = False
        for op in _AGGR_OPS:
            token = f"{op}by("
            start = 0
            while True:
                idx = s.find(token, start)
                if idx < 0:
                    break
                labels_open = idx + len(op) + 2  # '(' after by
                labels_close = _matching_paren(s, labels_open)
                if labels_close < 0 or labels_close + 1 >= len(s) or s[labels_close + 1] != "(":
                    start = idx + 1
                    continue
                expr_open = labels_close + 1
                expr_close = _matching_paren(s, expr_open)
                if expr_close < 0:
                    start = idx + 1
                    continue
                labels = _sort_by_labels(s[labels_open + 1 : labels_close])
                expr = s[expr_open + 1 : expr_close]
                s = s[:idx] + f"{op}({expr})by({labels})" + s[expr_close + 1 :]
                changed = True
                break
            if changed:
                break

    # Sort labels inside by (...)
    def _sort_by_clause(m: re.Match[str]) -> str:
        return f"by({_sort_by_labels(m.group(1))})"

    s = re.sub(r"by\(([^)]*)\)", _sort_by_clause, s)

    # Sort label matchers inside metric{...} (order is semantically irrelevant).
    def _sort_braces(m: re.Match[str]) -> str:
        return "{" + _sort_by_labels(m.group(1)) + "}"

    s = re.sub(r"\{([^{}]*)\}", _sort_braces, s)

    # Peel redundant parens wrapping a trailing function argument: f(a,(x)) -> f(a,x)
    changed = True
    while changed:
        changed = False
        i = 0
        while i < len(s) - 1:
            if s[i : i + 2] == ",(":
                j = _matching_paren(s, i + 1)
                if j > 0 and j + 1 < len(s) and s[j + 1] == ")":
                    s = s[: i + 1] + s[i + 2 : j] + s[j + 1 :]
                    changed = True
                    break
            i += 1

    # Peel redundant ((...)) introduced by Expr.__str__
    prev = None
    while prev != s:
        prev = s
        s = _peel_double_parens(s)

    # Peel a single outer wrap when the entire expression is parenthesized.
    while s.startswith("(") and _matching_paren(s, 0) == len(s) - 1:
        s = s[1:-1]

    return s


def panel_key(row_title: str, panel: dict) -> str:
    return f"{row_title} :: {panel.get('title')}"


def extract_targets(panel: dict) -> list[tuple[str, str, bool]]:
    out = []
    for t in panel.get("targets") or []:
        out.append(
            (
                norm_expr(t.get("expr")),
                t.get("legendFormat") or "",
                bool(t.get("hide")),
            )
        )
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
        "labelY1": y1.get("label") or None,
        "labelY2": y2.get("label") or None,
        "showY1": y1.get("show", True),
        "showY2": y2.get("show", True),
        "logBaseY1": y1.get("logBase", 1),
        "logBaseY2": y2.get("logBase", 1),
        "minY1": None if y1.get("min") in (None, "") else str(y1.get("min")),
        "minY2": None if y2.get("min") in (None, "") else str(y2.get("min")),
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

        def rel_y_map(panels: dict) -> dict[str, int]:
            ys = [(p.get("gridPos") or {}).get("y") or 0 for p in panels.values()]
            base = min(ys) if ys else 0
            return {
                name: ((p.get("gridPos") or {}).get("y") or 0) - base
                for name, p in panels.items()
            }

        old_rel_y = rel_y_map(op)
        new_rel_y = rel_y_map(np)

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
            # Hidden right Y-axis fields are not user-visible; ignore cosmetic drift.
            ignore = set()
            if am.get("type") == "graph" and not am.get("showY2") and not bm.get("showY2"):
                ignore.update({"formatY2", "labelY2", "logBaseY2", "minY2", "showY2"})
            for k in am:
                if k in ignore:
                    continue
                if am[k] != bm.get(k):
                    # description whitespace / missing description is soft
                    if k == "description" and not am[k]:
                        continue
                    issues.append(
                        f"{k} differ: {title} :: {pt}: {am[k]!r} -> {bm.get(k)!r}"
                    )
            ag = a.get("gridPos") or {}
            bg = b.get("gridPos") or {}
            for gk in ("x", "w", "h"):
                if ag.get(gk) != bg.get(gk):
                    issues.append(
                        f"gridPos.{gk} differ: {title} :: {pt}: "
                        f"{ag.get(gk)!r} -> {bg.get(gk)!r}"
                    )
            if old_rel_y.get(pt) != new_rel_y.get(pt):
                issues.append(
                    f"gridPos.y(relative) differ: {title} :: {pt}: "
                    f"{old_rel_y.get(pt)!r} -> {new_rel_y.get(pt)!r}"
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
