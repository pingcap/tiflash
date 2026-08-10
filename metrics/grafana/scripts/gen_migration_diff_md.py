#!/usr/bin/env python3
"""Generate a markdown migration diff report for TiFlash Summary dashboard.

Compares pre-migration JSON (e.g. jsonnet_legacy original) against the
grafanalib-generated tiflash_summary.json, reusing compare_dashboards
normalization helpers.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# Allow `python3 scripts/gen_migration_diff_md.py` from metrics/grafana/
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from compare_dashboards import (  # noqa: E402
    extract_targets,
    graph_meta,
    heatmap_meta,
    load,
    rows,
)

TRUNCATE = 160
FIXED_RANGES = ("1m", "30s", "5m", "2m", "10m", "15s", "1m0s")


@dataclass
class FieldDiff:
    key: str
    old: Any
    new: Any


@dataclass
class PanelReport:
    row: str
    title: str
    status: str  # unchanged | changed | added | removed
    tags: list[str] = field(default_factory=list)
    field_diffs: list[FieldDiff] = field(default_factory=list)
    target_notes: list[str] = field(default_factory=list)
    old_targets: list[tuple[str, str, bool]] = field(default_factory=list)
    new_targets: list[tuple[str, str, bool]] = field(default_factory=list)


def _trunc(s: str, n: int = TRUNCATE) -> str:
    s = s.replace("\n", " ")
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


def _rel_y_map(panels: dict[str, dict]) -> dict[str, int]:
    ys = [(p.get("gridPos") or {}).get("y") or 0 for p in panels.values()]
    base = min(ys) if ys else 0
    return {
        name: ((p.get("gridPos") or {}).get("y") or 0) - base for name, p in panels.items()
    }


def _panel_meta(panel: dict) -> dict[str, Any]:
    if panel.get("type") == "graph":
        return graph_meta(panel)
    if panel.get("type") == "heatmap":
        return heatmap_meta(panel)
    # fallback for other types
    return {
        "type": panel.get("type"),
        "description": (panel.get("description") or "").strip(),
    }


def _ignore_meta_keys(am: dict, bm: dict) -> set[str]:
    ignore: set[str] = set()
    # Hidden right Y-axis fields are not user-visible.
    if am.get("type") == "graph" and not am.get("showY2") and not bm.get("showY2"):
        ignore.update({"formatY2", "labelY2", "logBaseY2", "minY2", "showY2"})
    # Also treat original showY2=True with no series on yaxis=2 as cosmetic when
    # new hides the right axis — classified via tags, but still report showY2.
    return ignore


def _expr_join(targets: list[tuple[str, str, bool]]) -> str:
    return " | ".join(t[0] for t in targets)


def _has_fixed_range(expr: str) -> Optional[str]:
    for r in FIXED_RANGES:
        if f"[{r}]" in expr:
            return r
    return None


def classify_target_diff(
    old_t: list[tuple[str, str, bool]],
    new_t: list[tuple[str, str, bool]],
) -> list[str]:
    tags: list[str] = []
    if not old_t and not new_t:
        return tags
    oexpr = _expr_join(old_t)
    nexpr = _expr_join(new_t)

    fixed = _has_fixed_range(oexpr)
    if fixed and "[$__rate_interval]" in nexpr:
        tags.append("rate_interval")

    if (
        'instance=~"$tiflash_role"' in oexpr
        and 'instance=~"$proxy_instance"' not in oexpr
        and 'instance=~"$proxy_instance"' in nexpr
    ):
        tags.append("proxy_instance_selector")

    # Duration / histogram quantile expansion (S3-style)
    o_hq = sum(1 for e, _, _ in old_t if "histogram_quantile" in e)
    n_hq = sum(1 for e, _, _ in new_t if "histogram_quantile" in e)
    if o_hq > 0 and n_hq >= o_hq and len(new_t) > len(old_t):
        tags.append("duration_quantiles")
    elif o_hq > 0 and n_hq > 0 and [t[0] for t in old_t] != [t[0] for t in new_t]:
        # quantile / hide reshuffle without count change
        if any("histogram_quantile" in e for e, _, _ in new_t):
            # check hide pattern changes
            if [t[2] for t in old_t] != [t[2] for t in new_t] or len(old_t) != len(new_t):
                tags.append("duration_quantiles")

    # legend/hide only
    if [t[0] for t in old_t] == [t[0] for t in new_t] and old_t != new_t:
        tags.append("legend_or_hide")

    # After stripping rate interval, exprs equal?
    def strip_range(s: str) -> str:
        return re.sub(r"\[(?:1m|30s|5m|2m|10m|15s|1m0s|\$__rate_interval)\]", "[R]", s)

    if (
        "rate_interval" in tags
        and strip_range(oexpr) == strip_range(nexpr)
        and [t[1:] for t in old_t] == [t[1:] for t in new_t]
    ):
        # purely rate interval
        pass

    if not tags:
        tags.append("other")
    return tags


def classify_panel(
    row: str,
    title: str,
    old_p: Optional[dict],
    new_p: Optional[dict],
) -> PanelReport:
    if old_p is None and new_p is not None:
        tags = ["panel_split"] if "Meta Cache" in title else ["added"]
        return PanelReport(
            row=row,
            title=title,
            status="added",
            tags=tags,
            new_targets=extract_targets(new_p),
        )
    if new_p is None and old_p is not None:
        tags = ["panel_split"] if title == "Columnar Meta Cache Gauge" else ["removed"]
        return PanelReport(
            row=row,
            title=title,
            status="removed",
            tags=tags,
            old_targets=extract_targets(old_p),
        )

    assert old_p is not None and new_p is not None
    report = PanelReport(row=row, title=title, status="unchanged")
    old_t = extract_targets(old_p)
    new_t = extract_targets(new_p)
    report.old_targets = old_t
    report.new_targets = new_t

    if old_t != new_t:
        report.status = "changed"
        ttags = classify_target_diff(old_t, new_t)
        report.tags.extend(ttags)
        report.field_diffs.append(
            FieldDiff("targets", f"{len(old_t)} series", f"{len(new_t)} series")
        )
        # brief notes for first differing target
        for i, (a, b) in enumerate(zip(old_t, new_t)):
            if a != b:
                report.target_notes.append(
                    f"t{i}: `{_trunc(a[0])}` → `{_trunc(b[0])}` "
                    f"(legend `{a[1]}`→`{b[1]}`, hide {a[2]}→{b[2]})"
                )
                if len(report.target_notes) >= 3:
                    break
        if len(new_t) > len(old_t):
            report.target_notes.append(
                f"+{len(new_t) - len(old_t)} extra target(s) in new"
            )
        elif len(old_t) > len(new_t):
            report.target_notes.append(
                f"-{len(old_t) - len(new_t)} target(s) removed in new"
            )

    am, bm = _panel_meta(old_p), _panel_meta(new_p)
    ignore = _ignore_meta_keys(am, bm)
    style_keys = {
        "nullPointMode",
        "stack",
        "legend_alignAsTable",
        "legend_rightSide",
        "legend_values",
        "legend_current",
        "legend_max",
        "description",
    }
    y_left_keys = {
        "formatY1",
        "labelY1",
        "showY1",
        "logBaseY1",
        "minY1",
    }
    y_right_keys = {
        "formatY2",
        "labelY2",
        "showY2",
        "logBaseY2",
        "minY2",
    }

    for k in am:
        if k in ignore:
            continue
        if am[k] != bm.get(k):
            if k == "description" and not am[k]:
                continue
            report.status = "changed"
            report.field_diffs.append(FieldDiff(k, am[k], bm.get(k)))
            if k in style_keys:
                report.tags.append("style_default")
            elif k in y_right_keys:
                # original often had showY2=True even when unused
                report.tags.append("hidden_right_axis")
            elif k in y_left_keys or k == "type":
                report.tags.append("yaxis_visible")
            else:
                report.tags.append("other")

    # Special: old showY2 True, new False, and no other right-axis usage — still tag
    if (
        am.get("type") == "graph"
        and am.get("showY2")
        and not bm.get("showY2")
        and "hidden_right_axis" not in report.tags
    ):
        # already added via field diff if showY2 compared; if ignored somehow skip
        pass

    # When old showY2=True and new showY2=False, compare still reports showY2 —
    # classify as hidden_right_axis (intentional cleanup). formatY2/minY2 also.
    # Override: if ONLY right-axis cosmetic + maybe layout, keep tags.

    # Layout
    ag = old_p.get("gridPos") or {}
    bg = new_p.get("gridPos") or {}
    layout_changed = False
    for gk in ("x", "w", "h"):
        if ag.get(gk) != bg.get(gk):
            report.status = "changed"
            layout_changed = True
            report.field_diffs.append(FieldDiff(f"gridPos.{gk}", ag.get(gk), bg.get(gk)))
    # relative y compared by caller via maps — handled in compare_row

    if layout_changed:
        report.tags.append("layout_repack")

    # Deduplicate tags preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for t in report.tags:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    report.tags = uniq
    return report


def compare_row(
    row_title: str, old_row: dict, new_row: dict
) -> list[PanelReport]:
    op = {p.get("title"): p for p in old_row.get("panels") or []}
    np = {p.get("title"): p for p in new_row.get("panels") or []}
    old_rel = _rel_y_map(op)
    new_rel = _rel_y_map(np)

    titles = sorted(set(op) | set(np), key=lambda t: (
        0 if t in op and t in np else 1 if t in np else 2,
        (op.get(t) or np.get(t) or {}).get("gridPos", {}).get("y") or 0,
        (op.get(t) or np.get(t) or {}).get("gridPos", {}).get("x") or 0,
        t or "",
    ))

    reports: list[PanelReport] = []
    for pt in titles:
        if pt is None:
            continue
        r = classify_panel(row_title, pt, op.get(pt), np.get(pt))
        if pt in op and pt in np:
            if old_rel.get(pt) != new_rel.get(pt):
                r.status = "changed"
                r.field_diffs.append(
                    FieldDiff("gridPos.y(relative)", old_rel.get(pt), new_rel.get(pt))
                )
                if "layout_repack" not in r.tags:
                    r.tags.append("layout_repack")
        reports.append(r)
    return reports


def dashboard_level_diff(old: dict, new: dict) -> list[str]:
    lines: list[str] = []
    for key in ("title", "uid", "refresh", "timezone", "graphTooltip", "schemaVersion"):
        if old.get(key) != new.get(key):
            lines.append(f"- `{key}`: `{old.get(key)!r}` → `{new.get(key)!r}`")
        else:
            lines.append(f"- `{key}`: unchanged (`{old.get(key)!r}`)")

    ot = old.get("tags") or []
    nt = new.get("tags") or []
    if ot != nt:
        lines.append(f"- `tags`: `{ot!r}` → `{nt!r}`")
    else:
        lines.append(f"- `tags`: unchanged (`{ot!r}`)")

    otime = old.get("time") or {}
    ntime = new.get("time") or {}
    if otime != ntime:
        lines.append(f"- `time`: `{otime!r}` → `{ntime!r}`")
    else:
        lines.append(f"- `time`: unchanged (`{otime!r}`)")

    ok, nk = set(old), set(new)
    only_old = sorted(ok - nk)
    only_new = sorted(nk - ok)
    if only_old:
        lines.append(f"- top-level keys only in **old**: {', '.join(f'`{k}`' for k in only_old)}")
    if only_new:
        lines.append(f"- top-level keys only in **new**: {', '.join(f'`{k}`' for k in only_new)}")

    old_vars = {v["name"]: v for v in (old.get("templating") or {}).get("list") or []}
    new_vars = {v["name"]: v for v in (new.get("templating") or {}).get("list") or []}
    lines.append(
        f"- templating names: old={sorted(old_vars)} new={sorted(new_vars)}"
    )
    if set(old_vars) != set(new_vars):
        lines.append(
            f"  - name set differs: only_old={sorted(set(old_vars)-set(new_vars))} "
            f"only_new={sorted(set(new_vars)-set(old_vars))}"
        )
    for name in sorted(set(old_vars) & set(new_vars)):
        oq = old_vars[name].get("query")
        nq = new_vars[name].get("query")
        if isinstance(oq, dict):
            oq = oq.get("query")
        if isinstance(nq, dict):
            nq = nq.get("query")
        if str(oq) != str(nq):
            lines.append(
                f"  - `template[{name}].query` differs:\n"
                f"    - old: `{_trunc(str(oq), 200)}`\n"
                f"    - new: `{_trunc(str(nq), 200)}`"
            )
        else:
            lines.append(f"  - `template[{name}].query`: unchanged")
    return lines


def count_panels(dash: dict) -> tuple[int, int]:
    rs = rows(dash)
    n = sum(len(r.get("panels") or []) for r in rs)
    return len(rs), n


KNOWN_INTENTIONAL = [
    (
        "rate_interval",
        "固定 scrape range（`[1m]` / `[30s]` / `[5m]` 等）改为 Grafana "
        "`[$__rate_interval]`，随 dashboard 刷新间隔自适应。",
    ),
    (
        "proxy_instance_selector",
        "Threads CPU 等 proxy 指标在 selector 中补上 `instance=~\"$proxy_instance\"`，"
        "与其它 proxy 面板一致。",
    ),
    (
        "duration_quantiles",
        "Duration 直方图面板收敛为 S3-style quantile 集（max/9999/999/99/80/avg）"
        "及默认 hide 可见性。",
    ),
    (
        "hidden_right_axis",
        "单轴面板隐藏右 Y 轴（`showY2: false`）；隐藏轴上的 `formatY2`/`minY2` "
        "漂移不影响显示。",
    ),
    (
        "panel_split",
        "`Columnar Meta Cache Gauge` 拆成 `Entries` + `Weighted Size`，"
        "最后一行三等分展示。",
    ),
    (
        "layout_repack",
        "Layout 由 `Layout.row` 均分宽度/相对 y 重排；部分 panel 的 `gridPos` 变化。",
    ),
    (
        "style_default",
        "样式收敛到 `graph_panel` 默认（如 `nullPointMode`、legend 表头、"
        "Threads 去掉 points/decimals 等）。",
    ),
    (
        "legend_or_hide",
        "仅 legendFormat 或 hide 标志变化（PromQL 表达式不变）。",
    ),
]


def render_markdown(
    old_path: str,
    new_path: str,
    old: dict,
    new: dict,
    all_reports: list[PanelReport],
) -> str:
    old_rows_n, old_panels_n = count_panels(old)
    new_rows_n, new_panels_n = count_panels(new)

    by_status = Counter(r.status for r in all_reports)
    by_tag = Counter(t for r in all_reports for t in r.tags)

    out: list[str] = []
    out.append("# TiFlash Summary grafanalib 迁移差异说明")
    out.append("")
    out.append("本文档由 `scripts/gen_migration_diff_md.py` 自动生成，对比：")
    out.append("")
    out.append(f"- **修改前**：`{old_path}`")
    out.append(f"- **修改后**：`{new_path}`")
    out.append("")
    out.append("## 1. 总览")
    out.append("")
    out.append("| | old | new |")
    out.append("|---|---:|---:|")
    out.append(f"| rows | {old_rows_n} | {new_rows_n} |")
    out.append(f"| panels | {old_panels_n} | {new_panels_n} |")
    out.append("")
    out.append("### Panel 状态统计")
    out.append("")
    out.append("| status | count |")
    out.append("|---|---:|")
    for s in ("unchanged", "changed", "added", "removed"):
        out.append(f"| `{s}` | {by_status.get(s, 0)} |")
    out.append("")
    out.append("### 差异标签统计（panel 可多标签）")
    out.append("")
    out.append("| tag | count |")
    out.append("|---|---:|")
    for tag, n in by_tag.most_common():
        out.append(f"| `{tag}` | {n} |")
    out.append("")

    out.append("## 2. Dashboard 级定义")
    out.append("")
    out.extend(dashboard_level_diff(old, new))
    out.append("")

    out.append("## 3. Intentional 变更目录")
    out.append("")
    out.append("下列差异为迁移中的预期行为，验收时一般可视为非回归：")
    out.append("")
    for tag, desc in KNOWN_INTENTIONAL:
        out.append(f"- **`{tag}`**（{by_tag.get(tag, 0)} panels）：{desc}")
    out.append("")
    out.append(
        "另外：Threads IO 单位 `Bps`→`binBps`（IEC）；`yaxis()` 强制禁止 SI 字节单位。"
    )
    out.append("")

    # Row summary table
    out.append("## 4. 逐 Row 摘要")
    out.append("")
    out.append("| Row | unchanged | changed | added | removed |")
    out.append("|---|---:|---:|---:|---:|")
    reports_by_row: dict[str, list[PanelReport]] = defaultdict(list)
    for r in all_reports:
        reports_by_row[r.row].append(r)

    old_row_order = [r["title"] for r in rows(old)]
    for row_title in old_row_order:
        rs = reports_by_row[row_title]
        c = Counter(x.status for x in rs)
        out.append(
            f"| {row_title} | {c.get('unchanged', 0)} | {c.get('changed', 0)} | "
            f"{c.get('added', 0)} | {c.get('removed', 0)} |"
        )
    out.append("")

    out.append("## 5. 逐 Row / Panel 明细")
    out.append("")
    for row_title in old_row_order:
        rs = reports_by_row[row_title]
        out.append(f"### {row_title}")
        out.append("")
        out.append("| Panel | status | tags |")
        out.append("|---|---|---|")
        for r in rs:
            tags = ", ".join(f"`{t}`" for t in r.tags) if r.tags else "—"
            out.append(f"| {r.title} | `{r.status}` | {tags} |")
        out.append("")

        interesting = [r for r in rs if r.status != "unchanged"]
        if not interesting:
            out.append("_本 row 全部 panel 语义对齐（或仅被忽略的隐藏右轴字段）。_")
            out.append("")
            continue

        for r in interesting:
            out.append(f"#### {r.title}")
            out.append("")
            out.append(f"- **status**: `{r.status}`")
            if r.tags:
                out.append(f"- **tags**: {', '.join(f'`{t}`' for t in r.tags)}")
            if r.field_diffs:
                out.append("- **field diffs**:")
                for fd in r.field_diffs:
                    out.append(f"  - `{fd.key}`: `{fd.old!r}` → `{fd.new!r}`")
            if r.target_notes:
                out.append("- **target notes**:")
                for note in r.target_notes:
                    out.append(f"  - {note}")
            out.append("")

    out.append("## 6. 附录：含 `other` 标签或未充分归类的 panel")
    out.append("")
    appendix_other = [r for r in all_reports if "other" in r.tags or "yaxis_visible" in r.tags]
    if not appendix_other:
        out.append("_无未归类差异。_")
    else:
        out.append(
            f"共 {len(appendix_other)} 个 panel 带有 `other` / `yaxis_visible`，"
            "建议人工确认："
        )
        out.append("")
        out.append("| Row | Panel | tags | field keys |")
        out.append("|---|---|---|---|")
        for r in appendix_other:
            keys = ", ".join(fd.key for fd in r.field_diffs) or "—"
            tags = ", ".join(f"`{t}`" for t in r.tags)
            out.append(f"| {r.row} | {r.title} | {tags} | {keys} |")
    out.append("")
    out.append("---")
    out.append("")
    out.append(
        f"_Generated by `scripts/gen_migration_diff_md.py`. "
        f"changed={by_status.get('changed', 0)}, "
        f"added={by_status.get('added', 0)}, "
        f"removed={by_status.get('removed', 0)}, "
        f"appendix={len(appendix_other)}._"
    )
    out.append("")
    return "\n".join(out)


def build_reports(old: dict, new: dict) -> list[PanelReport]:
    old_rows = {r["title"]: r for r in rows(old)}
    new_rows = {r["title"]: r for r in rows(new)}
    all_reports: list[PanelReport] = []
    for title in [r["title"] for r in rows(old)]:
        if title not in new_rows:
            # whole row missing — emit removed panels
            for p in old_rows[title].get("panels") or []:
                all_reports.append(
                    PanelReport(
                        row=title,
                        title=p.get("title") or "?",
                        status="removed",
                        tags=["removed"],
                        old_targets=extract_targets(p),
                    )
                )
            continue
        all_reports.extend(compare_row(title, old_rows[title], new_rows[title]))
    for title in sorted(set(new_rows) - set(old_rows)):
        for p in new_rows[title].get("panels") or []:
            all_reports.append(
                PanelReport(
                    row=title,
                    title=p.get("title") or "?",
                    status="added",
                    tags=["added"],
                    new_targets=extract_targets(p),
                )
            )
    return all_reports


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("old_json")
    ap.add_argument("new_json")
    ap.add_argument(
        "-o",
        "--output",
        default="GRAFANALIB_MIGRATION_DIFF.md",
        help="Output markdown path (default: GRAFANALIB_MIGRATION_DIFF.md)",
    )
    args = ap.parse_args()

    old = load(args.old_json)
    new = load(args.new_json)
    reports = build_reports(old, new)
    md = render_markdown(args.old_json, args.new_json, old, new, reports)
    out_path = Path(args.output)
    out_path.write_text(md)
    print(f"Wrote {out_path} ({len(md)} bytes, {len(reports)} panels)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
