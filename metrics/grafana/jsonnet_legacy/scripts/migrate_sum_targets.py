#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
"""Migrate standard sum / sum(rate) prometheus.target calls to common.expr helpers.

Usage:
  python3 scripts/migrate_sum_targets.py              # all rows
  python3 scripts/migrate_sum_targets.py Server       # one row
  python3 scripts/migrate_sum_targets.py Server Raft  # multiple rows
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / 'tiflash_summary.jsonnet'

SEL = 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"'
PROXY = 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"'

PAT_RATE = re.compile(
    r'^sum\(rate\(([A-Za-z0-9_]+)\{(.+)\}\[([^\]]+)\]\)\)(?: by \(([^)]+)\))?$'
)
PAT_RATE_BY = re.compile(
    r'^sum by \(([^)]+)\) \(rate\(([A-Za-z0-9_]+)\{(.+)\}\[([^\]]+)\]\)\)$'
)
PAT_SUM = re.compile(
    r'^sum\(([A-Za-z0-9_]+)\{(.+)\}\)(?: by \(([^)]+)\))?$'
)
PAT_SUM_BY = re.compile(
    r'^sum by \(([^)]+)\) \(([A-Za-z0-9_]+)\{(.+)\}\)$'
)


def split_labels(sel: str) -> list[str]:
    parts, cur, q = [], [], None
    for i, c in enumerate(sel):
        if q:
            cur.append(c)
            if c == q and (i == 0 or sel[i - 1] != '\\'):
                q = None
        elif c in ('"', "'"):
            q = c
            cur.append(c)
        elif c == ',':
            parts.append(''.join(cur).strip())
            cur = []
        else:
            cur.append(c)
    if cur:
        parts.append(''.join(cur).strip())
    return [p for p in parts if p]


def map_selector(label_body: str):
    labels = split_labels(label_body)
    for name, base in (('common.selector', SEL), ('common.proxySelector', PROXY)):
        base_parts = split_labels(base)
        if labels[: len(base_parts)] == base_parts:
            return name, labels[len(base_parts) :]
    return None, labels


def fmt_by(by_str: str | None) -> list[str]:
    if not by_str:
        return []
    return [b.strip() for b in by_str.split(',') if b.strip()]


def jstr(s: str) -> str:
    return "'" + s.replace('\\', '\\\\').replace("'", "\\'") + "'"


def build_call(kind: str, metric: str, sel_name: str | None, extras: list[str], by_labels: list[str], range_: str | None = None) -> str:
    args = [jstr(metric)]
    if sel_name:
        args.append(sel_name)
        if extras:
            args.append('labels=' + jstr(', '.join(extras)))
    else:
        args.append(jstr(', '.join(extras)))
    if by_labels:
        args.append('by=[' + ', '.join(jstr(b) for b in by_labels) + ']')
    if kind == 'sumRate' and range_ and range_ != '$__rate_interval':
        args.append('range=' + jstr(range_))
    return f'common.expr.{kind}({", ".join(args)})'


def convert_expr(e: str) -> str | None:
    if ' / ' in e or 'histogram' in e or 'irate(' in e:
        return None
    m = PAT_RATE.match(e)
    if m:
        metric, body, rng, by = m.group(1), m.group(2), m.group(3), m.group(4)
        sel_name, extras = map_selector(body)
        return build_call('sumRate', metric, sel_name, extras if sel_name else split_labels(body), fmt_by(by), rng)
    m = PAT_RATE_BY.match(e)
    if m:
        by, metric, body, rng = m.group(1), m.group(2), m.group(3), m.group(4)
        sel_name, extras = map_selector(body)
        return build_call('sumRate', metric, sel_name, extras if sel_name else split_labels(body), fmt_by(by), rng)
    m = PAT_SUM.match(e)
    if m:
        metric, body, by = m.group(1), m.group(2), m.group(3)
        if 'rate(' in e:
            return None
        sel_name, extras = map_selector(body)
        return build_call('sum', metric, sel_name, extras if sel_name else split_labels(body), fmt_by(by))
    m = PAT_SUM_BY.match(e)
    if m:
        by, metric, body = m.group(1), m.group(2), m.group(3)
        if 'rate(' in e:
            return None
        sel_name, extras = map_selector(body)
        return build_call('sum', metric, sel_name, extras if sel_name else split_labels(body), fmt_by(by))
    return None


def extract_targets(s: str):
    out = []
    for m in re.finditer(r'prometheus\.target\(', s):
        i = m.end()
        depth = 1
        while i < len(s) and depth:
            if s[i] == '(':
                depth += 1
            elif s[i] == ')':
                depth -= 1
            i += 1
        out.append((m.start(), i, s[m.start():i]))
    return out


def row_spans(text: str):
    rows = list(re.finditer(r'// --- Row: (.+?) ---', text))
    spans = []
    for i, rm in enumerate(rows):
        if i + 1 < len(rows):
            end = rows[i + 1].start()
        else:
            m = re.search(r'\n(?:grafana\.)?dashboard\.new\(', text)
            if not m:
                raise RuntimeError('dashboard.new not found')
            end = m.start()
        spans.append((rm.group(1), rm.start(), end))
    return spans


def migrate_chunk(chunk: str) -> tuple[str, int]:
    replacements = []
    for tstart, tend, block in extract_targets(chunk):
        em = re.search(r"'((?:\\'|[^'])*)'", block)
        if not em:
            continue
        expr = em.group(1)
        new_expr = convert_expr(expr)
        if not new_expr:
            continue
        lm = re.search(r"legendFormat='((?:\\'|[^'])*)'", block)
        legend = lm.group(1) if lm else '{{instance}}'
        hide = bool(re.search(r'\bhide\s*=\s*true\b', block))
        iv = re.search(r'intervalFactor\s*=\s*(\d+)', block)
        # Prefer indent relative to enclosing .addTarget( (2 more spaces).
        add_m = list(re.finditer(r'^([ \t]*)\.addTarget\(\s*$', chunk[:tstart], re.M))
        if add_m:
            indent = add_m[-1].group(1) + '  '
        else:
            line_start = chunk.rfind('\n', 0, tstart) + 1
            indent = chunk[line_start:tstart]
        lines = [
            f'{indent}common.target(',
            f'{indent}  {new_expr},',
            f'{indent}  {jstr(legend)},',
        ]
        if hide:
            lines.append(f'{indent}  hide=true,')
        if iv and int(iv.group(1)) != 2:
            lines.append(f'{indent}  intervalFactor={iv.group(1)},')
        lines.append(f'{indent})')
        replacements.append((tstart, tend, '\n'.join(lines)))

    new_chunk = chunk
    for tstart, tend, repl in reversed(replacements):
        new_chunk = new_chunk[:tstart] + repl + new_chunk[tend:]
    return new_chunk, len(replacements)


def main():
    # Process rows from end to start so offsets stay valid.
    text = PATH.read_text()
    want = set(sys.argv[1:]) if len(sys.argv) > 1 else None
    spans = row_spans(text)
    total = 0
    for name, start, end in reversed(spans):
        if want is not None and name not in want:
            continue
        new_chunk, n = migrate_chunk(text[start:end])
        if n:
            text = text[:start] + new_chunk + text[end:]
            print(f'{name}: migrated {n}')
            total += n
    PATH.write_text(text)
    print(f'total migrated: {total}')


if __name__ == '__main__':
    main()
