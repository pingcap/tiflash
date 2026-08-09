#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
"""Migrate hand-written graphPanel.new panels to common.graph.

Usage:
  python3 scripts/migrate_graph_panels.py              # all rows
  python3 scripts/migrate_graph_panels.py Server Raft  # selected rows
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATH = ROOT / 'tiflash_summary.jsonnet'

SKIP_KEYS = {'lines', 'legend_total', 'bars', 'dashes', 'percentage'}


def find_matching(s: str, open_idx: int) -> int:
    assert s[open_idx] == '('
    i = open_idx + 1
    depth = 1
    while i < len(s) and depth:
        c = s[i]
        if c in ('"', "'"):
            q = c
            i += 1
            while i < len(s):
                if s[i] == '\\':
                    i += 2
                    continue
                if s[i] == q:
                    i += 1
                    break
                i += 1
            continue
        if c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
        i += 1
    return i


def parse_kwargs(body: str) -> dict[str, str]:
    out: dict[str, str] = {}
    i = 0
    n = len(body)
    while i < n:
        while i < n and body[i] in ' \t\n,':
            i += 1
        if i >= n:
            break
        m = re.match(r'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*', body[i:])
        if not m:
            break
        key = m.group(1)
        i += m.end()
        start = i
        if i < n and body[i] in ("'", '"'):
            q = body[i]
            i += 1
            while i < n:
                if body[i] == '\\':
                    i += 2
                    continue
                if body[i] == q:
                    i += 1
                    break
                i += 1
            out[key] = body[start:i]
        elif i < n and body[i] == '{':
            depth = 0
            while i < n:
                if body[i] in ("'", '"'):
                    q = body[i]
                    i += 1
                    while i < n:
                        if body[i] == '\\':
                            i += 2
                            continue
                        if body[i] == q:
                            i += 1
                            break
                        i += 1
                    continue
                if body[i] == '{':
                    depth += 1
                elif body[i] == '}':
                    depth -= 1
                    i += 1
                    if depth == 0:
                        break
                    continue
                i += 1
            out[key] = body[start:i]
        else:
            while i < n and body[i] not in ',)\n':
                i += 1
            out[key] = body[start:i].strip()
        while i < n and body[i] in ' \t\n':
            i += 1
        if i < n and body[i] == ',':
            i += 1
    return out


def strip_new_body(call: str) -> str:
    open_idx = call.index('(')
    end = find_matching(call, open_idx)
    return call[open_idx + 1 : end - 1]


def map_panel_kwargs(kw: dict[str, str]) -> list[str] | None:
    if any(k in kw for k in SKIP_KEYS):
        return None
    if 'title' not in kw:
        return None
    # common.graph always enables table legend; skip panels that intentionally omit it.
    if kw.get('legend_alignAsTable') != 'true' or kw.get('legend_rightSide') != 'true':
        return None
    if kw.get('legend_values') != 'true':
        return None

    args: list[str] = []

    def add(name: str, val: str) -> None:
        args.append(f'{name}={val}')

    if 'description' in kw:
        add('description', kw['description'])
    if 'fill' in kw and kw['fill'] != '1':
        add('fill', kw['fill'])
    if 'linewidth' in kw and kw['linewidth'] != '1':
        add('linewidth', kw['linewidth'])
    if 'decimals' in kw:
        add('decimals', kw['decimals'])
    if 'nullPointMode' in kw and kw['nullPointMode'] != "'null as zero'":
        add('nullPointMode', kw['nullPointMode'])
    if 'points' in kw and kw['points'] != 'false':
        add('points', kw['points'])
    if 'pointradius' in kw and kw['pointradius'] != '5':
        add('pointradius', kw['pointradius'])
    if 'stack' in kw and kw['stack'] != 'false':
        add('stack', kw['stack'])

    if 'legend_current' not in kw:
        add('legendCurrent', 'false')
    elif kw['legend_current'] != 'true':
        add('legendCurrent', kw['legend_current'])

    if 'legend_max' not in kw:
        add('legendMax', 'false')
    elif kw['legend_max'] != 'true':
        add('legendMax', kw['legend_max'])

    if 'legend_avg' in kw and kw['legend_avg'] != 'false':
        add('legendAvg', kw['legend_avg'])
    if 'legend_hideZero' in kw:
        add('legendHideZero', kw['legend_hideZero'])
    if 'legend_hideEmpty' in kw:
        add('legendHideEmpty', kw['legend_hideEmpty'])
    if 'legend_sideWidth' in kw:
        add('sideWidth', kw['legend_sideWidth'])

    if 'legend_sort' not in kw:
        add('legendSort', 'null')
        add('legendSortDesc', 'null')
    else:
        if kw['legend_sort'] != "'max'":
            add('legendSort', kw['legend_sort'])
        if 'legend_sortDesc' in kw and kw['legend_sortDesc'] != 'true':
            add('legendSortDesc', kw['legend_sortDesc'])
        elif 'legend_sortDesc' not in kw:
            add('legendSortDesc', 'null')

    return args


def map_yaxis(kw: dict[str, str], side: str) -> list[str]:
    args: list[str] = []
    fmt = kw.get('format')
    if fmt is None:
        return args
    args.append(f'y{side}={fmt}')
    if 'min' in kw:
        if not (side == 'Left' and kw['min'] == "'0'"):
            args.append(f'y{side}Min={kw["min"]}')
    elif side == 'Left':
        args.append('yLeftMin=null')
    if 'max' in kw:
        args.append(f'y{side}Max={kw["max"]}')
    if 'decimals' in kw:
        args.append(f'y{side}Decimals={kw["decimals"]}')
    if side == 'Right' and 'show' in kw and kw['show'] != 'true':
        args.append(f'yRightShow={kw["show"]}')
    return args


def reindent_block(text: str, new_base: str) -> list[str]:
    raw_lines = text.split('\n')
    # Drop wrapping blank lines
    while raw_lines and not raw_lines[0].strip():
        raw_lines.pop(0)
    while raw_lines and not raw_lines[-1].strip():
        raw_lines.pop()
    nonempty = [ln for ln in raw_lines if ln.strip()]
    if not nonempty:
        return [new_base + text.strip()]
    orig_pad = min(len(ln) - len(ln.lstrip(' ')) for ln in nonempty)
    out = []
    for tl in raw_lines:
        if not tl.strip():
            out.append('')
            continue
        pad = len(tl) - len(tl.lstrip(' '))
        rel = max(pad - orig_pad, 0)
        out.append(new_base + (' ' * rel) + tl.lstrip(' '))
    return out


def migrate_panel(block: str, inner_indent: str) -> str | None:
    if not block.lstrip().startswith('graphPanel.new('):
        return None
    start = block.find('graphPanel.new(')
    open_idx = block.index('(', start)
    new_end = find_matching(block, open_idx)
    new_call = block[start:new_end]
    kw = parse_kwargs(strip_new_body(new_call))
    panel_args = map_panel_kwargs(kw)
    if panel_args is None:
        return None
    title = kw['title']

    rest = block[new_end:]
    targets: list[str] = []
    overrides: list[str] = []
    i = 0
    while True:
        while i < len(rest) and rest[i] in ' \t\n':
            i += 1
        if rest.startswith('.addTarget(', i):
            o = rest.index('(', i)
            e = find_matching(rest, o)
            targets.append(rest[o + 1 : e - 1])
            i = e
            continue
        if rest.startswith('.addSeriesOverride(', i):
            o = rest.index('(', i)
            e = find_matching(rest, o)
            overrides.append(rest[o + 1 : e - 1])
            i = e
            continue
        break

    if not rest.startswith('.resetYaxes()', i):
        return None
    i = rest.index(')', i) + 1

    yaxes = []
    for _ in range(2):
        while i < len(rest) and rest[i] in ' \t\n':
            i += 1
        if not rest.startswith('.addYaxis(', i):
            return None
        o = rest.index('(', i)
        e = find_matching(rest, o)
        ykw = parse_kwargs(rest[o + 1 : e - 1])
        # common.graph does not model axis label / logBase yet.
        if 'label' in ykw or ('logBase' in ykw and ykw['logBase'] != '1'):
            return None
        yaxes.append(ykw)
        i = e

    while i < len(rest) and rest[i] in ' \t\n':
        i += 1
    if i < len(rest) and rest[i:].strip() and not rest[i:].strip().startswith(';'):
        return None

    y_args = map_yaxis(yaxes[0], 'Left') + map_yaxis(yaxes[1], 'Right')
    if overrides:
        ov_lines = []
        for o in overrides:
            ov_lines.extend(reindent_block(o, inner_indent + '  '))
            if not ov_lines[-1].endswith(','):
                ov_lines[-1] += ','
        panel_args.append(
            'seriesOverrides=[\n'
            + '\n'.join(ov_lines)
            + '\n'
            + inner_indent
            + ']'
        )

    i1 = inner_indent
    i2 = inner_indent + '  '
    close_indent = inner_indent[:-2] if len(inner_indent) >= 2 else ''
    lines = ['common.graph(']
    lines.append(f'{i1}{title},')
    lines.append(f'{i1}[')
    for t in targets:
        t_lines = reindent_block(t, i2)
        if not t_lines[-1].rstrip().endswith(','):
            t_lines[-1] = t_lines[-1] + ','
        lines.extend(t_lines)
    lines.append(f'{i1}],')
    for a in panel_args + y_args:
        if a.startswith('seriesOverrides='):
            lines.append(f'{i1}{a},')
        else:
            lines.append(f'{i1}{a},')
    lines.append(f'{close_indent})')
    return '\n'.join(lines)


def find_panel_blocks(chunk: str) -> list[tuple[int, int, str]]:
    out = []
    for m in re.finditer(r'(=\s*)graphPanel\.new\(', chunk):
        expr_start = m.start(1) + len(m.group(1))
        depth = 0
        j = expr_start
        end = None
        while j < len(chunk):
            c = chunk[j]
            if c in ("'", '"'):
                q = c
                j += 1
                while j < len(chunk):
                    if chunk[j] == '\\':
                        j += 2
                        continue
                    if chunk[j] == q:
                        j += 1
                        break
                    j += 1
                continue
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
            elif c == ';' and depth == 0:
                end = j
                break
            j += 1
        if end is None:
            continue
        out.append((expr_start, end, chunk[expr_start:end]))
    return out


def row_spans(text: str):
    rows = list(re.finditer(r'// --- Row: (.+?) ---', text))
    spans = []
    for i, rm in enumerate(rows):
        if i + 1 < len(rows):
            end = rows[i + 1].start()
        else:
            m = re.search(r'\n(?:grafana\.)?dashboard\.new\(', text)
            end = m.start() if m else len(text)
        spans.append((rm.group(1), rm.start(), end))
    return spans


def migrate_chunk(chunk: str) -> tuple[str, int, int]:
    blocks = find_panel_blocks(chunk)
    migrated = 0
    skipped = 0
    new_chunk = chunk
    for start, end, block in reversed(blocks):
        line_start = chunk.rfind('\n', 0, start) + 1
        local_indent = re.match(r'[ \t]*', chunk[line_start:]).group(0)
        inner_indent = local_indent + '  '
        repl = migrate_panel(block, inner_indent)
        if repl is None:
            skipped += 1
            continue
        new_chunk = new_chunk[:start] + repl + new_chunk[end:]
        migrated += 1
    return new_chunk, migrated, skipped


def main():
    text = PATH.read_text()
    want = set(sys.argv[1:]) if len(sys.argv) > 1 else None
    total_m = total_s = 0
    for name, start, end in reversed(row_spans(text)):
        if want is not None and name not in want:
            continue
        new_chunk, m, s = migrate_chunk(text[start:end])
        if m or s:
            text = text[:start] + new_chunk + text[end:]
            print(f'{name}: migrated {m}, skipped {s}')
            total_m += m
            total_s += s
    PATH.write_text(text)
    print(f'total migrated: {total_m}, skipped: {total_s}')


if __name__ == '__main__':
    main()
