#!/usr/bin/env python3
"""Emit zones.inc from a directory of TZif files compiled from IANA tzdata 2024a."""
import pathlib
import sys

names = [
    "Africa/Monrovia", "America/Goose_Bay", "America/New_York", "America/St_Johns",
    "Antarctica/Vostok", "Asia/Shanghai", "Asia/Singapore", "Australia/Lord_Howe",
    "EST", "Europe/Amsterdam", "Pacific/Apia", "Pacific/Kwajalein",
]
print("// Generated from public-domain IANA tzdata 2024a. See README.md.")
print("// clang-format off")
for index, name in enumerate(names):
    data = (pathlib.Path(sys.argv[1]) / name).read_bytes()
    print(f"constexpr char zone_{index}[] =")
    for start in range(0, len(data), 24):
        print('    "' + ''.join(f'\\x{b:02x}' for b in data[start:start + 24]) + '"')
    print(";")
print("constexpr std::pair<std::string_view, std::string_view> zones[] = {")
for index, name in enumerate(names):
    print(f'    {{"{name}", {{zone_{index}, sizeof(zone_{index}) - 1}}}},')
print("};")
print("// clang-format on")
