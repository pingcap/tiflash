// Copyright 2023 PingCAP, Inc.
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

#pragma once

#include <cstdint>

#define CLICKHOUSE_DICTIONARY_LIBRARY_API 1

namespace ClickHouseLibrary
{
using CString = const char *;
using ColumnName = CString;
using ColumnNames = ColumnName[];

struct CStrings
{
    CString * data = nullptr;
    uint64_t size = 0;
};

struct VectorUInt64
{
    const uint64_t * data = nullptr;
    uint64_t size = 0;
};

struct ColumnsUInt64
{
    VectorUInt64 * data = nullptr;
    uint64_t size = 0;
};

struct Field
{
    const void * data = nullptr;
    uint64_t size = 0;
};

struct Row
{
    const Field * data = nullptr;
    uint64_t size = 0;
};

struct Table
{
    const Row * data = nullptr;
    uint64_t size = 0;
    uint64_t error_code = 0; // 0 = ok; !0 = error, with message in error_string
    const char * error_string = nullptr;
};
} // namespace ClickHouseLibrary
