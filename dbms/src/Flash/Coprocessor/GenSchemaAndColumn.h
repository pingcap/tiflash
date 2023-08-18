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

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Storages/Transaction/TiDB.h>
#include <common/StringRef.h>

namespace DB
{
NamesAndTypes genNamesAndTypes(const TiDBTableScan & table_scan, const StringRef & column_prefix);
ColumnsWithTypeAndName getColumnWithTypeAndName(const NamesAndTypes & names_and_types);
NamesAndTypes toNamesAndTypes(const DAGSchema & dag_schema);
<<<<<<< HEAD
=======

namespace DM
{
struct ColumnDefine;
using ColumnDefinesPtr = std::shared_ptr<std::vector<ColumnDefine>>;
} // namespace DM

// The column defines and `extra table id index`
std::tuple<DM::ColumnDefinesPtr, int> genColumnDefinesForDisaggregatedRead(const TiDBTableScan & table_scan);

>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
} // namespace DB
