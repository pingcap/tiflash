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
#include <Storages/DeltaMerge/ColumnDefine_fwd.h>
#include <common/StringRef.h>

namespace DB
{
NamesAndTypes genNamesAndTypesForExchangeReceiver(const TiDBTableScan & table_scan);
NamesAndTypes genNamesAndTypesForTableScan(const TiDBTableScan & table_scan);
String genNameForExchangeReceiver(Int32 col_index);

NamesAndTypes genNamesAndTypes(const TiDBTableScan & table_scan, const StringRef & column_prefix);
NamesAndTypes genNamesAndTypes(const ColumnInfos & column_infos, const StringRef & column_prefix);
ColumnsWithTypeAndName getColumnWithTypeAndName(const NamesAndTypes & names_and_types);
NamesAndTypes toNamesAndTypes(const DAGSchema & dag_schema);

// The column defines and `extra table id index`
std::tuple<DM::ColumnDefinesPtr, int> genColumnDefinesForDisaggregatedRead(const TiDBTableScan & table_scan);

} // namespace DB
