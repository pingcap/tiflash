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
#include <TiDB/Schema/TiDB_fwd.h>
#include <common/StringRef.h>

namespace DB
{
NamesAndTypes genNamesAndTypesForExchangeReceiver(const TiDBTableScan & table_scan);
NamesAndTypes genNamesAndTypesForTableScan(const TiDBTableScan & table_scan);
String genNameForExchangeReceiver(Int32 col_index);
String genNameForCTESource(Int32 cte_id, Int32 col_index);

NamesAndTypes genNamesAndTypes(const TiDBTableScan & table_scan, const StringRef & column_prefix);
NamesAndTypes genNamesAndTypes(const TiDB::ColumnInfos & column_infos, const StringRef & column_prefix);
ColumnsWithTypeAndName getColumnWithTypeAndName(const NamesAndTypes & names_and_types);
NamesAndTypes toNamesAndTypes(const DAGSchema & dag_schema);

// The column defines, `extra table id index` and `generated columns info` for disaggregated read.
std::tuple<DM::ColumnDefinesPtr, int, std::vector<std::tuple<UInt64, String, DataTypePtr>>> genColumnDefinesForDisaggregatedRead(
    const TiDBTableScan & table_scan);

// Map filters against the original storage projection, before casts discard column IDs.
google::protobuf::RepeatedPtrField<tipb::Expr> remapColumnarFilterConditions(
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filter_conditions,
    const TiDB::ColumnInfos & scan_columns,
    const Block & early_block);

// Columnar keeps MVCC versions outside its business-column schema.
ColumnID getStorageColumnIDForColumnarRead(ColumnID column_id);
tipb::Executor genTableScanForColumnarRead(const TiDBTableScan & table_scan);
tipb::TableInfo genTableInfoForColumnarRead(const TiDBTableScan & table_scan);
std::tuple<DM::ColumnDefinesPtr, int> genColumnDefinesForDisaggregatedReadThroughColumnar(
    const TiDBTableScan & table_scan);

} // namespace DB
