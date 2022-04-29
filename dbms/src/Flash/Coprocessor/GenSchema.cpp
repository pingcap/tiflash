// Copyright 2022 PingCAP, Ltd.
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
#include <Flash/Coprocessor/GenSchema.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
ColumnsWithTypeAndName getColumnWithTypeAndName(const DAGSchema & schema)
{
    std::vector<DB::ColumnWithTypeAndName> column_with_type_and_names;
    column_with_type_and_names.reserve(schema.size());
    for (const auto & col : schema)
    {
        auto type = getDataTypeByColumnInfoForComputingLayer(col.second);
        column_with_type_and_names.push_back(DB::ColumnWithTypeAndName(type, col.first));
    }
    return column_with_type_and_names;
}

ColumnsWithTypeAndName getColumnWithTypeAndName(const NamesAndTypes & names_and_types)
{
    std::vector<DB::ColumnWithTypeAndName> column_with_type_and_names;
    column_with_type_and_names.reserve(names_and_types.size());
    for (const auto & col : names_and_types)
    {
        column_with_type_and_names.push_back(DB::ColumnWithTypeAndName(col.type, col.name));
    }
    return column_with_type_and_names;
}

DAGSchema genSchemaFromTableScan(const tipb::TableScan & table_scan)
{
    DAGSchema schema;
    schema.reserve(table_scan.columns_size());
    for (Int32 i = 0; i < table_scan.columns_size(); ++i)
    {
        String name = "mock_table_scan_" + std::to_string(i);
        TiDB::ColumnInfo column_info;
        auto const & ci = table_scan.columns(i);
        column_info.tp = static_cast<TiDB::TP>(ci.tp());
        column_info.id = ci.column_id();
        column_info.name = name;
        schema.push_back({name, column_info});
    }
    return schema;
}

NamesAndTypes genNamesAndTypesFromTableScan(const tipb::TableScan & table_scan)
{
    NamesAndTypes names_and_types;
    names_and_types.reserve(table_scan.columns_size());

    for (Int32 i = 0; i < table_scan.columns_size(); ++i)
    {
        String name = "mock_table_scan_" + std::to_string(i);
        TiDB::ColumnInfo column_info;
        auto const & ci = table_scan.columns(i);
        column_info.tp = static_cast<TiDB::TP>(ci.tp());
        column_info.id = ci.column_id();
        auto type = getDataTypeByColumnInfoForComputingLayer(column_info);
        names_and_types.push_back({name, type});
    }
    return names_and_types;
}
} // namespace DB