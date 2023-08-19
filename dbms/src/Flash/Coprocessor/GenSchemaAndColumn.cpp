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
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Storages/MutableSupport.h>

namespace DB
{
NamesAndTypes genNamesAndTypes(const TiDBTableScan & table_scan)
{
    NamesAndTypes names_and_types;
    names_and_types.reserve(table_scan.getColumnSize());
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        TiDB::ColumnInfo column_info;
        const auto & ci = table_scan.getColumns()[i];
        column_info.tp = static_cast<TiDB::TP>(ci.tp());
        column_info.id = ci.column_id();

        switch (column_info.id)
        {
        case TiDBPkColumnID:
            // TODO: need to check if the type of pk_handle_columns matches the type that used in delta merge tree.
            names_and_types.emplace_back(MutableSupport::tidb_pk_column_name, getDataTypeByColumnInfoForComputingLayer(column_info));
            break;
        case ExtraTableIDColumnID:
            names_and_types.emplace_back(MutableSupport::extra_table_id_column_name, MutableSupport::extra_table_id_column_type);
            break;
        default:
            names_and_types.emplace_back(fmt::format("mock_table_scan_{}", i), getDataTypeByColumnInfoForComputingLayer(column_info));
        }
    }
    return names_and_types;
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
} // namespace DB