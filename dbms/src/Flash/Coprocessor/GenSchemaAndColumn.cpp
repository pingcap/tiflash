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

#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TypeMapping.h>

namespace DB
{
namespace
{
DataTypePtr getPkType(const ColumnInfo & column_info)
{
    const auto & pk_data_type = getDataTypeByColumnInfoForComputingLayer(column_info);
    /// primary key type must be tidb_pk_column_int_type or tidb_pk_column_string_type.
    RUNTIME_CHECK(
        pk_data_type->equals(*MutableSupport::tidb_pk_column_int_type) || pk_data_type->equals(*MutableSupport::tidb_pk_column_string_type),
        pk_data_type->getName(),
        MutableSupport::tidb_pk_column_int_type->getName(),
        MutableSupport::tidb_pk_column_string_type->getName());
    return pk_data_type;
}
} // namespace

NamesAndTypes genNamesAndTypes(const TiDBTableScan & table_scan, const StringRef & column_prefix)
{
    NamesAndTypes names_and_types;
    names_and_types.reserve(table_scan.getColumnSize());
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        auto column_info = TiDB::toTiDBColumnInfo(table_scan.getColumns()[i]);
        switch (column_info.id)
        {
        case TiDBPkColumnID:
            names_and_types.emplace_back(MutableSupport::tidb_pk_column_name, getPkType(column_info));
            break;
        case ExtraTableIDColumnID:
            names_and_types.emplace_back(MutableSupport::extra_table_id_column_name, MutableSupport::extra_table_id_column_type);
            break;
        default:
            names_and_types.emplace_back(fmt::format("{}_{}", column_prefix, i), getDataTypeByColumnInfoForComputingLayer(column_info));
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

NamesAndTypes toNamesAndTypes(const DAGSchema & dag_schema)
{
    NamesAndTypes names_and_types;
    for (const auto & col : dag_schema)
    {
        auto tp = getDataTypeByColumnInfoForComputingLayer(col.second);
        names_and_types.emplace_back(col.first, tp);
    }
    return names_and_types;
}
} // namespace DB
