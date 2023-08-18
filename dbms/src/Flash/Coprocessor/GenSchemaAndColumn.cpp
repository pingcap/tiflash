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
<<<<<<< HEAD
NamesAndTypes genNamesAndTypes(const TiDBTableScan & table_scan)
=======
namespace
{
DataTypePtr getPkType(const ColumnInfo & column_info)
{
    const auto & pk_data_type = getDataTypeByColumnInfoForComputingLayer(column_info);
    /// primary key type must be tidb_pk_column_int_type or tidb_pk_column_string_type.
    RUNTIME_CHECK(
        pk_data_type->equals(*MutableSupport::tidb_pk_column_int_type)
            || pk_data_type->equals(*MutableSupport::tidb_pk_column_string_type),
        pk_data_type->getName(),
        MutableSupport::tidb_pk_column_int_type->getName(),
        MutableSupport::tidb_pk_column_string_type->getName());
    return pk_data_type;
}
} // namespace

NamesAndTypes genNamesAndTypesForTableScan(const TiDBTableScan & table_scan)
{
    return genNamesAndTypes(table_scan, "table_scan");
}

NamesAndTypes genNamesAndTypesForExchangeReceiver(const TiDBTableScan & table_scan)
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
{
    NamesAndTypes names_and_types;
    names_and_types.reserve(table_scan.getColumnSize());
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
<<<<<<< HEAD
        TiDB::ColumnInfo column_info;
        const auto & ci = table_scan.getColumns()[i];
        column_info.tp = static_cast<TiDB::TP>(ci.tp());
        column_info.id = ci.column_id();
=======
        const auto & column_info = table_scan.getColumns()[i];
        names_and_types.emplace_back(
            genNameForExchangeReceiver(i),
            getDataTypeByColumnInfoForComputingLayer(column_info));
    }
    return names_and_types;
}
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))

        switch (column_info.id)
        {
        case TiDBPkColumnID:
            // TODO: need to check if the type of pk_handle_columns matches the type that used in delta merge tree.
            names_and_types.emplace_back(MutableSupport::tidb_pk_column_name, getDataTypeByColumnInfoForComputingLayer(column_info));
            break;
        case ExtraTableIDColumnID:
            names_and_types.emplace_back(
                MutableSupport::extra_table_id_column_name,
                MutableSupport::extra_table_id_column_type);
            break;
        default:
<<<<<<< HEAD
            names_and_types.emplace_back(fmt::format("mock_table_scan_{}", i), getDataTypeByColumnInfoForComputingLayer(column_info));
=======
            names_and_types.emplace_back(
                column_info.name.empty() ? fmt::format("{}_{}", column_prefix, i) : column_info.name,
                getDataTypeByColumnInfoForComputingLayer(column_info));
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
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