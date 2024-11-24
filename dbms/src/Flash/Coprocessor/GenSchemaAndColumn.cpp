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
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/MutableSupport.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>


namespace DB
{
namespace
{
DataTypePtr getPkType(const TiDB::ColumnInfo & column_info)
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
{
    NamesAndTypes names_and_types;
    names_and_types.reserve(table_scan.getColumnSize());
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        const auto & column_info = table_scan.getColumns()[i];
        names_and_types.emplace_back(
            genNameForExchangeReceiver(i),
            getDataTypeByColumnInfoForComputingLayer(column_info));
    }
    return names_and_types;
}

String genNameForExchangeReceiver(Int32 col_index)
{
    return fmt::format("exchange_receiver_{}", col_index);
}

NamesAndTypes genNamesAndTypes(const TiDB::ColumnInfos & column_infos, const StringRef & column_prefix)
{
    NamesAndTypes names_and_types;
    names_and_types.reserve(column_infos.size());
    for (size_t i = 0; i < column_infos.size(); ++i)
    {
        const auto & column_info = column_infos[i];
        switch (column_info.id)
        {
        case TiDBPkColumnID:
            names_and_types.emplace_back(MutableSupport::tidb_pk_column_name, getPkType(column_info));
            break;
        case ExtraTableIDColumnID:
            names_and_types.emplace_back(
                MutableSupport::extra_table_id_column_name,
                MutableSupport::extra_table_id_column_type);
            break;
        default:
            names_and_types.emplace_back(
                column_info.name.empty() ? fmt::format("{}_{}", column_prefix, i) : column_info.name,
                getDataTypeByColumnInfoForComputingLayer(column_info));
        }
    }
    return names_and_types;
}
NamesAndTypes genNamesAndTypes(const TiDBTableScan & table_scan, const StringRef & column_prefix)
{
    return genNamesAndTypes(table_scan.getColumns(), column_prefix);
}

std::tuple<DM::ColumnDefinesPtr, int> genColumnDefinesForDisaggregatedRead(const TiDBTableScan & table_scan)
{
    auto column_defines = std::make_shared<DM::ColumnDefines>();
    int extra_table_id_index = InvalidColumnID;
    column_defines->reserve(table_scan.getColumnSize());
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        const auto & column_info = table_scan.getColumns()[i];
        // Now the upper level seems treat disagg read as an ExchangeReceiver output, so
        // use this as output column prefix.
        // Even if the id is pk_column or extra_table_id, we still output it as
        // a exchange receiver output column
        const auto output_name = genNameForExchangeReceiver(i);
        switch (column_info.id)
        {
        case TiDBPkColumnID:
            column_defines->emplace_back(DM::ColumnDefine{
                TiDBPkColumnID,
                output_name, // MutableSupport::tidb_pk_column_name
                getPkType(column_info)});
            break;
        case ExtraTableIDColumnID:
        {
            column_defines->emplace_back(DM::ColumnDefine{
                ExtraTableIDColumnID,
                output_name, // MutableSupport::extra_table_id_column_name
                MutableSupport::extra_table_id_column_type});
            extra_table_id_index = i;
            break;
        }
        default:
            column_defines->emplace_back(DM::ColumnDefine{
                column_info.id,
                output_name,
                getDataTypeByColumnInfoForDisaggregatedStorageLayer(column_info),
                column_info.defaultValueToField()});
            break;
        }
    }
    return {std::move(column_defines), extra_table_id_index};
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
