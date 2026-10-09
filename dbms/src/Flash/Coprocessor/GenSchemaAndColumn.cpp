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

#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/CodecUtils.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/MutableSupport.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>

#include <unordered_map>


namespace DB
{
namespace
{
void remapColumnRefsForLateMaterialization(
    tipb::Expr & expr,
    const std::vector<TiDB::ColumnInfo> & scan_columns,
    const std::unordered_map<ColumnID, size_t> & early_column_indexes)
{
    if (expr.tp() == tipb::ExprType::ColumnRef)
    {
        const auto column_id = getStorageColumnIDForColumnarRead(getColumnIDForColumnExpr(expr, scan_columns));
        const auto it = early_column_indexes.find(column_id);
        if (it == early_column_indexes.end())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Late-materialization predicate column {} is absent from the early projection",
                column_id);

        WriteBufferFromOwnString buffer;
        encodeDAGInt64(static_cast<Int64>(it->second), buffer);
        expr.set_val(buffer.releaseStr());
    }

    for (int i = 0; i < expr.children_size(); ++i)
        remapColumnRefsForLateMaterialization(*expr.mutable_children(i), scan_columns, early_column_indexes);
}

DataTypePtr getPkType(const TiDB::ColumnInfo & column_info)
{
    const auto & pk_data_type = getDataTypeByColumnInfoForComputingLayer(column_info);
    /// primary key type must be getTiDBPkColumnIntType or getTiDBPkColumnStringType.
    RUNTIME_CHECK(
        pk_data_type->equals(*MutSup::getExtraHandleColumnIntType())
            || pk_data_type->equals(*MutSup::getExtraHandleColumnStringType()),
        pk_data_type->getName(),
        MutSup::getExtraHandleColumnIntType()->getName(),
        MutSup::getExtraHandleColumnStringType()->getName());
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

String genNameForCTESource(Int32 cte_id, Int32 col_index)
{
    return fmt::format("cte_source_{}_{}", cte_id, col_index);
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
        case MutSup::extra_handle_id:
            names_and_types.emplace_back(MutSup::extra_handle_column_name, getPkType(column_info));
            break;
        case MutSup::extra_table_id_col_id:
            names_and_types.emplace_back(MutSup::extra_table_id_column_name, MutSup::getExtraTableIdColumnType());
            break;
        case MutSup::extra_commit_ts_col_id:
            names_and_types.emplace_back(
                MutSup::version_column_name,
                getDataTypeByColumnInfoForComputingLayer(column_info));
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

std::tuple<DM::ColumnDefinesPtr, int, std::vector<std::tuple<UInt64, String, DataTypePtr>>> genColumnDefinesForDisaggregatedRead(
    const TiDBTableScan & table_scan)
{
    auto column_defines = std::make_shared<DM::ColumnDefines>();
    int extra_table_id_index = MutSup::invalid_col_id;
    column_defines->reserve(table_scan.getColumnSize());
    std::vector<std::tuple<UInt64, String, DataTypePtr>> generated_column_infos;
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        const auto & column_info = table_scan.getColumns()[i];
        if (column_info.hasGeneratedColumnFlag())
        {
            const auto & data_type = getDataTypeByColumnInfoForComputingLayer(column_info);
            const auto & col_name = GeneratedColumnPlaceholderBlockInputStream::getColumnName(i);
            generated_column_infos.push_back(std::make_tuple(i, col_name, data_type));
            continue;
        }
        // Now the upper level seems treat disagg read as an ExchangeReceiver output, so
        // use this as output column prefix.
        // Even if the id is pk_column or extra_table_id, we still output it as
        // a exchange receiver output column
        const auto output_name = genNameForExchangeReceiver(i);
        switch (column_info.id)
        {
        case MutSup::extra_handle_id:
            column_defines->emplace_back(DM::ColumnDefine{
                MutSup::extra_handle_id,
                output_name, // MutSup::extra_handle_column_name
                getPkType(column_info)});
            break;
        case MutSup::extra_table_id_col_id:
        {
            extra_table_id_index = i;
            break;
        }
        case MutSup::extra_commit_ts_col_id:
            // Read the MVCC version using its storage ID and type. The computing
            // layer casts it to the type requested by TiDB after the scan.
            column_defines->emplace_back(MutSup::version_col_id, output_name, MutSup::getVersionColumnType());
            break;
        default:
            column_defines->emplace_back(DM::ColumnDefine{
                column_info.id,
                output_name,
                getDataTypeByColumnInfo(column_info),
                column_info.defaultValueToField()});
            break;
        }
    }
    return {std::move(column_defines), extra_table_id_index, std::move(generated_column_infos)};
}

google::protobuf::RepeatedPtrField<tipb::Expr> remapColumnarFilterConditions(
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filter_conditions,
    const TiDB::ColumnInfos & scan_columns,
    const Block & early_block)
{
    std::unordered_map<ColumnID, size_t> early_column_indexes;
    early_column_indexes.reserve(early_block.columns());
    for (size_t index = 0; index < early_block.columns(); ++index)
    {
        const auto [it, inserted] = early_column_indexes.emplace(early_block.getByPosition(index).column_id, index);
        if (!inserted)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Late-materialization early projection contains duplicate column ID {} at indexes {} and {}",
                it->first,
                it->second,
                index);
    }

    auto conditions = filter_conditions;
    for (int i = 0; i < conditions.size(); ++i)
        remapColumnRefsForLateMaterialization(*conditions.Mutable(i), scan_columns, early_column_indexes);
    return conditions;
}

ColumnID getStorageColumnIDForColumnarRead(ColumnID column_id)
{
    return column_id == MutSup::extra_commit_ts_col_id ? MutSup::version_col_id : column_id;
}

tipb::Executor genTableScanForColumnarRead(const TiDBTableScan & table_scan)
{
    auto executor = *table_scan.getTableScanPB();
    auto * columns = table_scan.isPartitionTableScan() ? executor.mutable_partition_table_scan()->mutable_columns()
                                                       : executor.mutable_tbl_scan()->mutable_columns();
    // ColumnRef offsets stay unchanged; only the IDs used by storage filters change.
    for (auto & column : *columns)
        column.set_column_id(getStorageColumnIDForColumnarRead(column.column_id()));
    return executor;
}

tipb::TableInfo genTableInfoForColumnarRead(const TiDBTableScan & table_scan)
{
    tipb::TableInfo table_info;
    bool needs_version = false;
    bool has_handle = false;
    const auto & columns = table_scan.isPartitionTableScan()
        ? table_scan.getTableScanPB()->partition_table_scan().columns()
        : table_scan.getTableScanPB()->tbl_scan().columns();
    for (int i = 0; i < columns.size(); ++i)
    {
        const auto & column = columns[i];
        if (table_scan.getColumns()[i].hasGeneratedColumnFlag() || column.column_id() == MutSup::extra_table_id_col_id)
            continue;
        if (column.column_id() == MutSup::extra_commit_ts_col_id)
        {
            needs_version = true;
            continue;
        }
        *table_info.add_columns() = column;
        has_handle |= column.column_id() == MutSup::extra_handle_id || column.pk_handle();
    }
    if (needs_version && !has_handle)
    {
        // CSE pack clean reads synthesize dummy handles AND versions when neither
        // is requested. Request the handle to require real MVCC buffers. CSE uses
        // the stored handle schema (including common handles) for this system ID;
        // this dependency is not added to the TiFlash output columns.
        auto * handle = table_info.add_columns();
        handle->set_column_id(MutSup::extra_handle_id);
        handle->set_tp(TiDB::TypeLongLong);
        handle->set_flag(TiDB::ColumnFlagNotNull);
    }
    return table_info;
}

std::tuple<DM::ColumnDefinesPtr, int> genColumnDefinesForDisaggregatedReadThroughColumnar(
    const TiDBTableScan & table_scan)
{
    auto [column_defines, extra_table_id_index, generated_column_infos]
        = genColumnDefinesForDisaggregatedRead(table_scan);
    for (auto & column : *column_defines)
    {
        if (column.id == MutSup::version_col_id)
        {
            // CSE's version buffer is nullable: the null map represents tombstones.
            // MVCC removes them, but the FFI wire format still includes the null map.
            column.type = makeNullable(MutSup::getVersionColumnType());
        }
        else
        {
            const auto & converted_type = CodecUtils::convertDataType(*column.type);
            if (&converted_type != column.type.get())
                column.type = DataTypeFactory::instance().getOrSet(converted_type.getName());
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
