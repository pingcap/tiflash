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
#include <DataStreams/IBlockOutputStream.h>
#include <Debug/MockStorage.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/StorageDeltaMerge.h>


namespace DB
{
/// for table scan
void MockStorage::addTableSchema(const String & name, const MockColumnInfoVec & columnInfos)
{
    name_to_id_map[name] = MockTableIdGenerator::instance().nextTableId();
    table_schema[getTableId(name)] = columnInfos;
    addTableInfo(name, columnInfos);
}

void MockStorage::addTableData(const String & name, ColumnsWithTypeAndName & columns)
{
    for (size_t i = 0; i < columns.size(); ++i)
        columns[i].column_id = i;

    table_columns[getTableId(name)] = columns;
}

Int64 MockStorage::getTableId(const String & name)
{
    if (name_to_id_map.find(name) != name_to_id_map.end())
    {
        return name_to_id_map[name];
    }
    throw Exception(fmt::format("Failed to get table id by table name '{}'", name));
}

bool MockStorage::tableExists(Int64 table_id)
{
    return table_schema.find(table_id) != table_schema.end();
}

ColumnsWithTypeAndName MockStorage::getColumns(Int64 table_id)
{
    if (tableExists(table_id))
    {
        return table_columns[table_id];
    }
    throw Exception(fmt::format("Failed to get columns by table_id '{}'", table_id));
}

MockColumnInfoVec MockStorage::getTableSchema(const String & name)
{
    if (tableExists(getTableId(name)))
    {
        return table_schema[getTableId(name)];
    }
    throw Exception(fmt::format("Failed to get table schema by table name '{}'", name));
}

/// for delta merge
void MockStorage::addTableSchemaForDeltaMerge(const String & name, const MockColumnInfoVec & columnInfos)
{
    name_to_id_map_for_delta_merge[name] = MockTableIdGenerator::instance().nextTableId();
    table_schema_for_delta_merge[getTableIdForDeltaMerge(name)] = columnInfos;
    addTableInfoForDeltaMerge(name, columnInfos);
}

void MockStorage::addTableDataForDeltaMerge(Context & context, const String & name, ColumnsWithTypeAndName & columns)
{
    auto id = getTableIdForDeltaMerge(name);
    addNamesAndTypesForDeltaMerge(id, columns);
    if (storage_delta_merge_map.find(id) == storage_delta_merge_map.end())
    {
        // init
        ASTPtr astptr(new ASTIdentifier(name, ASTIdentifier::Kind::Table));
        NamesAndTypesList names_and_types_list;
        for (const auto & column : columns)
        {
            names_and_types_list.emplace_back(column.name, column.type);
        }
        astptr->children.emplace_back(new ASTIdentifier(columns[0].name));

        storage_delta_merge_map[id] = StorageDeltaMerge::create("TiFlash",
                                                                /* db_name= */ "default",
                                                                name,
                                                                std::nullopt,
                                                                ColumnsDescription{names_and_types_list},
                                                                astptr,
                                                                0,
                                                                context);

        auto storage = storage_delta_merge_map[id];
        storage->startup();

        // write data to DeltaMergeStorage
        ASTPtr insertptr(new ASTInsertQuery());
        BlockOutputStreamPtr output = storage->write(insertptr, context.getSettingsRef());

        Block insert_block{columns};

        output->writePrefix();
        output->write(insert_block);
        output->writeSuffix();
    }
}

BlockInputStreamPtr MockStorage::getStreamFromDeltaMerge(Context & context, Int64 id)
{
    auto storage = storage_delta_merge_map[id];
    auto column_infos = table_schema_for_delta_merge[id];
    Names column_names;
    for (const auto & column_info : column_infos)
        column_names.push_back(column_info.first);

    auto scan_context = std::make_shared<DM::ScanContext>();
    QueryProcessingStage::Enum stage2;
    SelectQueryInfo query_info;
    query_info.query = std::make_shared<ASTSelectQuery>();
    query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(context.getSettingsRef().resolve_locks, std::numeric_limits<UInt64>::max(), scan_context);
    BlockInputStreams ins = storage->read(column_names, query_info, context, stage2, 8192, 1);

    BlockInputStreamPtr in = ins[0];
    return in;
}

void MockStorage::addTableInfoForDeltaMerge(const String & name, const MockColumnInfoVec & columns)
{
    TableInfo table_info;
    table_info.name = name;
    table_info.id = getTableIdForDeltaMerge(name);
    int i = 0;
    for (const auto & column : columns)
    {
        TiDB::ColumnInfo ret;
        std::tie(ret.name, ret.tp) = column;
        // TODO: find a way to assign decimal field's flen.
        if (ret.tp == TiDB::TP::TypeNewDecimal)
            ret.flen = 65;
        ret.id = i++;
        table_info.columns.push_back(std::move(ret));
    }
    table_infos_for_delta_merge[name] = table_info;
}

void MockStorage::addNamesAndTypesForDeltaMerge(Int64 table_id, const ColumnsWithTypeAndName & columns)
{
    NamesAndTypes names_and_types;
    for (auto column : columns)
    {
        names_and_types.emplace_back(column.name, column.type);
    }
    names_and_types_map_for_delta_merge[table_id] = names_and_types;
}


Int64 MockStorage::getTableIdForDeltaMerge(const String & name)
{
    if (name_to_id_map_for_delta_merge.find(name) != name_to_id_map_for_delta_merge.end())
    {
        return name_to_id_map_for_delta_merge[name];
    }
    throw Exception(fmt::format("Failed to get table id by table name '{}'", name));
}

bool MockStorage::tableExistsForDeltaMerge(Int64 table_id)
{
    return table_schema_for_delta_merge.find(table_id) != table_schema_for_delta_merge.end();
}

MockColumnInfoVec MockStorage::getTableSchemaForDeltaMerge(const String & name)
{
    if (tableExistsForDeltaMerge(getTableIdForDeltaMerge(name)))
    {
        return table_schema_for_delta_merge[getTableIdForDeltaMerge(name)];
    }
    throw Exception(fmt::format("Failed to get table schema by table name '{}'", name));
}

MockColumnInfoVec MockStorage::getTableSchemaForDeltaMerge(Int64 table_id)
{
    if (tableExistsForDeltaMerge(table_id))
    {
        return table_schema_for_delta_merge[table_id];
    }
    throw Exception(fmt::format("Failed to get table schema by table id '{}'", table_id));
}

NamesAndTypes MockStorage::getNameAndTypesForDeltaMerge(Int64 table_id)
{
    if (tableExistsForDeltaMerge(table_id))
    {
        return names_and_types_map_for_delta_merge[table_id];
    }
    throw Exception(fmt::format("Failed to get NamesAndTypes by table id '{}'", table_id));
}

/// for exchange receiver
void MockStorage::addExchangeSchema(const String & exchange_name, const MockColumnInfoVec & columnInfos)
{
    exchange_schemas[exchange_name] = columnInfos;
}

void MockStorage::addExchangeData(const String & exchange_name, const ColumnsWithTypeAndName & columns)
{
    exchange_columns[exchange_name] = columns;
}

bool MockStorage::exchangeExists(const String & executor_id)
{
    return exchange_schemas.find(executor_id_to_name_map[executor_id]) != exchange_schemas.end();
}

bool MockStorage::exchangeExistsWithName(const String & name)
{
    return exchange_schemas.find(name) != exchange_schemas.end();
}

ColumnsWithTypeAndName MockStorage::getExchangeColumns(const String & executor_id)
{
    if (exchangeExists(executor_id))
    {
        return exchange_columns[executor_id_to_name_map[executor_id]];
    }
    throw Exception(fmt::format("Failed to get exchange columns by executor_id '{}'", executor_id));
}

void MockStorage::addExchangeRelation(const String & executor_id, const String & exchange_name)
{
    executor_id_to_name_map[executor_id] = exchange_name;
}

MockColumnInfoVec MockStorage::getExchangeSchema(const String & exchange_name)
{
    if (exchangeExistsWithName(exchange_name))
    {
        return exchange_schemas[exchange_name];
    }
    throw Exception(fmt::format("Failed to get exchange schema by exchange name '{}'", exchange_name));
}

void MockStorage::clear()
{
    for (auto [_, storage] : storage_delta_merge_map)
    {
        storage->drop();
        storage->removeFromTMTContext();
    }
}

void MockStorage::setUseDeltaMerge()
{
    use_storage_delta_merge = true;
}

bool MockStorage::useDeltaMerge() const
{
    return use_storage_delta_merge;
}

// use this function to determine where to cut the columns,
// and how many rows are needed for each partition of MPP task.
CutColumnInfo getCutColumnInfo(size_t rows, Int64 partition_id, Int64 partition_num)
{
    int start, per_rows, rows_left, cur_rows;
    per_rows = rows / partition_num;
    rows_left = rows - per_rows * partition_num;
    if (partition_id >= rows_left)
    {
        start = (per_rows + 1) * rows_left + (partition_id - rows_left) * per_rows;
        cur_rows = per_rows;
    }
    else
    {
        start = (per_rows + 1) * partition_id;
        cur_rows = per_rows + 1;
    }
    return {start, cur_rows};
}

ColumnsWithTypeAndName MockStorage::getColumnsForMPPTableScan(const TiDBTableScan & table_scan, Int64 partition_id, Int64 partition_num)
{
    auto table_id = table_scan.getLogicalTableID();
    if (tableExists(table_id))
    {
        auto columns_with_type_and_name = table_columns[table_scan.getLogicalTableID()];
        size_t rows = 0;
        for (const auto & col : columns_with_type_and_name)
        {
            if (rows == 0)
                rows = col.column->size();
            assert(rows == col.column->size());
        }

        CutColumnInfo cut_info = getCutColumnInfo(rows, partition_id, partition_num);

        ColumnsWithTypeAndName res;
        for (const auto & column_with_type_and_name : columns_with_type_and_name)
        {
            bool contains = false;
            for (const auto & column : table_scan.getColumns())
            {
                if (column.id == column_with_type_and_name.column_id)
                {
                    contains = true;
                    break;
                }
            }
            if (contains)
            {
                res.push_back(
                    ColumnWithTypeAndName(
                        column_with_type_and_name.column->cut(cut_info.first, cut_info.second),
                        column_with_type_and_name.type,
                        column_with_type_and_name.name));
            }
        }
        return res;
    }
    throw Exception(fmt::format("Failed to get table columns by table_id '{}'", table_id));
}

void MockStorage::addTableInfo(const String & name, const MockColumnInfoVec & columns)
{
    TableInfo table_info;
    table_info.name = name;
    table_info.id = getTableId(name);
    int i = 0;
    for (const auto & column : columns)
    {
        TiDB::ColumnInfo ret;
        std::tie(ret.name, ret.tp) = column;
        // TODO: find a way to assign decimal field's flen.
        if (ret.tp == TiDB::TP::TypeNewDecimal)
            ret.flen = 65;
        ret.id = i++;
        table_info.columns.push_back(std::move(ret));
    }
    table_infos[name] = table_info;
}

TableInfo MockStorage::getTableInfo(const String & name)
{
    return table_infos[name];
}

TableInfo MockStorage::getTableInfoForDeltaMerge(const String & name)
{
    return table_infos_for_delta_merge[name];
}
} // namespace DB
