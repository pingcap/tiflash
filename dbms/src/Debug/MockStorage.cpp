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
#include <Debug/MockStorage.h>

namespace DB::tests
{
void MockStorage::addTableSchema(const String & name, const MockColumnInfoVec & columnInfos)
{
    name_to_id_map[name] = MockTableIdGenerator::instance().nextTableId();
    table_schema[getTableId(name)] = columnInfos;
    addTableInfo(name, columnInfos);
}

void MockStorage::addTableData(const String & name, const ColumnsWithTypeAndName & columns)
{
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

ColumnsWithTypeAndName MockStorage::getColumnsForMPPTableScan(Int64 table_id, Int64 partition_id, Int64 partition_num)
{
    if (tableExists(table_id))
    {
        auto columns_with_type_and_name = table_columns[table_id];
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
            res.push_back(
                ColumnWithTypeAndName(
                    column_with_type_and_name.column->cut(cut_info.first, cut_info.second),
                    column_with_type_and_name.type,
                    column_with_type_and_name.name));
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
} // namespace DB::tests
