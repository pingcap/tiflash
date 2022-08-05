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
#include <TestUtils/MockStorage.h>

namespace DB::tests
{
void MockStorage::addTableSchema(String name, const MockColumnInfoVec & columnInfos)
{
    name_to_id_map[name] = MockTableIdGenerator::getInstance().nextTableId();
    table_schema[getTableId(name)] = columnInfos;
}

void MockStorage::addTableData(String name, const ColumnsWithTypeAndName & columns)
{
    table_columns[getTableId(name)] = columns;
}

Int64 MockStorage::getTableId(String name)
{
    if (name_to_id_map.find(name) != name_to_id_map.end())
    {
        return name_to_id_map[name];
    }
    throw Exception(fmt::format("Failed to get table id  by table name '{}'", name));
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

MockColumnInfoVec MockStorage::getTableSchema(String name)
{
    if (tableExists(getTableId(name)))
    {
        return table_schema[getTableId(name)];
    }
    throw Exception(fmt::format("Failed to get table schema by table name '{}'", name));
}

/// for exchange receiver
void MockStorage::addExchangeSchema(String & exchange_name, const MockColumnInfoVec & columnInfos)
{
    exchange_schemas[exchange_name] = columnInfos;
}

void MockStorage::addExchangeData(const String & exchange_name, ColumnsWithTypeAndName & columns)
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

ColumnsWithTypeAndName MockStorage::getExchangeColumnsInternal(String & executor_id)
{
    if (exchangeExists(executor_id))
    {
        return exchange_columns[executor_id_to_name_map[executor_id]];
    }
    throw Exception(fmt::format("Failed to get exchange columns by executor_id '{}'", executor_id));
}

ColumnsWithTypeAndName MockStorage::getExchangeColumns(String & executor_id)
{
    return getExchangeColumnsInternal(executor_id);
}

void MockStorage::addExchangeRelation(String & executor_id, String & exchange_name)
{
    executor_id_to_name_map[executor_id] = exchange_name;
}

MockColumnInfoVec MockStorage::getExchangeSchema(String exchange_name)
{
    if (exchangeExistsWithName(exchange_name))
    {
        return exchange_schemas[exchange_name];
    }
    throw Exception(fmt::format("Failed to get exchange schema by exchange name '{}'", exchange_name));
}

/// for mpp ywq todo not ready to use.
ColumnsWithTypeAndName MockStorage::getColumnsForMPPTableScan(Int64 table_id, Int64 partition_id, Int64 partition_num)
{
    if (tableExists(table_id))
    {
        auto columns_with_type_and_name = table_columns[table_id];
        int rows = 0;
        for (const auto & col : columns_with_type_and_name)
        {
            if (rows == 0)
                rows = col.column->size();
        }
        int per_rows = rows / partition_num;
        int rows_left = rows = per_rows * partition_num;
        int cur_rows = per_rows;
        if (partition_id < rows_left)
        {
            cur_rows += 1;
        }
        int start = 0;
        if (partition_id < rows_left)
        {
            start = cur_rows * partition_id;
        }
        ColumnsWithTypeAndName res;
        for (const auto & column_with_type_and_name : columns_with_type_and_name)
        {
            res.push_back(
                ColumnWithTypeAndName(
                    column_with_type_and_name.column->cut(start, cur_rows),
                    column_with_type_and_name.type,
                    column_with_type_and_name.name));
        }
        return res;
    }
    return {};
}


} // namespace DB::tests
