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
#pragma once
#include <Core/ColumnsWithTypeAndName.h>
#include <Storages/Transaction/TiDB.h>
#include <common/types.h>

#include <unordered_map>
namespace DB::tests
{
using MockColumnInfo = std::pair<String, TiDB::TP>;
using MockColumnInfoVec = std::vector<MockColumnInfo>;

class MockTableIdGenerator
{
public:
    static MockTableIdGenerator & getInstance()
    {
        static MockTableIdGenerator table_id_generator;
        return table_id_generator;
    }
    Int64 nextTableId()
    {
        return ++current_id;
    }

private:
    Int64 current_id = 0;
};

class MockStorage
{
public:
    void addTableSchema(String name, const MockColumnInfoVec & columnInfos)
    {
        name_to_id_map[name] = MockTableIdGenerator::getInstance().nextTableId();
        table_schema[getTableId(name)] = columnInfos;
    }

    void addTableData(String name, const ColumnsWithTypeAndName & columns)
    {
        table_columns[getTableId(name)] = columns;
    }

    // ywq todo error handling.
    Int64 getTableId(String name)
    {
        return name_to_id_map[name];
    }

    bool tableExists(Int64 table_id)
    {
        return table_columns.find(table_id) != table_columns.end();
    }

    ColumnsWithTypeAndName getColumns(Int64 table_id)
    {
        if (tableExists(table_id))
        {
            return table_columns[table_id];
        }
        return {};
    }

    // ywq todo safety.
    MockColumnInfoVec getTableSchema(String name)
    {
        if (tableExists(getTableId(name)))
        {
            return table_schema[getTableId(name)];
        }
        return {};
    }

    /// for exchange receiver
    void addExchangeSchema(String & exchange_name, const MockColumnInfoVec & columnInfos)
    {
        exchange_schemas[exchange_name] = columnInfos;
    }

    void addExchangeData(const String & exchange_name, ColumnsWithTypeAndName & columns)
    {
        exchange_columns[exchange_name] = columns;
    }

    bool exchangeExists(const String & executor_id)
    {
        return exchange_schemas.find(executor_id_to_name_map[executor_id]) != exchange_schemas.end();
    }

    bool exchangeExistsWithName(const String & name)
    {
        return exchange_schemas.find(name) != exchange_schemas.end();
    }

    ColumnsWithTypeAndName getExchangeColumnsInternal(String & executor_id)
    {
        if (exchangeExists(executor_id))
        {
            return exchange_columns[executor_id_to_name_map[executor_id]];
        }
        return {};
    }

    ColumnsWithTypeAndName getExchangeColumns(String & executor_id)
    {
        std::cout << "ywq test executor id:" << executor_id << ", exchange name: " << executor_id_to_name_map[executor_id] << std::endl;
        return getExchangeColumnsInternal(executor_id);
    }

    void addExchangeRelation(String & executor_id, String & exchange_name)
    {
        executor_id_to_name_map[executor_id] = exchange_name;
    }

    // ywq todo safety.
    // todo check exists.
    MockColumnInfoVec getExchangeSchema(String exchange_name)
    {
        if (exchangeExistsWithName(exchange_name))
        {
            std::cout << "ywq test exchange exist with name" << std::endl;
            return exchange_schemas[exchange_name];
        }
        return {};
    }

    /// for mpp

    ColumnsWithTypeAndName getColumnsForMPPTableScan(Int64 table_id, Int64 partition_id, Int64 partition_num)
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

private:
    /// for mock table scan
    std::unordered_map<String, Int64> name_to_id_map; /// <table_name, table_id>
    std::unordered_map<Int64, MockColumnInfoVec> table_schema; /// <table_id, columnInfo>

    std::unordered_map<Int64, ColumnsWithTypeAndName> table_columns; /// <table_id, columns>
    /// for mock exchange receiver
    std::unordered_map<String, String> executor_id_to_name_map; /// <executor_id, exchange name>
    std::unordered_map<String, MockColumnInfoVec> exchange_schemas; /// <exchange_name, columnInfo>
    std::unordered_map<String, ColumnsWithTypeAndName> exchange_columns; /// <exchange_name, columns>
};

} // namespace DB::tests
