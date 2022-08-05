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
    void addTableSchema(String name, const MockColumnInfoVec & columnInfos);

    void addTableData(String name, const ColumnsWithTypeAndName & columns);

    Int64 getTableId(String name);

    bool tableExists(Int64 table_id);

    ColumnsWithTypeAndName getColumns(Int64 table_id);

    MockColumnInfoVec getTableSchema(String name);

    /// for exchange receiver
    void addExchangeSchema(String & exchange_name, const MockColumnInfoVec & columnInfos);

    void addExchangeData(const String & exchange_name, ColumnsWithTypeAndName & columns);

    bool exchangeExists(const String & executor_id);

    bool exchangeExistsWithName(const String & name);

    ColumnsWithTypeAndName getExchangeColumnsInternal(String & executor_id);

    ColumnsWithTypeAndName getExchangeColumns(String & executor_id);

    void addExchangeRelation(String & executor_id, String & exchange_name);

    MockColumnInfoVec getExchangeSchema(String exchange_name);

    /// for mpp ywq todo not ready to use.
    ColumnsWithTypeAndName getColumnsForMPPTableScan(Int64 table_id, Int64 partition_id, Int64 partition_num);

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
