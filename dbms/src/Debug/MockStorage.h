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
#pragma once
#include <Core/ColumnsWithTypeAndName.h>
#include <Storages/Transaction/TiDB.h>
#include <common/types.h>

#include <atomic>
#include <unordered_map>
namespace DB::tests
{
using MockColumnInfo = std::pair<String, TiDB::TP>;
using MockColumnInfoVec = std::vector<MockColumnInfo>;
using TableInfo = TiDB::TableInfo;
using CutColumnInfo = std::pair<int, int>; // <start_idx, row_num>

class MockTableIdGenerator : public ext::Singleton<MockTableIdGenerator>
{
public:
    Int64 nextTableId()
    {
        return ++current_id;
    }

private:
    std::atomic<Int64> current_id = 0;
};

/** Responsible for mock data for executor tests and mpp tests.
  * 1. Use this class to add mock table schema and table column data.
  * 2. Use this class to add mock exchange schema and exchange column data.
  */
class MockStorage
{
public:
    /// for table scan
    void addTableSchema(const String & name, const MockColumnInfoVec & columnInfos);

    void addTableData(const String & name, const ColumnsWithTypeAndName & columns);

    Int64 getTableId(const String & name);

    bool tableExists(Int64 table_id);

    ColumnsWithTypeAndName getColumns(Int64 table_id);

    MockColumnInfoVec getTableSchema(const String & name);

    /// for exchange receiver
    void addExchangeSchema(const String & exchange_name, const MockColumnInfoVec & columnInfos);

    void addExchangeData(const String & exchange_name, const ColumnsWithTypeAndName & columns);

    bool exchangeExists(const String & executor_id);
    bool exchangeExistsWithName(const String & name);

    ColumnsWithTypeAndName getExchangeColumns(const String & executor_id);

    void addExchangeRelation(const String & executor_id, const String & exchange_name);

    MockColumnInfoVec getExchangeSchema(const String & exchange_name);

    /// for MPP Tasks, it will split data by partition num, then each MPP service will have a subset of mock data.
    ColumnsWithTypeAndName getColumnsForMPPTableScan(Int64 table_id, Int64 partition_id, Int64 partition_num);

    TableInfo getTableInfo(const String & name);

private:
    /// for mock table scan
    std::unordered_map<String, Int64> name_to_id_map; /// <table_name, table_id>
    std::unordered_map<Int64, MockColumnInfoVec> table_schema; /// <table_id, columnInfo>
    std::unordered_map<Int64, ColumnsWithTypeAndName> table_columns; /// <table_id, columns>
    std::unordered_map<String, TableInfo> table_infos;

    /// for mock exchange receiver
    std::unordered_map<String, String> executor_id_to_name_map; /// <executor_id, exchange name>
    std::unordered_map<String, MockColumnInfoVec> exchange_schemas; /// <exchange_name, columnInfo>
    std::unordered_map<String, ColumnsWithTypeAndName> exchange_columns; /// <exchange_name, columns>

private:
    void addTableInfo(const String & name, const MockColumnInfoVec & columns);
};
} // namespace DB::tests
