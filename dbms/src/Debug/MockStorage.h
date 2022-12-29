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
#include <DataStreams/IBlockOutputStream.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Storages/Transaction/TiDB.h>
#include <common/types.h>

#include <atomic>
#include <memory>
#include <unordered_map>

#include "DBGInvoker.h"

namespace DB
{
class StorageDeltaMerge;
class Context;

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

    void addTableData(const String & name, ColumnsWithTypeAndName & columns);

    MockColumnInfoVec getTableSchema(const String & name);

    ColumnsWithTypeAndName getColumns(Int64 table_id);

    bool tableExists(Int64 table_id);

    /// for storage delta merge table scan
    void addTableSchemaForDeltaMerge(const String & name, const MockColumnInfoVec & columnInfos);

    void addTableDataForDeltaMerge(Context & context, const String & name, ColumnsWithTypeAndName & columns);
    MockColumnInfoVec getTableSchemaForDeltaMerge(const String & name);
    MockColumnInfoVec getTableSchemaForDeltaMerge(Int64 table_id);
    NamesAndTypes getNameAndTypesForDeltaMerge(Int64 table_id);

    BlockInputStreamPtr getStreamFromDeltaMerge(Context & context, Int64 table_id);

    /// for exchange receiver
    void addExchangeSchema(const String & exchange_name, const MockColumnInfoVec & columnInfos);

    void addExchangeData(const String & exchange_name, const ColumnsWithTypeAndName & columns);

    MockColumnInfoVec getExchangeSchema(const String & exchange_name);

    void addExchangeRelation(const String & executor_id, const String & exchange_name);

    ColumnsWithTypeAndName getExchangeColumns(const String & executor_id);

    bool exchangeExists(const String & executor_id);

    /// for MPP Tasks, it will split data by partition num, then each MPP service will have a subset of mock data.
    ColumnsWithTypeAndName getColumnsForMPPTableScan(const TiDBTableScan & table_scan, Int64 partition_id, Int64 partition_num);

    TableInfo getTableInfo(const String & name);
    TableInfo getTableInfoForDeltaMerge(const String & name);

    /// clear for StorageDeltaMerge
    void clear();

    void setUseDeltaMerge();

    bool useDeltaMerge() const;

private:
    /// for mock table scan
    std::unordered_map<String, Int64> name_to_id_map; /// <table_name, table_id>
    std::unordered_map<Int64, MockColumnInfoVec> table_schema; /// <table_id, columnInfo>
    std::unordered_map<Int64, ColumnsWithTypeAndName> table_columns; /// <table_id, columns>
    std::unordered_map<String, TableInfo> table_infos; /// <table_name, table_info>

    /// for mock exchange receiver
    std::unordered_map<String, String> executor_id_to_name_map; /// <executor_id, exchange name>
    std::unordered_map<String, MockColumnInfoVec> exchange_schemas; /// <exchange_name, columnInfo>
    std::unordered_map<String, ColumnsWithTypeAndName> exchange_columns; /// <exchange_name, columns>

    /// for mock storage delta merge
    std::unordered_map<String, Int64> name_to_id_map_for_delta_merge; /// <table_name, table_id>
    std::unordered_map<Int64, MockColumnInfoVec> table_schema_for_delta_merge; /// <table_id, columnInfo>
    std::unordered_map<Int64, std::shared_ptr<StorageDeltaMerge>> storage_delta_merge_map; // <table_name, StorageDeltaMerge>
    std::unordered_map<String, TableInfo> table_infos_for_delta_merge; /// <table_name, table_info>
    std::unordered_map<Int64, NamesAndTypes> names_and_types_map_for_delta_merge; /// <table_id, NamesAndTypes>

    bool use_storage_delta_merge = false;

private:
    /// for table scan
    Int64 getTableId(const String & name);

    void addTableInfo(const String & name, const MockColumnInfoVec & columns);

    // for storage delta merge table scan
    Int64 getTableIdForDeltaMerge(const String & name);

    bool tableExistsForDeltaMerge(Int64 table_id);

    void addTableInfoForDeltaMerge(const String & name, const MockColumnInfoVec & columns);

    void addNamesAndTypesForDeltaMerge(Int64 table_id, const ColumnsWithTypeAndName & columns);
    /// for exchange receiver
    bool exchangeExistsWithName(const String & name);
};
} // namespace DB
