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
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Operators/Operator.h>
#include <Storages/DeltaMerge/ColumnDefine_fwd.h>
#include <TiDB/Schema/TiDB.h>
#include <common/types.h>

#include <atomic>
#include <memory>
#include <unordered_map>

namespace DB
{
class StorageDeltaMerge;
using StorageDeltaMergePtr = std::shared_ptr<StorageDeltaMerge>;
class Context;
struct SelectQueryInfo;

struct MockColumnInfo
{
    String name;
    TiDB::TP type;
    bool nullable = true;
};
using MockColumnInfoVec = std::vector<MockColumnInfo>;
using TableInfo = TiDB::TableInfo;
using CutColumnInfo = std::pair<int, int>; // <start_idx, row_num>

class MockTableIdGenerator : public ext::Singleton<MockTableIdGenerator>
{
public:
    Int64 nextTableId() { return ++current_id; }

private:
    std::atomic<Int64> current_id = 0;
};

ColumnInfos mockColumnInfosToTiDBColumnInfos(const MockColumnInfoVec & mock_column_infos);
ColumnsWithTypeAndName getUsedColumns(const ColumnInfos & used_columns, const ColumnsWithTypeAndName & all_columns);

/** Responsible for mock data for executor tests and mpp tests.
  * 1. Use this class to add mock table schema and table column data.
  * 2. Use this class to add mock exchange schema and exchange column data.
  * 3. Use this class to add table schema and table column data into StorageDeltaMerge.
  */
class MockStorage
{
public:
    /// for table scan
    void addTableSchema(const String & name, const MockColumnInfoVec & columnInfos);

    void addTableData(const String & name, ColumnsWithTypeAndName & columns);

    void addTableScanConcurrencyHint(const String & name, size_t concurrency_hint);

    MockColumnInfoVec getTableSchema(const String & name);

    ColumnsWithTypeAndName getColumns(Int64 table_id);

    size_t getScanConcurrencyHint(Int64 table_id);

    bool tableExists(Int64 table_id);

    /// for storage delta merge table scan
    Int64 addTableSchemaForDeltaMerge(const String & name, const MockColumnInfoVec & columnInfos);

    Int64 addTableDataForDeltaMerge(Context & context, const String & name, ColumnsWithTypeAndName & columns);

    MockColumnInfoVec getTableSchemaForDeltaMerge(const String & name);

    MockColumnInfoVec getTableSchemaForDeltaMerge(Int64 table_id);

    NamesAndTypes getNameAndTypesForDeltaMerge(Int64 table_id);

    std::tuple<StorageDeltaMergePtr, Names, SelectQueryInfo> prepareForRead(
        Context & context,
        Int64 table_id,
        bool keep_order = false);

    BlockInputStreamPtr getStreamFromDeltaMerge(
        Context & context,
        Int64 table_id,
        const FilterConditions * filter_conditions = nullptr,
        bool keep_order = false,
        std::vector<int> runtime_filter_ids = std::vector<int>(),
        int rf_max_wait_time_ms = 0);

    void buildExecFromDeltaMerge(
        PipelineExecutorContext & exec_context_,
        PipelineExecGroupBuilder & group_builder,
        Context & context,
        Int64 table_id,
        size_t concurrency = 1,
        bool keep_order = false,
        const FilterConditions * filter_conditions = nullptr,
        std::vector<int> runtime_filter_ids = std::vector<int>(),
        int rf_max_wait_time_ms = 0);

    bool tableExistsForDeltaMerge(Int64 table_id);

    void addDeltaMergeTableConcurrencyHint(const String & name, size_t concurrency_hint);

    size_t getDelatMergeTableConcurrencyHint(Int64 table_id);

    /// for exchange receiver
    void addExchangeSchema(const String & exchange_name, const MockColumnInfoVec & columnInfos);

    void addExchangeData(const String & exchange_name, const ColumnsWithTypeAndName & columns);

    void addFineGrainedExchangeData(const String & exchange_name, const std::vector<ColumnsWithTypeAndName> & columns);

    MockColumnInfoVec getExchangeSchema(const String & exchange_name);

    void addExchangeRelation(const String & executor_id, const String & exchange_name);

    ColumnsWithTypeAndName getExchangeColumns(const String & executor_id);
    std::vector<ColumnsWithTypeAndName> getFineGrainedExchangeColumnsVector(
        const String & executor_id,
        size_t fine_grained_stream_count);

    bool exchangeExists(const String & executor_id);

    /// for MPP Tasks, it will split data by partition num, then each MPP service will have a subset of mock data.
    ColumnsWithTypeAndName getColumnsForMPPTableScan(
        const TiDBTableScan & table_scan,
        Int64 partition_id,
        Int64 partition_num);

    TableInfo getTableInfo(const String & name);
    TableInfo getTableInfoForDeltaMerge(const String & name);
    DM::ColumnDefines getStoreColumnDefines(Int64 table_id);

    size_t getTableScanConcurrencyHint(const TiDBTableScan & table_scan);

    /// clear for StorageDeltaMerge
    void clear();

    void setUseDeltaMerge(bool flag);

    bool useDeltaMerge() const;

private:
    /// for mock table scan
    std::unordered_map<String, Int64> name_to_id_map; /// <table_name, table_id>
    std::unordered_map<Int64, MockColumnInfoVec> table_schema; /// <table_id, columnInfo>
    std::unordered_map<Int64, ColumnsWithTypeAndName> table_columns; /// <table_id, columns>
    std::unordered_map<String, TableInfo> table_infos; /// <table_name, table_info>
    std::unordered_map<Int64, size_t> table_scan_concurrency_hint; /// <table_id, concurrency_hint>

    /// for mock exchange receiver
    std::unordered_map<String, String> executor_id_to_name_map; /// <executor_id, exchange name>
    std::unordered_map<String, MockColumnInfoVec> exchange_schemas; /// <exchange_name, columnInfo>
    std::unordered_map<String, ColumnsWithTypeAndName> exchange_columns; /// <exchange_name, columns>
    std::unordered_map<String, std::vector<ColumnsWithTypeAndName>>
        fine_grained_exchange_columns; /// <exchange_name, vector<columns>>

    /// for mock storage delta merge
    std::unordered_map<String, Int64> name_to_id_map_for_delta_merge; /// <table_name, table_id>
    std::unordered_map<Int64, MockColumnInfoVec> table_schema_for_delta_merge; /// <table_id, columnInfo>
    std::unordered_map<Int64, std::shared_ptr<StorageDeltaMerge>>
        storage_delta_merge_map; // <table_id, StorageDeltaMerge>
    std::unordered_map<String, TableInfo> table_infos_for_delta_merge; /// <table_name, table_info>
    std::unordered_map<Int64, NamesAndTypes> names_and_types_map_for_delta_merge; /// <table_id, NamesAndTypes>
    std::unordered_map<Int64, size_t> delta_merge_table_id_to_concurrency_hint; /// <table_id, concurrency_hint>

    // storage delta merge can be used in executor ut test only.
    bool use_storage_delta_merge = false;

private:
    /// for table scan
    Int64 getTableId(const String & name);

    void addTableInfo(const String & name, const MockColumnInfoVec & columns);

    // for storage delta merge table scan
    Int64 getTableIdForDeltaMerge(const String & name);

    void addTableInfoForDeltaMerge(const String & name, const MockColumnInfoVec & columns);

    void addNamesAndTypesForDeltaMerge(Int64 table_id, const ColumnsWithTypeAndName & columns);
    /// for exchange receiver
    bool exchangeExistsWithName(const String & name);
};
} // namespace DB
