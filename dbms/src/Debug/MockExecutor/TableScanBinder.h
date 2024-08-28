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

#include <Debug/MockExecutor/ExecutorBinder.h>
#include <TiDB/Schema/TiDB.h>

namespace DB::mock
{

class TableScanBinder : public ExecutorBinder
{
public:
    TableScanBinder(
        size_t & index_,
        const DAGSchema & output_schema_,
        const TiDB::TableInfo & table_info_,
        bool keep_order_)
        : ExecutorBinder(index_, "table_scan_" + std::to_string(index_), output_schema_)
        , table_info(table_info_)
        , keep_order(keep_order_)
    {}

    void setRuntimeFilterIds(const std::vector<int> & rf_ids_);

    void columnPrune(std::unordered_set<String> & used_columns) override;


    bool toTiPBExecutor(tipb::Executor * tipb_executor, int32_t, const MPPInfo &, const Context &) override;

    void toMPPSubPlan(
        size_t &,
        const DAGProperties &,
        std::unordered_map<
            String,
            std::pair<std::shared_ptr<ExchangeReceiverBinder>, std::shared_ptr<ExchangeSenderBinder>>> &) override
    {}

    TableID getTableId() const;

private:
    TiDB::TableInfo table_info; /// used by column pruner
    bool keep_order;
    std::vector<int> rf_ids;

private:
    void setTipbColumnInfo(tipb::ColumnInfo * ci, const DAGColumnInfo & dag_column_info) const;
    void buildPartionTable(tipb::Executor * tipb_executor);
    void buildTable(tipb::Executor * tipb_executor);
};

ExecutorBinderPtr compileTableScan(
    size_t & executor_index,
    TiDB::TableInfo & table_info,
    const String & db,
    const String & table_name,
    bool append_pk_column,
    bool keep_order = false);
} // namespace DB::mock
