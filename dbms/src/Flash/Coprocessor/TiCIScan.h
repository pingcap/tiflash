// Copyright 2025 PingCAP, Inc.
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

#include <Flash/Coprocessor/DAGContext.h>
#include <TiDB/Schema/TiDB.h>
#include <tipb/executor.pb.h>
namespace DB
{
class DAGContext;

class TiCIScan
{
public:
    TiCIScan(const tipb::Executor * tici_scan_, const String & executor_id_, const DAGContext & dag_context);
    explicit TiCIScan(const tipb::Executor * tici_scan_);

    // Do not use getReturnColumns when is_count_agg is true
    const TiDB::ColumnInfos & getReturnColumns() const { return return_columns; }
    void setNamesAndTypes(const NamesAndTypes & new_names_and_types) { names_and_types = new_names_and_types; }
    const NamesAndTypes getNamesAndTypes() const { return names_and_types; }
    const TableShardInfos & getShardInfos() const { return shard_infos; }
    const uint & getKeyspaceID() const { return keyspace_id; }
    const int & getTableId() const { return table_id; }
    const int & getIndexId() const { return index_id; }
    const int & getLimit() const { return limit; }
    const tipb::Executor * getTiCIScan() const { return tici_scan; }

    void constructTiCIScanForRemoteRead(tipb::IndexScan * tipb_index_scan) const;

    const ::google::protobuf::RepeatedPtrField<::tipb::Expr> & getMatchExpr() const
    {
        return tici_scan->idx_scan().fts_query_info().match_expr();
    }

    bool isCount() const { return is_count_agg; }

    void setIsCountAgg(bool v) { is_count_agg = v; }

    void setCountAggExecutorId(const String & v) { count_agg_executor_id = v; }

    const String & getCountAggExecutorId() const { return count_agg_executor_id; }

private:
    const tipb::Executor * tici_scan;
    [[maybe_unused]] String executor_id;
    const uint keyspace_id;
    const int table_id;
    const int index_id;
    TiDB::ColumnInfos return_columns;
    NamesAndTypes names_and_types;
    [[maybe_unused]] tipb::FTSQueryType query_type;
    const TableShardInfos shard_infos;
    const int limit;
    bool is_count_agg = false;
    String count_agg_executor_id;
};
} // namespace DB
