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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Flash/Coprocessor/TiCIScan.h>
#include <TiDB/Schema/TiDB.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>

#include <cassert>
namespace DB
{

static TiCIQueryMode resolveQueryMode(const tipb::IndexScan & idx_scan)
{
    if (idx_scan.has_tici_vector_query_info())
        return TiCIQueryMode::Vector;
    RUNTIME_CHECK_MSG(idx_scan.has_fts_query_info(), "IndexScan must have either fts_query_info or tici_vector_query_info");
    return TiCIQueryMode::FTS;
}

TiCIScan::TiCIScan(const tipb::Executor * tici_scan_, const String & executor_id_, const DAGContext & dag_context)
    : tici_scan(tici_scan_)
    , executor_id(executor_id_)
    , keyspace_id(dag_context.getKeyspaceID())
    , table_id(tici_scan->idx_scan().table_id())
    , index_id(tici_scan->idx_scan().index_id())
    , return_columns(TiDB::toTiDBColumnInfos(tici_scan->idx_scan().columns()))
    , query_mode(resolveQueryMode(tici_scan->idx_scan()))
    , shard_infos(dag_context.query_shard_infos.getTableShardInfosByExecutorID(tici_scan_->executor_id()))
    , limit(
          query_mode == TiCIQueryMode::Vector
              ? tici_scan->idx_scan().tici_vector_query_info().top_k()
              : tici_scan->idx_scan().fts_query_info().top_k())
    , sort_column_ids(
          query_mode == TiCIQueryMode::FTS
              ? std::vector<Int64>(
                    tici_scan->idx_scan().fts_query_info().sort_column_ids().begin(),
                    tici_scan->idx_scan().fts_query_info().sort_column_ids().end())
              : std::vector<Int64>())
    , sort_column_asc(
          query_mode == TiCIQueryMode::FTS
              ? std::vector<bool>(
                    tici_scan->idx_scan().fts_query_info().sort_column_asc().begin(),
                    tici_scan->idx_scan().fts_query_info().sort_column_asc().end())
              : std::vector<bool>())
{}

void TiCIScan::constructTiCIScanForRemoteRead(tipb::IndexScan * tipb_index_scan) const
{
    assert(tipb_index_scan != nullptr);
    *tipb_index_scan = tici_scan->idx_scan();
}

} // namespace DB
