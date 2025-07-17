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
#include <Flash/Coprocessor/TiCIScan.h>
#include <TiDB/Schema/TiDB.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>

#include <cassert>
namespace DB
{
TiCIScan::TiCIScan(const tipb::Executor * tici_scan_, const String & executor_id_, const DAGContext & dag_context)
    : tici_scan(tici_scan_)
    , executor_id(executor_id_)
    , table_id(tici_scan->idx_scan().table_id())
    , index_id(tici_scan->idx_scan().index_id())
    , return_columns(TiDB::toTiDBColumnInfos(tici_scan->idx_scan().columns()))
    , query_columns(TiDB::toTiDBColumnInfos(tici_scan->idx_scan().fts_query_info().columns()))
    , query_type(tici_scan->idx_scan().fts_query_info().query_type())
    , shard_infos(dag_context.query_shard_infos.getTableShardInfosByExecutorID(tici_scan_->executor_id()))
    , query_json_str(tici_scan->idx_scan().fts_query_info().query_text())
    , limit(tici_scan->idx_scan().fts_query_info().top_k())
{
    RUNTIME_ASSERT(tici_scan->idx_scan().fts_query_info().query_func() == tipb::ScalarFuncSig::FTSMatchWord);
}

void TiCIScan::constructTiCIScanForRemoteRead(tipb::IndexScan * tipb_index_scan) const
{
    assert(tipb_index_scan != nullptr);
    *tipb_index_scan = tici_scan->idx_scan();
} // namespace DB
} // namespace DB
