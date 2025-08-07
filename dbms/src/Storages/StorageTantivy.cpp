// Copyright 2025 PingCAP, Ltd.
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

#include <Common/TiFlashException.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <Flash/Coprocessor/TiCIScan.h>
#include <Flash/Coprocessor/TiDBTableScan.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Operators/TantivyReaderSourceOp.h>
#include <Storages/IStorage.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/StorageTantivy.h>
#include <Storages/Tantivy/TantivyInputStream.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <tici-search-lib/src/lib.rs.h>

#include <cstddef>
#include <cstdlib>
#include <memory>

namespace DB
{

StorageTantivy::StorageTantivy(Context & context_, const TiCIScan & tici_scan_)
    : IStorage()
    , tici_scan(tici_scan_)
    , context(context_)
    , log(Logger::get(context_.getDAGContext()->log ? context_.getDAGContext()->log->identifier() : ""))
{}

BlockInputStreams StorageTantivy::read(
    const Names &,
    const SelectQueryInfo &,
    const Context &,
    QueryProcessingStage::Enum &,
    size_t,
    unsigned int)
{
    return {};
}

void StorageTantivy::read(
    PipelineExecutorContext & exec_status,
    PipelineExecGroupBuilder & group_builder,
    [[maybe_unused]] const Names & column_names,
    [[maybe_unused]] const SelectQueryInfo & info,
    [[maybe_unused]] const Context & context,
    [[maybe_unused]] size_t max_block_size,
    [[maybe_unused]] unsigned num_streams)
{
    auto return_columns = genNamesAndTypesForTiCI(tici_scan.getReturnColumns(), "column");

    // local read
    group_builder.addConcurrency(std::make_unique<TantivyReaderSourceOp>(
        exec_status,
        log->identifier(),
        tici_scan.getTableId(),
        tici_scan.getIndexId(),
        local_read,
        return_columns,
        tici_scan.getLimit(),
        /*read_ts*/ 0,
        tici_scan.getMatchExpr()));
}

void StorageTantivy::splitRemoteReadAndLocalRead()
{
    const auto & all = tici_scan.getShardInfos().shard_info_list;
    ShardInfoList local_shard_infos;
    ShardInfoList remote_shard_infos;
    ::rust::Vec<::Shard> shards;
    auto index_id = tici_scan.getIndexId();
    auto table_id = tici_scan.getTableId();
    for (const auto & shard_info : all)
    {
        shards.push_back(::Shard{
            .table_id = table_id,
            .index_id = index_id,
            .shard_id = shard_info.shard_id,
            .shard_epoch = shard_info.shard_epoch,
        });
    }

    auto result = check_shards(shards);
    for (size_t i = 0; i < result.size(); ++i)
    {
        const auto & is_local = result[i];
        if (is_local)
        {
            local_shard_infos.push_back(all[i]);
        }
        else
        {
            remote_shard_infos.push_back(all[i]);
        }
    }
    remote_read = remote_shard_infos;
    local_read = local_shard_infos;
}

} // namespace DB
