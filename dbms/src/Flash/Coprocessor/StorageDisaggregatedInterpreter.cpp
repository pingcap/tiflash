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

#include <Flash/Coprocessor/StorageDisaggregatedInterpreter.h>
#include <Flash/Coprocessor/InterpreterUtils.h>


namespace DB
{
void StorageDisaggregatedInterpreter::execute(DAGPipeline & pipeline)
{
    auto remote_requests = buildRemoteRequests();

    auto storage = std::make_unique<StorageDisaggregated>(context, table_scan, remote_requests);
    auto stage = QueryProcessingStage::Enum::FetchColumns;
    pipeline.streams = storage->read(Names(), SelectQueryInfo(), context, stage, 0, max_streams);
    pushDownFilter(pipeline, storage->getExchangeReceiver());
}

std::vector<RemoteRequest> StorageDisaggregatedInterpreter::buildRemoteRequests()
{
    std::unordered_map<Int64, RegionRetryList> all_remote_regions;
    for (auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & table_regions_info = context.getDAGContext()->getTableRegionsInfoByTableID(physical_table_id);

        RUNTIME_CHECK_MSG(table_regions_info.local_regions.empty(), "in disaggregated_compute_mode, local_regions should be empty");
        for (const auto & reg : table_regions_info.remote_regions)
            all_remote_regions[physical_table_id].emplace_back(std::cref(reg));
    }

    std::vector<RemoteRequest> remote_requests;
    for (auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & remote_regions = all_remote_regions[physical_table_id];
        if (remote_regions.empty())
            continue;
        remote_requests.push_back(RemoteRequest::build(
                    remote_regions,
                    *context.getDAGContext(),
                    table_scan,
                    TiDB::TableInfo{},
                    push_down_filter,
                    log,
                    physical_table_id,
                    /*is_disaggregated_compute_mode=*/true));
    }
    return remote_requests;
}


void StorageDisaggregatedInterpreter::pushDownFilter(DAGPipeline & pipeline, std::shared_ptr<ExchangeReceiver> exchange_receiver)
{
    NamesAndTypes source_columns = genNamesAndTypes(table_scan, "exchange_receiver");
    const auto & receiver_dag_schema = exchange_receiver->getOutputSchema();
    assert(receiver_dag_schema.size() == source_columns.size());

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    if (push_down_filter.hasValue())
    {
        // No need to cast, because already done by tiflash_storage node.
        ::DB::executePushedDownFilter(/*remote_read_streams_start_index=*/0, push_down_filter, *analyzer, log, pipeline);

        auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[push_down_filter.executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}
} // namespace DB
