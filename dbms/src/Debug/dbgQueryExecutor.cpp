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

#include <DataStreams/UnionBlockInputStream.h>
#include <Debug/MockExecutor/AstToPBUtils.h>
#include <Debug/dbgQueryExecutor.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/CoprocessorHandler.h>
#include <Interpreters/Context.h>
#include <Server/MockComputeClient.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <TestUtils/TiFlashTestEnv.h>
namespace DB
{
using TiFlashTestEnv = tests::TiFlashTestEnv;

void fillTaskMetaWithDAGProperties(mpp::TaskMeta & meta, const DAGProperties & properties)
{
    meta.set_start_ts(properties.start_ts);
    meta.set_gather_id(properties.gather_id);
    meta.set_query_ts(properties.query_ts);
    meta.set_local_query_id(properties.local_query_id);
    meta.set_server_id(properties.server_id);
}

void setTipbRegionInfo(
    coprocessor::RegionInfo * tipb_region_info,
    const std::pair<RegionID, RegionPtr> & region,
    TableID table_id)
{
    tipb_region_info->set_region_id(region.first);
    auto * meta = tipb_region_info->mutable_region_epoch();
    meta->set_conf_ver(region.second->confVer());
    meta->set_version(region.second->version());
    auto * range = tipb_region_info->add_ranges();
    auto handle_range = getHandleRangeByTable(region.second->getRange()->rawKeys(), table_id);
    range->set_start(RecordKVFormat::genRawKey(table_id, handle_range.first.handle_id));
    range->set_end(RecordKVFormat::genRawKey(table_id, handle_range.second.handle_id));
}

BlockInputStreamPtr constructRootExchangeReceiverStream(
    Context & context,
    tipb::ExchangeReceiver & tipb_exchange_receiver,
    const DAGProperties & properties,
    DAGSchema & root_task_schema,
    const String & root_addr)
{
    for (auto & field : root_task_schema)
    {
        auto tipb_type = TiDB::columnInfoToFieldType(field.second);
        tipb_type.set_collate(properties.collator);
        auto * field_type = tipb_exchange_receiver.add_field_types();
        *field_type = tipb_type;
    }

    mpp::TaskMeta root_tm;
    fillTaskMetaWithDAGProperties(root_tm, properties);
    root_tm.set_address(root_addr);
    root_tm.set_task_id(-1);
    root_tm.set_partition_id(-1);

    std::shared_ptr<ExchangeReceiver> exchange_receiver = std::make_shared<ExchangeReceiver>(
        std::make_shared<GRPCReceiverContext>(
            tipb_exchange_receiver,
            root_tm,
            context.getTMTContext().getKVCluster(),
            context.getTMTContext().getMPPTaskManager(),
            false,
            context.getSettingsRef().enable_async_grpc_client),
        tipb_exchange_receiver.encoded_task_meta_size(),
        10,
        /*req_id=*/"",
        /*executor_id=*/"",
        /*fine_grained_shuffle_stream_count=*/0,
        context.getSettingsRef());
    BlockInputStreamPtr ret = std::make_shared<ExchangeReceiverInputStream>(
        exchange_receiver,
        /*req_id=*/"",
        /*executor_id=*/"",
        /*stream_id*/ 0);
    return ret;
}

BlockInputStreamPtr prepareRootExchangeReceiver(
    Context & context,
    const DAGProperties & properties,
    std::vector<Int64> & root_task_ids,
    DAGSchema & root_task_schema)
{
    tipb::ExchangeReceiver tipb_exchange_receiver;
    for (const auto root_task_id : root_task_ids)
    {
        mpp::TaskMeta tm;
        fillTaskMetaWithDAGProperties(tm, properties);
        tm.set_address(Debug::LOCAL_HOST);
        tm.set_task_id(root_task_id);
        tm.set_partition_id(-1);
        auto * tm_string = tipb_exchange_receiver.add_encoded_task_meta();
        tm.AppendToString(tm_string);
    }
    return constructRootExchangeReceiverStream(
        context,
        tipb_exchange_receiver,
        properties,
        root_task_schema,
        Debug::LOCAL_HOST);
}

void prepareExchangeReceiverMetaWithMultipleContext(
    tipb::ExchangeReceiver & tipb_exchange_receiver,
    const DAGProperties & properties,
    Int64 task_id,
    String & addr)
{
    mpp::TaskMeta tm;
    fillTaskMetaWithDAGProperties(tm, properties);
    tm.set_address(addr);
    tm.set_task_id(task_id);
    tm.set_partition_id(-1);
    auto * tm_string = tipb_exchange_receiver.add_encoded_task_meta();
    tm.AppendToString(tm_string);
}

BlockInputStreamPtr prepareRootExchangeReceiverWithMultipleContext(
    Context & context,
    const DAGProperties & properties,
    Int64 task_id,
    DAGSchema & root_task_schema,
    String & addr,
    String & root_addr)
{
    tipb::ExchangeReceiver tipb_exchange_receiver;

    prepareExchangeReceiverMetaWithMultipleContext(tipb_exchange_receiver, properties, task_id, addr);

    return constructRootExchangeReceiverStream(
        context,
        tipb_exchange_receiver,
        properties,
        root_task_schema,
        root_addr);
}

void prepareDispatchTaskRequest(
    QueryTask & task,
    mpp::DispatchTaskRequest & req,
    const DAGProperties & properties,
    std::vector<Int64> & root_task_ids,
    DAGSchema & root_task_schema,
    String & addr)
{
    if (task.is_root_task)
    {
        root_task_ids.push_back(task.task_id);
        root_task_schema = task.result_schema;
    }
    auto * tm = req.mutable_meta();
    fillTaskMetaWithDAGProperties(*tm, properties);
    tm->set_partition_id(task.partition_id);
    tm->set_address(addr);
    tm->set_task_id(task.task_id);
    auto * encoded_plan = req.mutable_encoded_plan();
    task.dag_request->AppendToString(encoded_plan);
    req.set_timeout(properties.mpp_timeout);
    req.set_schema_ver(DEFAULT_UNSPECIFIED_SCHEMA_VERSION);
}

void prepareDispatchTaskRequestWithMultipleContext(
    QueryTask & task,
    std::shared_ptr<mpp::DispatchTaskRequest> req,
    const DAGProperties & properties,
    std::vector<Int64> & root_task_ids,
    std::vector<Int64> & root_task_partition_ids,
    DAGSchema & root_task_schema,
    String & addr)
{
    if (task.is_root_task)
    {
        root_task_ids.push_back(task.task_id);
        root_task_partition_ids.push_back(task.partition_id);
        root_task_schema = task.result_schema;
    }
    auto * tm = req->mutable_meta();
    fillTaskMetaWithDAGProperties(*tm, properties);
    tm->set_partition_id(task.partition_id);
    tm->set_address(addr);
    tm->set_task_id(task.task_id);
    auto * encoded_plan = req->mutable_encoded_plan();
    task.dag_request->AppendToString(encoded_plan);
    req->set_timeout(properties.mpp_timeout);
    req->set_schema_ver(DEFAULT_UNSPECIFIED_SCHEMA_VERSION);
}


// execute MPP Query in one service
BlockInputStreamPtr executeMPPQuery(Context & context, const DAGProperties & properties, QueryTasks & query_tasks)
{
    DAGSchema root_task_schema;
    std::vector<Int64> root_task_ids;
    for (auto & task : query_tasks)
    {
        mpp::DispatchTaskRequest req;
        prepareDispatchTaskRequest(task, req, properties, root_task_ids, root_task_schema, Debug::LOCAL_HOST);
        auto table_id = task.table_id;
        if (table_id != -1)
        {
            /// contains a table scan
            const auto & table_info = MockTiDB::instance().getTableInfoByID(table_id);
            if (table_info->is_partition_table)
            {
                size_t current_region_size = 0;
                coprocessor::TableRegions * current_table_regions = nullptr;
                for (const auto & partition : table_info->partition.definitions)
                {
                    const auto partition_id = partition.id;
                    auto regions
                        = context.getTMTContext().getRegionTable().getRegionsByTable(NullspaceID, partition_id);
                    for (size_t i = 0; i < regions.size(); ++i)
                    {
                        if ((current_region_size + i) % properties.mpp_partition_num
                            != static_cast<size_t>(task.partition_id))
                            continue;
                        if (current_table_regions != nullptr
                            && current_table_regions->physical_table_id() != partition_id)
                            current_table_regions = nullptr;
                        if (current_table_regions == nullptr)
                        {
                            current_table_regions = req.add_table_regions();
                            current_table_regions->set_physical_table_id(partition_id);
                        }
                        setTipbRegionInfo(current_table_regions->add_regions(), regions[i], partition_id);
                    }
                    current_region_size += regions.size();
                }
                if (current_region_size < static_cast<size_t>(properties.mpp_partition_num))
                    throw Exception("Not supported: table region num less than mpp partition num");
            }
            else
            {
                auto regions = context.getTMTContext().getRegionTable().getRegionsByTable(NullspaceID, table_id);
                if (regions.size() < static_cast<size_t>(properties.mpp_partition_num))
                    throw Exception("Not supported: table region num less than mpp partition num");
                for (size_t i = 0; i < regions.size(); ++i)
                {
                    if (i % properties.mpp_partition_num != static_cast<size_t>(task.partition_id))
                        continue;
                    setTipbRegionInfo(req.add_regions(), regions[i], table_id);
                }
            }
        }

        pingcap::kv::RpcCall<pingcap::kv::RPC_NAME(DispatchMPPTask)> rpc(
            context.getTMTContext().getKVCluster()->rpc_client,
            Debug::LOCAL_HOST);
        grpc::ClientContext client_context;
        rpc.setClientContext(client_context, 5);
        mpp::DispatchTaskResponse resp;
        auto status = rpc.call(&client_context, req, &resp);
        if (!status.ok())
            throw Exception("Meet grpc error while dispatch mpp task: " + rpc.errMsg(status));
        if (resp.has_error())
            throw Exception("Meet error while dispatch mpp task: " + resp.error().msg());
    }
    return prepareRootExchangeReceiver(context, properties, root_task_ids, root_task_schema);
}

BlockInputStreamPtr executeNonMPPQuery(
    Context & context,
    RegionID region_id,
    const DAGProperties & properties,
    QueryTasks & query_tasks,
    MakeResOutputStream & func_wrap_output_stream)
{
    auto & task = query_tasks[0];
    auto table_id = task.table_id;
    RegionPtr region;
    if (region_id == InvalidRegionID)
    {
        auto regions = context.getTMTContext().getRegionTable().getRegionsByTable(NullspaceID, table_id);
        if (regions.empty())
            throw Exception("No region for table", ErrorCodes::BAD_ARGUMENTS);
        region = regions[0].second;
        region_id = regions[0].first;
    }
    else
    {
        region = context.getTMTContext().getKVStore()->getRegion(region_id);
        if (!region)
            throw Exception("No such region", ErrorCodes::BAD_ARGUMENTS);
    }
    auto handle_range = getHandleRangeByTable(region->getRange()->rawKeys(), table_id);
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> key_ranges;
    DecodedTiKVKeyPtr start_key
        = std::make_shared<DecodedTiKVKey>(RecordKVFormat::genRawKey(table_id, handle_range.first.handle_id));
    DecodedTiKVKeyPtr end_key
        = std::make_shared<DecodedTiKVKey>(RecordKVFormat::genRawKey(table_id, handle_range.second.handle_id));
    key_ranges.emplace_back(std::make_pair(std::move(start_key), std::move(end_key)));
    tipb::SelectResponse dag_response = executeDAGRequest(
        context,
        *task.dag_request,
        region_id,
        region->version(),
        region->confVer(),
        properties.start_ts,
        key_ranges);

    return func_wrap_output_stream(outputDAGResponse(context, task.result_schema, dag_response));
}

BlockInputStreamPtr executeMPPQueryWithMultipleContext(
    const DAGProperties & properties,
    QueryTasks & query_tasks,
    std::unordered_map<size_t, MockServerConfig> & server_config_map)
{
    DAGSchema root_task_schema;
    std::vector<Int64> root_task_ids;
    std::vector<Int64> root_task_partition_ids;
    for (auto & task : query_tasks)
    {
        auto req = std::make_shared<mpp::DispatchTaskRequest>();

        auto addr = server_config_map[task.partition_id].addr;
        prepareDispatchTaskRequestWithMultipleContext(
            task,
            req,
            properties,
            root_task_ids,
            root_task_partition_ids,
            root_task_schema,
            addr);
        MockComputeClient client(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        client.runDispatchMPPTask(req);
    }

    std::vector<BlockInputStreamPtr> res;
    auto addr = server_config_map[root_task_partition_ids[0]].addr;
    for (size_t i = 0; i < root_task_ids.size(); ++i)
    {
        auto id = root_task_ids[i];
        auto partition_id = root_task_partition_ids[i];
        res.emplace_back(prepareRootExchangeReceiverWithMultipleContext(
            TiFlashTestEnv::getGlobalContext(TiFlashTestEnv::globalContextSize() - i - 1),
            properties,
            id,
            root_task_schema,
            server_config_map[partition_id].addr,
            addr));
    }
    auto top_stream = std::make_shared<UnionBlockInputStream<>>(res, BlockInputStreams{}, res.size(), 0, "mpp_root");
    return top_stream;
}

BlockInputStreamPtr executeQuery(
    Context & context,
    RegionID region_id,
    const DAGProperties & properties,
    QueryTasks & query_tasks,
    MakeResOutputStream & func_wrap_output_stream)
{
    if (properties.is_mpp_query)
    {
        return executeMPPQuery(context, properties, query_tasks);
    }
    else
    {
        return executeNonMPPQuery(context, region_id, properties, query_tasks, func_wrap_output_stream);
    }
}

tipb::SelectResponse executeDAGRequest(
    Context & context,
    tipb::DAGRequest & dag_request,
    RegionID region_id,
    UInt64 region_version,
    UInt64 region_conf_version,
    Timestamp start_ts,
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> & key_ranges)
{
    static auto log = Logger::get();
    LOG_DEBUG(log, "Handling DAG request: {}", dag_request.DebugString());
    tipb::SelectResponse dag_response;
    TablesRegionsInfo tables_regions_info(true);
    auto & table_regions_info = tables_regions_info.getSingleTableRegions();

    table_regions_info.local_regions.emplace(
        region_id,
        RegionInfo(region_id, region_version, region_conf_version, std::move(key_ranges), nullptr));

    DAGContext
        dag_context(dag_request, std::move(tables_regions_info), NullspaceID, "", DAGRequestKind::Cop, "", 0, "", log);
    context.setDAGContext(&dag_context);

    DAGDriver<DAGRequestKind::Cop> driver(context, start_ts, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, &dag_response, true);
    driver.execute();
    LOG_DEBUG(log, "Handle DAG request done");
    return dag_response;
}

bool runAndCompareDagReq(
    const coprocessor::Request & req,
    const coprocessor::Response & res,
    Context & context,
    String & unequal_msg)
{
    const kvrpcpb::Context & req_context = req.context();
    RegionID region_id = req_context.region_id();
    tipb::DAGRequest dag_request = getDAGRequestFromStringWithRetry(req.data());
    RegionPtr region = context.getTMTContext().getKVStore()->getRegion(region_id);
    if (!region)
        throw Exception(fmt::format("No such region: {}", region_id), ErrorCodes::BAD_ARGUMENTS);

    bool unequal_flag = false;
    DAGProperties properties = getDAGProperties("");
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> key_ranges = genCopKeyRange(req.ranges());
    static auto log = Logger::get();
    LOG_INFO(log, "Handling DAG request: {}", dag_request.DebugString());
    tipb::SelectResponse dag_response;
    TablesRegionsInfo tables_regions_info(true);
    auto & table_regions_info = tables_regions_info.getSingleTableRegions();
    table_regions_info.local_regions.emplace(
        region_id,
        RegionInfo(region_id, region->version(), region->confVer(), std::move(key_ranges), nullptr));

    DAGContext
        dag_context(dag_request, std::move(tables_regions_info), NullspaceID, "", DAGRequestKind::Cop, "", 0, "", log);
    context.setDAGContext(&dag_context);
    DAGDriver<DAGRequestKind::Cop>
        driver(context, properties.start_ts, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, &dag_response, true);
    driver.execute();

    auto resp_ptr = std::make_shared<tipb::SelectResponse>();
    if (!resp_ptr->ParseFromString(res.data()))
    {
        throw Exception("Incorrect json response data!", ErrorCodes::BAD_ARGUMENTS);
    }
    else
    {
        unequal_flag |= (!dagRspEqual(context, *resp_ptr, dag_response, unequal_msg));
    }
    return unequal_flag;
}
} // namespace DB
