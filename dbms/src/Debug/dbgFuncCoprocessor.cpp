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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <Common/typeid_cast.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataTypes/FieldToDataType.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Debug/dbgFuncCoprocessorUtils.h>
#include <Debug/dbgNaturalDag.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Server/MockComputeClient.h>
#include <Storages/IManageableStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/TypeMapping.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <tipb/select.pb.h>

#include <utility>
namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

using DAGColumnInfo = std::pair<String, ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;
using TiFlashTestEnv = tests::TiFlashTestEnv;
using ExecutorBinderPtr = mock::ExecutorBinderPtr;

class UniqRawResReformatBlockOutputStream : public IProfilingBlockInputStream
{
public:
    explicit UniqRawResReformatBlockOutputStream(const BlockInputStreamPtr & in_)
        : in(in_)
    {}

    String getName() const override { return "UniqRawResReformat"; }

    Block getHeader() const override { return in->getHeader(); }

protected:
    Block readImpl() override
    {
        while (true)
        {
            Block block = in->read();
            if (!block)
                return block;

            size_t num_columns = block.columns();
            MutableColumns columns(num_columns);
            for (size_t i = 0; i < num_columns; ++i)
            {
                ColumnWithTypeAndName & ori_column = block.getByPosition(i);

                if (std::string::npos != ori_column.name.find_first_of(uniq_raw_res_name))
                {
                    MutableColumnPtr mutable_holder = ori_column.column->cloneEmpty();

                    for (size_t j = 0; j < ori_column.column->size(); ++j)
                    {
                        Field field;
                        ori_column.column->get(j, field);

                        auto & str_ref = field.safeGet<String>();

                        ReadBufferFromString in(str_ref);
                        AggregateFunctionUniqUniquesHashSetDataForVariadicRawRes set;
                        set.set.read(in);

                        mutable_holder->insert(std::to_string(set.set.size()));
                    }
                    ori_column.column = std::move(mutable_holder);
                }
            }
            return block;
        }
    }

private:
    BlockInputStreamPtr in;
};

void setTipbRegionInfo(coprocessor::RegionInfo * tipb_region_info, const std::pair<RegionID, RegionPtr> & region, TableID table_id)
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

BlockInputStreamPtr constructExchangeReceiverStream(Context & context, tipb::ExchangeReceiver & tipb_exchange_receiver, const DAGProperties & properties, DAGSchema & root_task_schema, const String & root_addr, bool enable_local_tunnel)
{
    for (auto & field : root_task_schema)
    {
        auto tipb_type = TiDB::columnInfoToFieldType(field.second);
        tipb_type.set_collate(properties.collator);
        auto * field_type = tipb_exchange_receiver.add_field_types();
        *field_type = tipb_type;
    }

    mpp::TaskMeta root_tm;
    root_tm.set_start_ts(properties.start_ts);
    root_tm.set_address(root_addr);
    root_tm.set_task_id(-1);
    root_tm.set_partition_id(-1);

    std::shared_ptr<ExchangeReceiver> exchange_receiver
        = std::make_shared<ExchangeReceiver>(
            std::make_shared<GRPCReceiverContext>(
                tipb_exchange_receiver,
                root_tm,
                context.getTMTContext().getKVCluster(),
                context.getTMTContext().getMPPTaskManager(),
                enable_local_tunnel,
                context.getSettingsRef().enable_async_grpc_client),
            tipb_exchange_receiver.encoded_task_meta_size(),
            10,
            /*req_id=*/"",
            /*executor_id=*/"",
            /*fine_grained_shuffle_stream_count=*/0);
    BlockInputStreamPtr ret = std::make_shared<ExchangeReceiverInputStream>(exchange_receiver, /*req_id=*/"", /*executor_id=*/"", /*stream_id*/ 0);
    return ret;
}

BlockInputStreamPtr prepareRootExchangeReceiver(Context & context, const DAGProperties & properties, std::vector<Int64> & root_task_ids, DAGSchema & root_task_schema, bool enable_local_tunnel)
{
    tipb::ExchangeReceiver tipb_exchange_receiver;
    for (const auto root_task_id : root_task_ids)
    {
        mpp::TaskMeta tm;
        tm.set_start_ts(properties.start_ts);
        tm.set_address(Debug::LOCAL_HOST);
        tm.set_task_id(root_task_id);
        tm.set_partition_id(-1);
        auto * tm_string = tipb_exchange_receiver.add_encoded_task_meta();
        tm.AppendToString(tm_string);
    }
    return constructExchangeReceiverStream(context, tipb_exchange_receiver, properties, root_task_schema, Debug::LOCAL_HOST, enable_local_tunnel);
}

void prepareExchangeReceiverMetaWithMultipleContext(tipb::ExchangeReceiver & tipb_exchange_receiver, const DAGProperties & properties, Int64 task_id, String & addr)
{
    mpp::TaskMeta tm;
    tm.set_start_ts(properties.start_ts);
    tm.set_address(addr);
    tm.set_task_id(task_id);
    tm.set_partition_id(-1);
    auto * tm_string = tipb_exchange_receiver.add_encoded_task_meta();
    tm.AppendToString(tm_string);
}

BlockInputStreamPtr prepareRootExchangeReceiverWithMultipleContext(Context & context, const DAGProperties & properties, Int64 task_id, DAGSchema & root_task_schema, String & addr, String & root_addr)
{
    tipb::ExchangeReceiver tipb_exchange_receiver;

    prepareExchangeReceiverMetaWithMultipleContext(tipb_exchange_receiver, properties, task_id, addr);

    return constructExchangeReceiverStream(context, tipb_exchange_receiver, properties, root_task_schema, root_addr, true);
}

void prepareDispatchTaskRequest(QueryTask & task, std::shared_ptr<mpp::DispatchTaskRequest> req, const DAGProperties & properties, std::vector<Int64> & root_task_ids, DAGSchema & root_task_schema, String & addr)
{
    if (task.is_root_task)
    {
        root_task_ids.push_back(task.task_id);
        root_task_schema = task.result_schema;
    }
    auto * tm = req->mutable_meta();
    tm->set_start_ts(properties.start_ts);
    tm->set_partition_id(task.partition_id);
    tm->set_address(addr);
    tm->set_task_id(task.task_id);
    auto * encoded_plan = req->mutable_encoded_plan();
    task.dag_request->AppendToString(encoded_plan);
    req->set_timeout(properties.mpp_timeout);
    req->set_schema_ver(DEFAULT_UNSPECIFIED_SCHEMA_VERSION);
}

void prepareDispatchTaskRequestWithMultipleContext(QueryTask & task, std::shared_ptr<mpp::DispatchTaskRequest> req, const DAGProperties & properties, std::vector<Int64> & root_task_ids, std::vector<Int64> & root_task_partition_ids, DAGSchema & root_task_schema, String & addr)
{
    if (task.is_root_task)
    {
        root_task_ids.push_back(task.task_id);
        root_task_partition_ids.push_back(task.partition_id);
        root_task_schema = task.result_schema;
    }
    auto * tm = req->mutable_meta();
    tm->set_start_ts(properties.start_ts);
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
        auto req = std::make_shared<mpp::DispatchTaskRequest>();
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
                    auto regions = context.getTMTContext().getRegionTable().getRegionsByTable(partition_id);
                    for (size_t i = 0; i < regions.size(); ++i)
                    {
                        if ((current_region_size + i) % properties.mpp_partition_num != static_cast<size_t>(task.partition_id))
                            continue;
                        if (current_table_regions != nullptr && current_table_regions->physical_table_id() != partition_id)
                            current_table_regions = nullptr;
                        if (current_table_regions == nullptr)
                        {
                            current_table_regions = req->add_table_regions();
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
                auto regions = context.getTMTContext().getRegionTable().getRegionsByTable(table_id);
                if (regions.size() < static_cast<size_t>(properties.mpp_partition_num))
                    throw Exception("Not supported: table region num less than mpp partition num");
                for (size_t i = 0; i < regions.size(); ++i)
                {
                    if (i % properties.mpp_partition_num != static_cast<size_t>(task.partition_id))
                        continue;
                    setTipbRegionInfo(req->add_regions(), regions[i], table_id);
                }
            }
        }

        pingcap::kv::RpcCall<mpp::DispatchTaskRequest> call(req);
        context.getTMTContext().getCluster()->rpc_client->sendRequest(Debug::LOCAL_HOST, call, 1000);
        if (call.getResp()->has_error())
            throw Exception("Meet error while dispatch mpp task: " + call.getResp()->error().msg());
    }
    return prepareRootExchangeReceiver(context, properties, root_task_ids, root_task_schema, context.getSettingsRef().enable_local_tunnel);
}

std::vector<BlockInputStreamPtr> executeMPPQueryWithMultipleContext(const DAGProperties & properties, QueryTasks & query_tasks, std::unordered_map<size_t, MockServerConfig> & server_config_map)
{
    DAGSchema root_task_schema;
    std::vector<Int64> root_task_ids;
    std::vector<Int64> root_task_partition_ids;
    for (auto & task : query_tasks)
    {
        auto req = std::make_shared<mpp::DispatchTaskRequest>();

        auto addr = server_config_map[task.partition_id].addr;
        prepareDispatchTaskRequestWithMultipleContext(task, req, properties, root_task_ids, root_task_partition_ids, root_task_schema, addr);
        MockComputeClient client(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
        client.runDispatchMPPTask(req);
    }

    std::vector<BlockInputStreamPtr> res;
    auto addr = server_config_map[root_task_partition_ids[0]].addr;
    for (size_t i = 0; i < root_task_ids.size(); ++i)
    {
        auto id = root_task_ids[i];
        auto partition_id = root_task_partition_ids[i];
        res.emplace_back(prepareRootExchangeReceiverWithMultipleContext(TiFlashTestEnv::getGlobalContext(TiFlashTestEnv::globalContextSize() - i - 1), properties, id, root_task_schema, server_config_map[partition_id].addr, addr));
    }
    return res;
}

BlockInputStreamPtr executeNonMPPQuery(Context & context, RegionID region_id, const DAGProperties & properties, QueryTasks & query_tasks, MakeResOutputStream & func_wrap_output_stream)
{
    auto & task = query_tasks[0];
    auto table_id = task.table_id;
    RegionPtr region;
    if (region_id == InvalidRegionID)
    {
        auto regions = context.getTMTContext().getRegionTable().getRegionsByTable(table_id);
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
    DecodedTiKVKeyPtr start_key = std::make_shared<DecodedTiKVKey>(RecordKVFormat::genRawKey(table_id, handle_range.first.handle_id));
    DecodedTiKVKeyPtr end_key = std::make_shared<DecodedTiKVKey>(RecordKVFormat::genRawKey(table_id, handle_range.second.handle_id));
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

BlockInputStreamPtr executeQuery(Context & context, RegionID region_id, const DAGProperties & properties, QueryTasks & query_tasks, MakeResOutputStream & func_wrap_output_stream)
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


void dbgFuncTiDBQueryFromNaturalDag(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: json_dag_path", ErrorCodes::BAD_ARGUMENTS);

    auto json_dag_path = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    auto dag = NaturalDag(json_dag_path, &Poco::Logger::get("MockDAG"));
    dag.init();
    dag.build(context);
    std::vector<std::pair<int32_t, String>> failed_req_msg_vec;
    int req_idx = 0;
    for (const auto & it : dag.getReqAndRspVec())
    {
        auto && req = it.first;
        auto && res = it.second;
        int32_t req_id = dag.getReqIDVec()[req_idx];
        bool unequal_flag = false;
        bool failed_flag = false;
        String unequal_msg;
        static auto log = Logger::get("MockDAG");
        try
        {
            unequal_flag = runAndCompareDagReq(req, res, context, unequal_msg);
        }
        catch (const Exception & e)
        {
            failed_flag = true;
            unequal_msg = e.message();
        }
        catch (...)
        {
            failed_flag = true;
            unequal_msg = "Unknown execution exception!";
        }

        if (unequal_flag || failed_flag)
        {
            failed_req_msg_vec.push_back(std::make_pair(req_id, unequal_msg));
            if (!dag.continueWhenError())
                break;
        }
        ++req_idx;
    }
    dag.clean(context);
    if (!failed_req_msg_vec.empty())
    {
        output("Invalid");
        FmtBuffer fmt_buf;
        fmt_buf.joinStr(
            failed_req_msg_vec.begin(),
            failed_req_msg_vec.end(),
            [](const auto & pair, FmtBuffer & fb) { fb.fmtAppend("request {} failed, msg: {}", pair.first, pair.second); },
            "\n");
        throw Exception(fmt_buf.toString(), ErrorCodes::LOGICAL_ERROR);
    }
}

BlockInputStreamPtr dbgFuncTiDBQuery(Context & context, const ASTs & args)
{
    if (args.empty() || args.size() > 3)
        throw Exception("Args not matched, should be: query[, region-id, dag_prop_string]", ErrorCodes::BAD_ARGUMENTS);

    auto query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID region_id = InvalidRegionID;
    if (args.size() >= 2)
        region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);

    String prop_string;
    if (args.size() == 3)
        prop_string = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    DAGProperties properties = getDAGProperties(prop_string);
    properties.start_ts = context.getTMTContext().getPDClient()->getTS();

    auto [query_tasks, func_wrap_output_stream] = compileQuery(
        context,
        query,
        [&](const String & database_name, const String & table_name) {
            auto storage = context.getTable(database_name, table_name);
            auto managed_storage = std::dynamic_pointer_cast<IManageableStorage>(storage);
            if (!managed_storage //
                || !(managed_storage->engineType() == ::TiDB::StorageEngine::DT
                     || managed_storage->engineType() == ::TiDB::StorageEngine::TMT))
                throw Exception(database_name + "." + table_name + " is not ManageableStorage", ErrorCodes::BAD_ARGUMENTS);
            return managed_storage->getTableInfo();
        },
        properties);

    return executeQuery(context, region_id, properties, query_tasks, func_wrap_output_stream);
}

BlockInputStreamPtr dbgFuncMockTiDBQuery(Context & context, const ASTs & args)
{
    if (args.size() < 2 || args.size() > 4)
        throw Exception("Args not matched, should be: query, region-id[, start-ts, dag_prop_string]", ErrorCodes::BAD_ARGUMENTS);

    auto query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    auto region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);
    Timestamp start_ts = DEFAULT_MAX_READ_TSO;
    if (args.size() >= 3)
        start_ts = safeGet<Timestamp>(typeid_cast<const ASTLiteral &>(*args[2]).value);
    if (start_ts == 0)
        start_ts = context.getTMTContext().getPDClient()->getTS();

    String prop_string;
    if (args.size() == 4)
        prop_string = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[3]).value);
    DAGProperties properties = getDAGProperties(prop_string);
    properties.start_ts = start_ts;

    auto [query_tasks, func_wrap_output_stream] = compileQuery(
        context,
        query,
        [&](const String & database_name, const String & table_name) {
            return MockTiDB::instance().getTableByName(database_name, table_name)->table_info;
        },
        properties);

    return executeQuery(context, region_id, properties, query_tasks, func_wrap_output_stream);
}

struct QueryFragment
{
    ExecutorBinderPtr root_executor;
    TableID table_id;
    bool is_top_fragment;
    std::vector<Int64> sender_target_task_ids;
    std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;
    std::vector<Int64> task_ids;
    QueryFragment(ExecutorBinderPtr root_executor_, TableID table_id_, bool is_top_fragment_, std::vector<Int64> && sender_target_task_ids_ = {}, std::unordered_map<String, std::vector<Int64>> && receiver_source_task_ids_map_ = {}, std::vector<Int64> && task_ids_ = {})
        : root_executor(std::move(root_executor_))
        , table_id(table_id_)
        , is_top_fragment(is_top_fragment_)
        , sender_target_task_ids(std::move(sender_target_task_ids_))
        , receiver_source_task_ids_map(std::move(receiver_source_task_ids_map_))
        , task_ids(std::move(task_ids_))
    {}

    QueryTask toQueryTask(const DAGProperties & properties, MPPInfo & mpp_info, const Context & context) const
    {
        std::shared_ptr<tipb::DAGRequest> dag_request_ptr = std::make_shared<tipb::DAGRequest>();
        tipb::DAGRequest & dag_request = *dag_request_ptr;
        dag_request.set_time_zone_name(properties.tz_name);
        dag_request.set_time_zone_offset(properties.tz_offset);
        dag_request.set_flags(dag_request.flags() | (1u << 1u /* TRUNCATE_AS_WARNING */) | (1u << 6u /* OVERFLOW_AS_WARNING */));
        if (is_top_fragment)
        {
            if (properties.encode_type == "chunk")
                dag_request.set_encode_type(tipb::EncodeType::TypeChunk);
            else if (properties.encode_type == "chblock")
                dag_request.set_encode_type(tipb::EncodeType::TypeCHBlock);
            else
                dag_request.set_encode_type(tipb::EncodeType::TypeDefault);
        }
        else
        {
            dag_request.set_encode_type(tipb::EncodeType::TypeCHBlock);
        }

        for (size_t i = 0; i < root_executor->output_schema.size(); i++)
            dag_request.add_output_offsets(i);
        auto * root_tipb_executor = dag_request.mutable_root_executor();
        root_executor->toTiPBExecutor(root_tipb_executor, properties.collator, mpp_info, context);
        return QueryTask(dag_request_ptr, table_id, root_executor->output_schema, mpp_info.sender_target_task_ids.empty() ? QueryTaskType::DAG : QueryTaskType::MPP_DISPATCH, mpp_info.task_id, mpp_info.partition_id, is_top_fragment);
    }

    QueryTasks toQueryTasks(const DAGProperties & properties, const Context & context) const
    {
        QueryTasks ret;
        if (properties.is_mpp_query)
        {
            for (size_t partition_id = 0; partition_id < task_ids.size(); partition_id++)
            {
                MPPInfo mpp_info(
                    properties.start_ts,
                    partition_id,
                    task_ids[partition_id],
                    sender_target_task_ids,
                    receiver_source_task_ids_map);
                ret.push_back(toQueryTask(properties, mpp_info, context));
            }
        }
        else
        {
            MPPInfo mpp_info(properties.start_ts, /*partition_id*/ -1, /*task_id*/ -1, /*sender_target_task_ids*/ {}, /*receiver_source_task_ids_map*/ {});
            ret.push_back(toQueryTask(properties, mpp_info, context));
        }
        return ret;
    }
};

using QueryFragments = std::vector<QueryFragment>;

TableID findTableIdForQueryFragment(ExecutorBinderPtr root_executor, bool must_have_table_id)
{
    ExecutorBinderPtr current_executor = root_executor;
    while (!current_executor->children.empty())
    {
        ExecutorBinderPtr non_exchange_child;
        for (const auto & c : current_executor->children)
        {
            if (dynamic_cast<mock::ExchangeReceiverBinder *>(c.get()))
                continue;
            if (non_exchange_child != nullptr)
                throw Exception("More than one non-exchange child, should not happen");
            non_exchange_child = c;
        }
        if (non_exchange_child == nullptr)
        {
            if (must_have_table_id)
                throw Exception("Table scan not found");
            return -1;
        }
        current_executor = non_exchange_child;
    }
    auto * ts = dynamic_cast<mock::TableScanBinder *>(current_executor.get());
    if (ts == nullptr)
    {
        if (must_have_table_id)
            throw Exception("Table scan not found");
        return -1;
    }
    return ts->getTableId();
}

QueryFragments mppQueryToQueryFragments(
    ExecutorBinderPtr root_executor,
    size_t & executor_index,
    const DAGProperties & properties,
    bool for_root_fragment,
    MPPCtxPtr mpp_ctx)
{
    QueryFragments fragments;
    std::unordered_map<String, std::pair<std::shared_ptr<mock::ExchangeReceiverBinder>, std::shared_ptr<mock::ExchangeSenderBinder>>> exchange_map;
    root_executor->toMPPSubPlan(executor_index, properties, exchange_map);
    TableID table_id = findTableIdForQueryFragment(root_executor, exchange_map.empty());
    std::vector<Int64> sender_target_task_ids = mpp_ctx->sender_target_task_ids;
    std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;
    size_t current_task_num = properties.mpp_partition_num;
    for (auto & exchange : exchange_map)
    {
        if (exchange.second.second->getType() == tipb::ExchangeType::PassThrough)
        {
            current_task_num = 1;
            break;
        }
    }
    std::vector<Int64> current_task_ids;
    for (size_t i = 0; i < current_task_num; i++)
        current_task_ids.push_back(mpp_ctx->next_task_id++);
    for (auto & exchange : exchange_map)
    {
        mpp_ctx->sender_target_task_ids = current_task_ids;
        auto sub_fragments = mppQueryToQueryFragments(exchange.second.second, executor_index, properties, false, mpp_ctx);
        receiver_source_task_ids_map[exchange.first] = sub_fragments[sub_fragments.size() - 1].task_ids;
        fragments.insert(fragments.end(), sub_fragments.begin(), sub_fragments.end());
    }
    fragments.emplace_back(root_executor, table_id, for_root_fragment, std::move(sender_target_task_ids), std::move(receiver_source_task_ids_map), std::move(current_task_ids));
    return fragments;
}

QueryFragments queryPlanToQueryFragments(const DAGProperties & properties, ExecutorBinderPtr root_executor, size_t & executor_index)
{
    if (properties.is_mpp_query)
    {
        ExecutorBinderPtr root_exchange_sender
            = std::make_shared<mock::ExchangeSenderBinder>(executor_index, root_executor->output_schema, tipb::PassThrough);
        root_exchange_sender->children.push_back(root_executor);
        root_executor = root_exchange_sender;
        MPPCtxPtr mpp_ctx = std::make_shared<MPPCtx>(properties.start_ts);
        mpp_ctx->sender_target_task_ids.emplace_back(-1);
        return mppQueryToQueryFragments(root_executor, executor_index, properties, true, mpp_ctx);
    }
    else
    {
        QueryFragments fragments;
        fragments.emplace_back(root_executor, findTableIdForQueryFragment(root_executor, true), true);
        return fragments;
    }
}

QueryTasks queryPlanToQueryTasks(
    const DAGProperties & properties,
    ExecutorBinderPtr root_executor,
    size_t & executor_index,
    const Context & context)
{
    QueryFragments fragments = queryPlanToQueryFragments(properties, root_executor, executor_index);
    QueryTasks tasks;
    for (auto & fragment : fragments)
    {
        auto t = fragment.toQueryTasks(properties, context);
        tasks.insert(tasks.end(), t.begin(), t.end());
    }
    return tasks;
}

const ASTTablesInSelectQueryElement * getJoin(ASTSelectQuery & ast_query)
{
    if (!ast_query.tables)
        return nullptr;

    const auto & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*ast_query.tables);
    if (tables_in_select_query.children.empty())
        return nullptr;

    const ASTTablesInSelectQueryElement * joined_table = nullptr;
    for (const auto & child : tables_in_select_query.children)
    {
        const auto & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*child);
        if (tables_element.table_join)
        {
            if (!joined_table)
                joined_table = &tables_element;
            else
                throw Exception("Support for more than one JOIN in query is not implemented", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
    return joined_table;
}

std::pair<ExecutorBinderPtr, bool> compileQueryBlock(
    Context & context,
    size_t & executor_index,
    SchemaFetcher schema_fetcher,
    const DAGProperties & properties,
    ASTSelectQuery & ast_query)
{
    const auto * joined_table = getJoin(ast_query);
    /// uniq_raw is used to test `ApproxCountDistinct`, when testing `ApproxCountDistinct` in mock coprocessor
    /// the return value of `ApproxCountDistinct` is just the raw result, we need to convert it to a readable
    /// value when decoding the result(using `UniqRawResReformatBlockOutputStream`)
    bool has_uniq_raw_res = false;
    ExecutorBinderPtr root_executor = nullptr;

    TableInfo table_info;
    String table_alias;
    {
        String database_name, table_name;
        auto query_database = ast_query.database();
        auto query_table = ast_query.table();
        if (query_database)
            database_name = typeid_cast<ASTIdentifier &>(*query_database).name;
        if (query_table)
            table_name = typeid_cast<ASTIdentifier &>(*query_table).name;
        if (!query_table)
        {
            database_name = "system";
            table_name = "one";
        }
        else if (!query_database)
        {
            database_name = context.getCurrentDatabase();
        }
        table_alias = query_table->tryGetAlias();
        if (table_alias.empty())
            table_alias = table_name;

        table_info = schema_fetcher(database_name, table_name);
    }

    if (!joined_table)
    {
        /// Table scan.
        {
            bool append_pk_column = false;
            for (const auto & expr : ast_query.select_expression_list->children)
            {
                if (auto * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
                {
                    if (identifier->getColumnName() == MutableSupport::tidb_pk_column_name)
                    {
                        append_pk_column = true;
                    }
                }
            }
            root_executor = mock::compileTableScan(executor_index, table_info, "", table_alias, append_pk_column);
        }
    }
    else
    {
        TableInfo left_table_info = table_info;
        auto const & left_table_alias = table_alias;
        TableInfo right_table_info;
        String right_table_alias;
        {
            String database_name, table_name;
            const auto & table_to_join = static_cast<const ASTTableExpression &>(*joined_table->table_expression);
            if (table_to_join.database_and_table_name)
            {
                auto identifier = static_cast<const ASTIdentifier &>(*table_to_join.database_and_table_name);
                table_name = identifier.name;
                if (!identifier.children.empty())
                {
                    if (identifier.children.size() != 2)
                        throw Exception("Qualified table name could have only two components", ErrorCodes::LOGICAL_ERROR);

                    database_name = typeid_cast<const ASTIdentifier &>(*identifier.children[0]).name;
                    table_name = typeid_cast<const ASTIdentifier &>(*identifier.children[1]).name;
                }
                if (database_name.empty())
                    database_name = context.getCurrentDatabase();

                right_table_alias = table_to_join.database_and_table_name->tryGetAlias();
                if (right_table_alias.empty())
                    right_table_alias = table_name;

                right_table_info = schema_fetcher(database_name, table_name);
            }
            else
            {
                throw Exception("subquery not supported as join source");
            }
        }
        /// Table scan.
        bool left_append_pk_column = false;
        bool right_append_pk_column = false;
        for (const auto & expr : ast_query.select_expression_list->children)
        {
            if (auto * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
            {
                auto [db_name, table_name, column_name] = splitQualifiedName(identifier->getColumnName());
                if (column_name == MutableSupport::tidb_pk_column_name)
                {
                    if (table_name.empty())
                    {
                        throw Exception("tidb pk column must be qualified since there are more than one tables");
                    }
                    if (table_name == left_table_alias)
                        left_append_pk_column = true;
                    else if (table_name == right_table_alias)
                        right_append_pk_column = true;
                    else
                        throw Exception("Unknown table alias: " + table_name);
                }
            }
        }
        auto left_ts = mock::compileTableScan(executor_index, left_table_info, "", left_table_alias, left_append_pk_column);
        auto right_ts = mock::compileTableScan(executor_index, right_table_info, "", right_table_alias, right_append_pk_column);
        root_executor = mock::compileJoin(executor_index, left_ts, right_ts, joined_table->table_join);
    }

    /// Filter.
    if (ast_query.where_expression)
    {
        root_executor = compileSelection(root_executor, executor_index, ast_query.where_expression);
    }

    /// TopN.
    if (ast_query.order_expression_list && ast_query.limit_length)
    {
        root_executor = compileTopN(root_executor, executor_index, ast_query.order_expression_list, ast_query.limit_length);
    }
    else if (ast_query.limit_length)
    {
        root_executor = compileLimit(root_executor, executor_index, ast_query.limit_length);
    }

    bool has_gby = ast_query.group_expression_list != nullptr;
    bool has_agg_func = false;
    for (const auto & child : ast_query.select_expression_list->children)
    {
        const auto * func = typeid_cast<const ASTFunction *>(child.get());
        if (func && AggregateFunctionFactory::instance().isAggregateFunctionName(func->name))
        {
            has_agg_func = true;
            break;
        }
    }
    DAGSchema final_schema;
    /// Aggregation
    std::vector<size_t> output_offsets;
    if (has_gby || has_agg_func)
    {
        if (!properties.is_mpp_query
            && (dynamic_cast<mock::LimitBinder *>(root_executor.get()) != nullptr || dynamic_cast<mock::TopNBinder *>(root_executor.get()) != nullptr))
            throw Exception("Limit/TopN and Agg cannot co-exist in non-mpp mode.", ErrorCodes::LOGICAL_ERROR);

        root_executor = compileAggregation(
            root_executor,
            executor_index,
            ast_query.select_expression_list,
            has_gby ? ast_query.group_expression_list : nullptr);

        if (dynamic_cast<mock::AggregationBinder *>(root_executor.get())->hasUniqRawRes())
        {
            // todo support uniq_raw in mpp mode
            if (properties.is_mpp_query)
                throw Exception("uniq_raw_res not supported in mpp mode.", ErrorCodes::LOGICAL_ERROR);
            else
                has_uniq_raw_res = true;
        }

        auto * agg = dynamic_cast<mock::AggregationBinder *>(root_executor.get());
        if (agg->needAppendProject() || ast_query.select_expression_list->children.size() != agg->exprSize())
        {
            /// Project if needed
            root_executor = compileProject(root_executor, executor_index, ast_query.select_expression_list);
        }
    }
    else
    {
        /// Project
        root_executor = compileProject(root_executor, executor_index, ast_query.select_expression_list);
    }
    return std::make_pair(root_executor, has_uniq_raw_res);
}

std::tuple<QueryTasks, MakeResOutputStream> compileQuery(
    Context & context,
    const String & query,
    SchemaFetcher schema_fetcher,
    const DAGProperties & properties)
{
    MakeResOutputStream func_wrap_output_stream = [](BlockInputStreamPtr in) {
        return in;
    };

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "from DAG compiler", 0);
    ASTSelectQuery & ast_query = typeid_cast<ASTSelectQuery &>(*ast);

    size_t executor_index = 0;
    auto [root_executor, has_uniq_raw_res] = compileQueryBlock(context, executor_index, schema_fetcher, properties, ast_query);
    if (has_uniq_raw_res)
        func_wrap_output_stream = [](BlockInputStreamPtr in) {
            return std::make_shared<UniqRawResReformatBlockOutputStream>(in);
        };

    /// finalize
    std::unordered_set<String> used_columns;
    for (auto & schema : root_executor->output_schema)
        used_columns.emplace(schema.first);
    root_executor->columnPrune(used_columns);

    return std::make_tuple(queryPlanToQueryTasks(properties, root_executor, executor_index, context), func_wrap_output_stream);
}

} // namespace DB
