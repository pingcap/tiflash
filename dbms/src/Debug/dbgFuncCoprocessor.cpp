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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniq.h>
#include <Common/typeid_cast.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataTypes/FieldToDataType.h>
#include <Debug/DAGProperties.h>
#include <Debug/MockTiDB.h>
#include <Debug/astToExecutor.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Debug/dbgNaturalDag.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/CoprocessorHandler.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/sortBlock.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/StringTokenizer.h>
#include <Storages/IManageableStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <tipb/select.pb.h>

#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int NO_SUCH_COLUMN_IN_TABLE;
} // namespace ErrorCodes

using DAGColumnInfo = std::pair<String, ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;
static const String ENCODE_TYPE_NAME = "encode_type";
static const String TZ_OFFSET_NAME = "tz_offset";
static const String TZ_NAME_NAME = "tz_name";
static const String COLLATOR_NAME = "collator";
static const String MPP_QUERY = "mpp_query";
static const String USE_BROADCAST_JOIN = "use_broadcast_join";
static const String MPP_PARTITION_NUM = "mpp_partition_num";
static const String MPP_TIMEOUT = "mpp_timeout";

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

tipb::SelectResponse executeDAGRequest(Context & context, const tipb::DAGRequest & dag_request, RegionID region_id, UInt64 region_version, UInt64 region_conf_version, Timestamp start_ts, std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> & key_ranges);
bool runAndCompareDagReq(const coprocessor::Request & req, const coprocessor::Response & res, Context & context, String & unequal_msg);
BlockInputStreamPtr outputDAGResponse(Context & context, const DAGSchema & schema, const tipb::SelectResponse & dag_response);
DAGSchema getSelectSchema(Context & context);
bool dagRspEqual(Context & context, const tipb::SelectResponse & expected, const tipb::SelectResponse & actual, String & unequal_msg);

DAGProperties getDAGProperties(const String & prop_string)
{
    DAGProperties ret;
    if (prop_string.empty())
        return ret;
    std::unordered_map<String, String> properties;
    Poco::StringTokenizer string_tokens(prop_string, ",");
    for (const auto & string_token : string_tokens)
    {
        Poco::StringTokenizer tokens(string_token, ":");
        if (tokens.count() != 2)
            continue;
        properties[Poco::toLower(tokens[0])] = tokens[1];
    }

    if (properties.find(ENCODE_TYPE_NAME) != properties.end())
        ret.encode_type = properties[ENCODE_TYPE_NAME];
    if (properties.find(TZ_OFFSET_NAME) != properties.end())
        ret.tz_offset = std::stol(properties[TZ_OFFSET_NAME]);
    if (properties.find(TZ_NAME_NAME) != properties.end())
        ret.tz_name = properties[TZ_NAME_NAME];
    if (properties.find(COLLATOR_NAME) != properties.end())
        ret.collator = std::stoi(properties[COLLATOR_NAME]);
    if (properties.find(MPP_QUERY) != properties.end())
        ret.is_mpp_query = properties[MPP_QUERY] == "true";
    if (properties.find(USE_BROADCAST_JOIN) != properties.end())
        ret.use_broadcast_join = properties[USE_BROADCAST_JOIN] == "true";
    if (properties.find(MPP_PARTITION_NUM) != properties.end())
        ret.mpp_partition_num = std::stoi(properties[MPP_PARTITION_NUM]);
    if (properties.find(MPP_TIMEOUT) != properties.end())
        ret.mpp_timeout = std::stoi(properties[MPP_TIMEOUT]);

    return ret;
}

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

BlockInputStreamPtr executeQuery(Context & context, RegionID region_id, const DAGProperties & properties, QueryTasks & query_tasks, MakeResOutputStream & func_wrap_output_stream)
{
    if (properties.is_mpp_query)
    {
        DAGSchema root_task_schema;
        std::vector<Int64> root_task_ids;
        for (auto & task : query_tasks)
        {
            if (task.is_root_task)
            {
                root_task_ids.push_back(task.task_id);
                root_task_schema = task.result_schema;
            }
            auto req = std::make_shared<mpp::DispatchTaskRequest>();
            auto * tm = req->mutable_meta();
            tm->set_start_ts(properties.start_ts);
            tm->set_partition_id(task.partition_id);
            tm->set_address(Debug::LOCAL_HOST);
            tm->set_task_id(task.task_id);
            auto * encoded_plan = req->mutable_encoded_plan();
            task.dag_request->AppendToString(encoded_plan);
            req->set_timeout(properties.mpp_timeout);
            req->set_schema_ver(DEFAULT_UNSPECIFIED_SCHEMA_VERSION);
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
        for (auto & field : root_task_schema)
        {
            auto tipb_type = TiDB::columnInfoToFieldType(field.second);
            tipb_type.set_collate(properties.collator);
            auto * field_type = tipb_exchange_receiver.add_field_types();
            *field_type = tipb_type;
        }
        mpp::TaskMeta root_tm;
        root_tm.set_start_ts(properties.start_ts);
        root_tm.set_address(Debug::LOCAL_HOST);
        root_tm.set_task_id(-1);
        root_tm.set_partition_id(-1);
        std::shared_ptr<ExchangeReceiver> exchange_receiver
            = std::make_shared<ExchangeReceiver>(
                std::make_shared<GRPCReceiverContext>(
                    tipb_exchange_receiver,
                    root_tm,
                    context.getTMTContext().getKVCluster(),
                    context.getTMTContext().getMPPTaskManager(),
                    context.getSettingsRef().enable_local_tunnel,
                    context.getSettingsRef().enable_async_grpc_client),
                tipb_exchange_receiver.encoded_task_meta_size(),
                10,
                /*req_id=*/"",
                /*executor_id=*/"");
        BlockInputStreamPtr ret = std::make_shared<ExchangeReceiverInputStream>(exchange_receiver, /*req_id=*/"", /*executor_id=*/"");
        return ret;
    }
    else
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
}

void dbgFuncTiDBQueryFromNaturalDag(Context & context, const ASTs & args, DBGInvoker::Printer output)
{
    if (args.size() != 1)
        throw Exception("Args not matched, should be: json_dag_path", ErrorCodes::BAD_ARGUMENTS);

    String json_dag_path = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
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

bool runAndCompareDagReq(const coprocessor::Request & req, const coprocessor::Response & res, Context & context, String & unequal_msg)
{
    const kvrpcpb::Context & req_context = req.context();
    RegionID region_id = req_context.region_id();
    tipb::DAGRequest dag_request = getDAGRequestFromStringWithRetry(req.data());
    RegionPtr region = context.getTMTContext().getKVStore()->getRegion(region_id);
    if (!region)
        throw Exception(fmt::format("No such region: {}", region_id), ErrorCodes::BAD_ARGUMENTS);

    bool unequal_flag = false;
    DAGProperties properties = getDAGProperties("");
    std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> key_ranges = CoprocessorHandler::GenCopKeyRange(req.ranges());
    static auto log = Logger::get("MockDAG");
    LOG_FMT_INFO(log, "Handling DAG request: {}", dag_request.DebugString());
    tipb::SelectResponse dag_response;
    TablesRegionsInfo tables_regions_info(true);
    auto & table_regions_info = tables_regions_info.getSingleTableRegions();
    table_regions_info.local_regions.emplace(region_id, RegionInfo(region_id, region->version(), region->confVer(), std::move(key_ranges), nullptr));

    DAGContext dag_context(dag_request);
    dag_context.tables_regions_info = std::move(tables_regions_info);
    dag_context.log = log;
    context.setDAGContext(&dag_context);
    DAGDriver driver(context, properties.start_ts, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, &dag_response, true);
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

BlockInputStreamPtr dbgFuncTiDBQuery(Context & context, const ASTs & args)
{
    if (args.empty() || args.size() > 3)
        throw Exception("Args not matched, should be: query[, region-id, dag_prop_string]", ErrorCodes::BAD_ARGUMENTS);

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
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

    String query = safeGet<String>(typeid_cast<const ASTLiteral &>(*args[0]).value);
    RegionID region_id = safeGet<RegionID>(typeid_cast<const ASTLiteral &>(*args[1]).value);
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
    ExecutorPtr root_executor;
    TableID table_id;
    bool is_top_fragment;
    std::vector<Int64> sender_target_task_ids;
    std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;
    std::vector<Int64> task_ids;
    QueryFragment(ExecutorPtr root_executor_, TableID table_id_, bool is_top_fragment_, std::vector<Int64> && sender_target_task_ids_ = {}, std::unordered_map<String, std::vector<Int64>> && receiver_source_task_ids_map_ = {}, std::vector<Int64> && task_ids_ = {})
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
        return QueryTask(dag_request_ptr, table_id, root_executor->output_schema, mpp_info.sender_target_task_ids.empty() ? DAG : MPP_DISPATCH, mpp_info.task_id, mpp_info.partition_id, is_top_fragment);
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

TableID findTableIdForQueryFragment(ExecutorPtr root_executor, bool must_have_table_id)
{
    ExecutorPtr current_executor = root_executor;
    while (!current_executor->children.empty())
    {
        ExecutorPtr non_exchange_child;
        for (const auto & c : current_executor->children)
        {
            if (dynamic_cast<mock::ExchangeReceiver *>(c.get()))
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
    auto * ts = dynamic_cast<mock::TableScan *>(current_executor.get());
    if (ts == nullptr)
    {
        if (must_have_table_id)
            throw Exception("Table scan not found");
        return -1;
    }
    return ts->table_info.id;
}

QueryFragments mppQueryToQueryFragments(
    ExecutorPtr root_executor,
    size_t & executor_index,
    const DAGProperties & properties,
    bool for_root_fragment,
    MPPCtxPtr mpp_ctx)
{
    QueryFragments fragments;
    std::unordered_map<String, std::pair<std::shared_ptr<mock::ExchangeReceiver>, std::shared_ptr<mock::ExchangeSender>>> exchange_map;
    root_executor->toMPPSubPlan(executor_index, properties, exchange_map);
    TableID table_id = findTableIdForQueryFragment(root_executor, exchange_map.empty());
    std::vector<Int64> sender_target_task_ids = mpp_ctx->sender_target_task_ids;
    std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;
    size_t current_task_num = properties.mpp_partition_num;
    for (auto & exchange : exchange_map)
    {
        if (exchange.second.second->type == tipb::ExchangeType::PassThrough)
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
        receiver_source_task_ids_map[exchange.first] = sub_fragments.cbegin()->task_ids;
        fragments.insert(fragments.end(), sub_fragments.begin(), sub_fragments.end());
    }
    fragments.emplace_back(root_executor, table_id, for_root_fragment, std::move(sender_target_task_ids), std::move(receiver_source_task_ids_map), std::move(current_task_ids));
    return fragments;
}

QueryFragments queryPlanToQueryFragments(const DAGProperties & properties, ExecutorPtr root_executor, size_t & executor_index)
{
    if (properties.is_mpp_query)
    {
        ExecutorPtr root_exchange_sender
            = std::make_shared<mock::ExchangeSender>(executor_index, root_executor->output_schema, tipb::PassThrough);
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
    ExecutorPtr root_executor,
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

    const ASTTablesInSelectQuery & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*ast_query.tables);
    if (tables_in_select_query.children.empty())
        return nullptr;

    const ASTTablesInSelectQueryElement * joined_table = nullptr;
    for (const auto & child : tables_in_select_query.children)
    {
        const ASTTablesInSelectQueryElement & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*child);
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

std::pair<ExecutorPtr, bool> compileQueryBlock(
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
    ExecutorPtr root_executor = nullptr;

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
                if (ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
                {
                    if (identifier->getColumnName() == MutableSupport::tidb_pk_column_name)
                    {
                        append_pk_column = true;
                    }
                }
            }
            root_executor = compileTableScan(executor_index, table_info, table_alias, append_pk_column);
        }
    }
    else
    {
        TableInfo left_table_info = table_info;
        String left_table_alias = table_alias;
        TableInfo right_table_info;
        String right_table_alias;
        {
            String database_name, table_name;
            const ASTTableExpression & table_to_join = static_cast<const ASTTableExpression &>(*joined_table->table_expression);
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
            if (ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(expr.get()))
            {
                auto names = splitQualifiedName(identifier->getColumnName());
                if (names.second == MutableSupport::tidb_pk_column_name)
                {
                    if (names.first.empty())
                    {
                        throw Exception("tidb pk column must be qualified since there are more than one tables");
                    }
                    if (names.first == left_table_alias)
                        left_append_pk_column = true;
                    else if (names.first == right_table_alias)
                        right_append_pk_column = true;
                    else
                        throw Exception("Unknown table alias: " + names.first);
                }
            }
        }
        auto left_ts = compileTableScan(executor_index, left_table_info, left_table_alias, left_append_pk_column);
        auto right_ts = compileTableScan(executor_index, right_table_info, right_table_alias, right_append_pk_column);
        root_executor = compileJoin(executor_index, left_ts, right_ts, joined_table->table_join);
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
        const ASTFunction * func = typeid_cast<const ASTFunction *>(child.get());
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
            && (dynamic_cast<mock::Limit *>(root_executor.get()) != nullptr || dynamic_cast<mock::TopN *>(root_executor.get()) != nullptr))
            throw Exception("Limit/TopN and Agg cannot co-exist in non-mpp mode.", ErrorCodes::LOGICAL_ERROR);

        root_executor = compileAggregation(
            root_executor,
            executor_index,
            ast_query.select_expression_list,
            has_gby ? ast_query.group_expression_list : nullptr);

        if (dynamic_cast<mock::Aggregation *>(root_executor.get())->has_uniq_raw_res)
        {
            // todo support uniq_raw in mpp mode
            if (properties.is_mpp_query)
                throw Exception("uniq_raw_res not supported in mpp mode.", ErrorCodes::LOGICAL_ERROR);
            else
                has_uniq_raw_res = true;
        }

        auto * agg = dynamic_cast<mock::Aggregation *>(root_executor.get());
        if (agg->need_append_project || ast_query.select_expression_list->children.size() != agg->agg_exprs.size() + agg->gby_exprs.size())
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

tipb::SelectResponse executeDAGRequest(Context & context, const tipb::DAGRequest & dag_request, RegionID region_id, UInt64 region_version, UInt64 region_conf_version, Timestamp start_ts, std::vector<std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr>> & key_ranges)
{
    static auto log = Logger::get("MockDAG");
    LOG_FMT_DEBUG(log, "Handling DAG request: {}", dag_request.DebugString());
    tipb::SelectResponse dag_response;
    TablesRegionsInfo tables_regions_info(true);
    auto & table_regions_info = tables_regions_info.getSingleTableRegions();

    table_regions_info.local_regions.emplace(region_id, RegionInfo(region_id, region_version, region_conf_version, std::move(key_ranges), nullptr));

    DAGContext dag_context(dag_request);
    dag_context.tables_regions_info = std::move(tables_regions_info);
    dag_context.log = log;
    context.setDAGContext(&dag_context);

    DAGDriver driver(context, start_ts, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, &dag_response, true);
    driver.execute();
    LOG_FMT_DEBUG(log, "Handle DAG request done");
    return dag_response;
}

std::unique_ptr<ChunkCodec> getCodec(tipb::EncodeType encode_type)
{
    switch (encode_type)
    {
    case tipb::EncodeType::TypeDefault:
        return std::make_unique<DefaultChunkCodec>();
    case tipb::EncodeType::TypeChunk:
        return std::make_unique<ArrowChunkCodec>();
    case tipb::EncodeType::TypeCHBlock:
        return std::make_unique<CHBlockChunkCodec>();
    default:
        throw Exception("Unsupported encode type", ErrorCodes::BAD_ARGUMENTS);
    }
}

void chunksToBlocks(const DAGSchema & schema, const tipb::SelectResponse & dag_response, BlocksList & blocks)
{
    auto codec = getCodec(dag_response.encode_type());
    for (const auto & chunk : dag_response.chunks())
        blocks.emplace_back(codec->decode(chunk.rows_data(), schema));
}

BlockInputStreamPtr outputDAGResponse(Context &, const DAGSchema & schema, const tipb::SelectResponse & dag_response)
{
    if (dag_response.has_error())
        throw Exception(dag_response.error().msg(), dag_response.error().code());

    BlocksList blocks;
    chunksToBlocks(schema, dag_response, blocks);
    return std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
}

DAGSchema getSelectSchema(Context & context)
{
    DAGSchema schema;
    auto * dag_context = context.getDAGContext();
    auto result_field_types = dag_context->result_field_types;
    for (int i = 0; i < static_cast<int>(result_field_types.size()); i++)
    {
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(result_field_types[i]);
        String col_name = "col_" + std::to_string(i);
        schema.push_back(std::make_pair(col_name, info));
    }
    return schema;
}

// Just for test usage, dag_response should not contain result more than 128M
Block getMergedBigBlockFromDagRsp(Context & context, const DAGSchema & schema, const tipb::SelectResponse & dag_response)
{
    auto src = outputDAGResponse(context, schema, dag_response);
    // Try to merge into big block. 128 MB should be enough.
    SquashingBlockInputStream squashed_delete_stream(src, 0, 128 * (1UL << 20), /*req_id=*/"");
    Blocks result_data;
    while (true)
    {
        Block block = squashed_delete_stream.read();
        if (!block)
        {
            if (result_data.empty())
            {
                // Ensure that result_data won't be empty in any situation
                result_data.emplace_back(std::move(block));
            }
            break;
        }
        else
        {
            result_data.emplace_back(std::move(block));
        }
    }

    if (result_data.size() > 1)
        throw Exception("Result block should be less than 128M!", ErrorCodes::BAD_ARGUMENTS);
    return result_data[0];
}

bool columnEqual(
    const ColumnPtr & expected,
    const ColumnPtr & actual,
    String & unequal_msg)
{
    for (size_t i = 0, size = expected->size(); i < size; ++i)
    {
        auto expected_field = (*expected)[i];
        auto actual_field = (*actual)[i];
        if (expected_field != actual_field)
        {
            unequal_msg = fmt::format("Value {} mismatch {} vs {} ", i, expected_field.toString(), actual_field.toString());
            return false;
        }
    }
    return true;
}

bool blockEqual(const Block & expected, const Block & actual, String & unequal_msg)
{
    size_t rows_a = expected.rows();
    size_t rows_b = actual.rows();
    if (rows_a != rows_b)
    {
        unequal_msg = fmt::format("Row counter are not equal: {} vs {} ", rows_a, rows_b);
        return false;
    }

    size_t size_a = expected.columns();
    size_t size_b = actual.columns();
    if (size_a != size_b)
    {
        unequal_msg = fmt::format("Columns size are not equal: {} vs {} ", size_a, size_b);
        return false;
    }

    for (size_t i = 0; i < size_a; i++)
    {
        bool equal = columnEqual(expected.getByPosition(i).column, actual.getByPosition(i).column, unequal_msg);
        if (!equal)
        {
            unequal_msg = fmt::format("{}th columns are not equal, details: {}", i, unequal_msg);
            return false;
        }
    }
    return true;
}

String formatBlockData(const Block & block)
{
    size_t rows = block.rows();
    size_t columns = block.columns();
    String result;
    for (size_t i = 0; i < rows; i++)
    {
        for (size_t j = 0; j < columns; j++)
        {
            auto column = block.getByPosition(j).column;
            auto field = (*column)[i];
            if (j + 1 < columns)
            {
                // Just use "," as separator now, maybe confusing when string column contains ","
                result += fmt::format("{},", field.toString());
            }
            else
            {
                result += fmt::format("{}\n", field.toString());
            }
        }
    }
    return result;
}

SortDescription generateSDFromSchema(const DAGSchema & schema)
{
    SortDescription sort_desc;
    sort_desc.reserve(schema.size());
    for (const auto & col : schema)
    {
        sort_desc.emplace_back(col.first, -1, -1, nullptr);
    }
    return sort_desc;
}

bool dagRspEqual(Context & context, const tipb::SelectResponse & expected, const tipb::SelectResponse & actual, String & unequal_msg)
{
    auto schema = getSelectSchema(context);
    SortDescription sort_desc = generateSDFromSchema(schema);
    Block block_a = getMergedBigBlockFromDagRsp(context, schema, expected);
    sortBlock(block_a, sort_desc);
    Block block_b = getMergedBigBlockFromDagRsp(context, schema, actual);
    sortBlock(block_b, sort_desc);
    bool equal = blockEqual(block_a, block_b, unequal_msg);
    if (!equal)
    {
        unequal_msg = fmt::format("{}\nExpected Results: \n{}\nActual Results: \n{}", unequal_msg, formatBlockData(block_a), formatBlockData(block_b));
    }
    return equal;
}
} // namespace DB
