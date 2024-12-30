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

#include <Debug/MockKVStore/MockTiKV.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessor.h>
#include <Debug/dbgNaturalDag.h>
#include <Debug/dbgTools.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/CoprocessorHandler.h>
#include <Interpreters/Context.h>
#include <Poco/Base64Decoder.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Parser.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <TiDB/Schema/TiDBSchemaManager.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

static const String CONTINUE_WHEN_ERROR = "continue_when_error";
static const String TABLE_IDS = "table_of_interest";
static const String TABLE_DATA = "table_data";
static const String TABLE_META = "meta";
static const String TABLE_REGIONS = "regions";
static const String REGION_ID = "id";
static const String REGION_START = "start";
static const String REGION_END = "end";
static const String REGION_KEYVALUE_DATA = "pairs";
static const String TIKV_KEY = "key";
static const String TIKV_VALUE = "value";
static const String REQ_RSP_DATA = "request_data";
static const String REQ_TYPE = "type";
static const String REQ_ID = "req_id";
static const String REQUEST = "request";
static const String RESPONSE = "response";
static const String DEFAULT_DATABASE_NAME = "test";
static const uint64_t DEFAULT_REGION_VERSION = 0;
static const uint64_t DEFAULT_REGION_CONF_VERSION = 0;
/// Since request types are very limited and only useful for natural dag building, so just hard code these type here
static const uint16_t COP_REQUEST_TYPE = 545;
//static const uint16_t BATCH_COP_REQUEST_TYPE = 547;
//static const uint16_t MPP_REQUEST_TYPE = 548;


void decodeBase64(const String & str, String & out)
{
    Poco::MemoryInputStream input_stream(str.data(), str.size());
    Poco::Base64Decoder decoder(input_stream);
    Poco::StreamCopier::copyToString(decoder, out);
}

String printAsBytes(const String & str)
{
    std::stringstream ss;
    for (auto c : str)
    {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
    }
    return ss.str();
}

void NaturalDag::init()
{
    std::ifstream file(json_dag_path);
    if (!file.is_open())
        throw Exception("Failed to open file: " + json_dag_path, ErrorCodes::BAD_ARGUMENTS);

    auto json_str = String((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(json_str);
    LOG_INFO(log, "Succeed parsing json file: {}", json_dag_path);

    const auto & obj = result.extract<JSONObjectPtr>();

    if (obj->has(CONTINUE_WHEN_ERROR))
    {
        continue_when_error = obj->getValue<bool>(CONTINUE_WHEN_ERROR);
        LOG_INFO(log, "Succeed load continue_when_error flag: {}!", continue_when_error);
    }
    loadTables(obj);
    LOG_INFO(log, "Succeed loading table data!");
    loadReqAndRsp(obj);
    LOG_INFO(log, "Succeed loading req and rsp data!");
}

void NaturalDag::loadReqAndRsp(const NaturalDag::JSONObjectPtr & obj)
{
    auto req_data_json = obj->getArray(REQ_RSP_DATA);
    int32_t default_req_id = 0;
    for (const auto & req_data_json_obj : *req_data_json)
    {
        auto req_data_obj = req_data_json_obj.extract<JSONObjectPtr>();
        auto req_type = req_data_obj->getValue<uint16_t>(REQ_TYPE);
        if (req_type != COP_REQUEST_TYPE)
            throw Exception(
                fmt::format(
                    "Currently only support cop_request({}), while receive type({})!",
                    COP_REQUEST_TYPE,
                    req_type),
                ErrorCodes::BAD_ARGUMENTS);
        String request;
        decodeBase64(req_data_obj->getValue<String>(REQUEST), request);
        coprocessor::Request cop_request;
        if (!cop_request.ParseFromString(request))
            throw Exception("Incorrect request data!", ErrorCodes::BAD_ARGUMENTS);

        String response;
        decodeBase64(req_data_obj->getValue<String>(RESPONSE), response);
        coprocessor::Response cop_response;
        if (!cop_response.ParseFromString(response))
            throw Exception("Incorrect response data!", ErrorCodes::BAD_ARGUMENTS);
        req_rsp.emplace_back(std::make_pair(std::move(cop_request), std::move(cop_response)));

        if (req_data_obj->has(REQ_ID))
            req_id_vec.push_back(req_data_obj->getValue<int32_t>(REQ_ID));
        else
            req_id_vec.push_back(default_req_id);
        ++default_req_id;
    }
}
void NaturalDag::loadTables(const NaturalDag::JSONObjectPtr & obj)
{
    auto toi_json = obj->getArray(TABLE_IDS);
    for (const auto & it : *toi_json)
    {
        table_ids.push_back(it.extract<TableID>());
    }

    auto td_json = obj->getObject(TABLE_DATA);
    for (auto id : table_ids)
    {
        auto table = LoadedTableData();
        table.id = id;
        auto tbl_json = td_json->getObject(std::to_string(id));
        auto meta_json = tbl_json->getObject(TABLE_META);
        table.meta = TiDB::TableInfo(meta_json, NullspaceID);
        auto regions_json = tbl_json->getArray(TABLE_REGIONS);
        for (const auto & region_json : *regions_json)
        {
            auto region = loadRegion(region_json);
            table.regions.push_back(std::move(region));
        }
        tables.emplace(id, std::move(table));
    }
}

NaturalDag::LoadedRegionInfo NaturalDag::loadRegion(const Poco::Dynamic::Var & region_json) const
{
    auto region = LoadedRegionInfo();
    auto region_obj = region_json.extract<JSONObjectPtr>();
    region.id = region_obj->getValue<uint64_t>(REGION_ID);
    region.version = DEFAULT_REGION_VERSION;
    region.conf_ver = DEFAULT_REGION_CONF_VERSION;
    String region_start;
    decodeBase64(region_obj->getValue<String>(REGION_START), region_start);
    region.start = RecordKVFormat::encodeAsTiKVKey(region_start);

    String region_end;
    decodeBase64(region_obj->getValue<String>(REGION_END), region_end);
    region.end = RecordKVFormat::encodeAsTiKVKey(region_end);
    LOG_INFO(
        log,
        "RegionID: {}, RegionStart: {}, RegionEnd: {}",
        region.id,
        printAsBytes(region.start),
        printAsBytes(region.end));

    auto pairs_json = region_obj->getArray(REGION_KEYVALUE_DATA);
    for (const auto & pair_json : *pairs_json)
    {
        auto pair_obj = pair_json.extract<JSONObjectPtr>();
        String key;
        decodeBase64(pair_obj->getValue<String>(TIKV_KEY), key);
        TiKVKey tikv_key = RecordKVFormat::encodeAsTiKVKey(key); // encode raw key
        String value;
        decodeBase64(pair_obj->getValue<String>(TIKV_VALUE), value);
        TiKVValue tikv_value(std::move(value)); // use value directly, no encoding needed
        region.pairs.push_back(std::make_pair(std::move(tikv_key), std::move(tikv_value)));
    }
    return region;
}

const String & NaturalDag::getDatabaseName()
{
    return DEFAULT_DATABASE_NAME;
}

void NaturalDag::buildTables(Context & context)
{
    using ClientPtr = pingcap::pd::ClientPtr;
    TMTContext & tmt = context.getTMTContext();
    ClientPtr pd_client = tmt.getPDClient();
    auto schema_syncer = tmt.getSchemaSyncerManager();

    String db_name(getDatabaseName());
    buildDatabase(context, schema_syncer, db_name);

    for (auto & it : tables)
    {
        auto & table = it.second;
        auto meta = table.meta;
        MockTiDB::instance().addTable(db_name, std::move(meta));
        schema_syncer->syncSchemas(context, NullspaceID);
        for (auto & region : table.regions)
        {
            metapb::Region region_pb;
            metapb::Peer peer;
            region_pb.set_id(region.id);
            region_pb.set_start_key(region.start.getStr());
            region_pb.set_end_key(region.end.getStr());
            RegionMeta region_meta(std::move(peer), std::move(region_pb), initialApplyState());
            auto raft_index = RAFT_INIT_LOG_INDEX;
            region_meta.setApplied(raft_index, RAFT_INIT_LOG_TERM);
            RegionPtr region_ptr = RegionBench::makeRegion(std::move(region_meta));
            tmt.getKVStore()->onSnapshot<RegionPtrWithSnapshotFiles>(region_ptr, nullptr, 0, tmt);

            auto & pairs = region.pairs;
            for (auto & pair : pairs)
            {
                UInt64 prewrite_ts = pd_client->getTS();
                UInt64 commit_ts = pd_client->getTS();
                raft_cmdpb::RaftCmdRequest request;
                RegionBench::addRequestsToRaftCmd(request, pair.first, pair.second, prewrite_ts, commit_ts, false);
                RegionBench::applyWriteRaftCmd(
                    *tmt.getKVStore(),
                    std::move(request),
                    region.id,
                    MockTiKV::instance().getRaftIndex(region.id),
                    MockTiKV::instance().getRaftTerm(region.id),
                    tmt);
            }
        }
    }
}

void NaturalDag::buildDatabase(
    Context & context,
    std::shared_ptr<TiDBSchemaSyncerManager> & schema_syncer,
    const String & db_name)
{
    auto result = MockTiDB::instance().getDBIDByName(db_name);
    if (result.first)
    {
        MockTiDB::instance().dropDB(context, db_name, true);
    }
    MockTiDB::instance().newDataBase(db_name);
    schema_syncer->syncSchemas(context, NullspaceID);
}

void NaturalDag::build(Context & context)
{
    buildTables(context);
}

void NaturalDag::clean(Context & context)
{
    String db_name(getDatabaseName());
    MockTiDB::instance().dropDB(context, db_name, true);
}

} // namespace DB
