#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <Poco/Base64Decoder.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/RegionMeta.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/KVStore.h>
#include <Debug/dbgTools.h>
#include <Debug/MockTiDB.h>
#include <Debug/MockTiKV.h>

#include <Flash/Coprocessor/RegionInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/CoprocessorHandler.h>

#include <tipb/select.pb.h>
#include <kvproto/coprocessor.pb.h>

namespace DB
{
class CopGenTester
{
public:
    struct RegionInfo
    {
        uint64_t id;
        uint64_t version;
        uint64_t conf_ver;
        TiKVKey start;
        TiKVKey end;
        std::vector<TiKVKeyValue> pairs;
    };
    
    struct TableData
    {
        uint64_t id;
        TiDB::TableInfo meta;
        std::vector<RegionInfo> regions;
    };

    static void decodeBase64(const std::string & str, std::string & out)
    {
        Poco::MemoryInputStream istr(str.data(), str.size());
        Poco::Base64Decoder decoder(istr);
        Poco::StreamCopier::copyToString(decoder, out);
    }

    static std::string printAsBytes(const std::string & str)
    {
        std::stringstream ss;
        for (auto c : str)
        {
            ss << std::hex << std::setw(2) << std::setfill('0') << (int)c;
        }
        return ss.str();
    }


    explicit CopGenTester(const std::string & case_path):context(DB::tests::TiFlashTestEnv::getContext(DB::Settings(), {"/tmp/copgentester"})) {
        std::ifstream file(case_path);
        if (!file.is_open())
        {
            throw std::runtime_error("Failed to open file: " + case_path);
        }

        auto json_str = std::string((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        Poco::JSON::Parser parser;
        std::cout << "parse begin" << std::endl;
        Poco::Dynamic::Var result = parser.parse(json_str);
        std::cout << "parse end" << std::endl;

        auto obj = result.extract<Poco::JSON::Object::Ptr>();
        auto toi_json = obj->getArray("table_of_interest");
        for (auto it = toi_json->begin(); it != toi_json->end(); ++it)
        {
            table_of_interest.push_back(it->extract<TableID>());
        }

        auto td_json = obj->getObject("table_data");
        for (auto & it : table_of_interest)
        {
            auto table = TableData();
            table.id = it;

            auto tbl_json = td_json->getObject(std::to_string(it));

            auto meta_json = tbl_json->getObject("meta");
            table.meta = TiDB::TableInfo(meta_json);

            auto regions_json = tbl_json->getArray("regions");
            for (auto & region_json : *regions_json)
            {
                auto region = RegionInfo();
                auto region_obj = region_json.extract<Poco::JSON::Object::Ptr>();
                region.id = region_obj->getValue<uint64_t>("id");
                region.version = region_obj->getValue<uint64_t>("version");
                region.conf_ver = region_obj->getValue<uint64_t>("conf_ver");

                std::string start, end;
                decodeBase64(region_obj->getValue<std::string>("start"), start);
                decodeBase64(region_obj->getValue<std::string>("end"), end);

                region.start = TiKVKey(std::move(start));
                std::cout << "region start " << printAsBytes(region.start.toString()) << std::endl;
                region.end = TiKVKey(std::move(end));
                std::cout << "region end " << printAsBytes(region.end.toString()) << std::endl;

                auto pairs_json = region_obj->getArray("pairs");
                for (auto & pair_json : *pairs_json) {
                    auto pair_obj = pair_json.extract<Poco::JSON::Object::Ptr>();
                    std::string key, value;
                    decodeBase64(pair_obj->getValue<std::string>("key"), key);
                    decodeBase64(pair_obj->getValue<std::string>("value"), value);

                    auto tikv_key = TiKVKey(std::move(key));
                    auto tikv_value = TiKVValue(std::move(value));
                    region.pairs.push_back(std::make_pair(std::move(tikv_key), std::move(tikv_value)));
                }
                table.regions.push_back(std::move(region));
            }
            table_data.emplace(it, std::move(table));
        }

        auto req_data_json = obj->getArray("request_data");
        for (auto & req_data_json_obj : *req_data_json)
        {
            auto req_data_obj = req_data_json_obj.extract<Poco::JSON::Object::Ptr>();
            auto req_type = req_data_obj->getValue<uint16_t>("type");
            auto request = req_data_obj->getValue<std::string>("request");
            auto response = req_data_obj->getValue<std::string>("response");
            coprocessor::Request cop_request;
            cop_request.ParseFromString(request);
            coprocessor::Response cop_response;
            cop_response.ParseFromString(response);
            request_data.emplace_back(std::make_pair(req_type, std::make_pair(std::move(cop_request), std::move(cop_response))));
        }
    }

    void prepare()
    {
        // prepare the table data
        for (auto & it : table_data)
        {
            std::cout << "prepare table " << it.first << std::endl;
            auto & table = it.second;
            auto meta = table.meta;
            auto & regions = table.regions;

            MockTiDB::instance().addTable("default", std::move(meta));

            TMTContext & tmt = context.getTMTContext();
            pingcap::pd::ClientPtr pd_client = tmt.getPDClient();

            for (auto & region : regions)
            {
                std::cout << "prepare region " << region.id << std::endl;
                metapb::Region region_pb;
                metapb::Peer peer;
                region_pb.set_id(region.id);

                std::cout << "region start " << printAsBytes(region.start.toString()) << " region end " << printAsBytes(region.end.toString()) << std::endl;
                region_pb.set_start_key(region.start.getStr());
                region_pb.set_end_key(region.end.getStr());

                RegionMeta region_meta(std::move(peer), std::move(region_pb), initialApplyState());
                std::cout << "region meta " << region_meta.toString() << std::endl;
                auto raft_index = RAFT_INIT_LOG_INDEX;
                region_meta.setApplied(raft_index, RAFT_INIT_LOG_TERM);
                RegionPtr region_ptr = std::make_shared<Region>(std::move(region_meta));
                tmt.getKVStore()->onSnapshot<RegionPtrWithBlock>(region_ptr, nullptr, 0, tmt);

                auto & pairs = region.pairs;
                for (auto & pair : pairs) {
                    auto & key = pair.first;
                    auto & value = pair.second;
                    auto key_str = key.getStr();
                    auto value_str = value.getStr();
                    UInt64 prewrite_ts = pd_client->getTS();
                    UInt64 commit_ts = pd_client->getTS();
                    raft_cmdpb::RaftCmdRequest request;
                    RegionBench::addRequestsToRaftCmd(request, std::move(key_str), std::move(value_str), prewrite_ts, commit_ts, false);
                    tmt.getKVStore()->handleWriteRaftCmd(std::move(request), region.id, MockTiKV::instance().getRaftIndex(region.id), MockTiKV::instance().getRaftTerm(region.id), tmt);
                }
            }
        }
    }

    void execute()
    {

        TMTContext & tmt = context.getTMTContext();
        pingcap::pd::ClientPtr pd_client = tmt.getPDClient();

        for (auto & it : request_data)
        {
            std::cout << "execute request " << it.first << std::endl;
            auto & req_type = it.first;
            auto & req_pair = it.second;
            auto & req = req_pair.first;
            auto & res = req_pair.second;

            kvrpcpb::Context req_context = req.context();
            tipb::DAGRequest dag_request = DB::getDAGRequestFromStringWithRetry(req.data());
            tipb::SelectResponse dag_response;
            RegionInfoMap regions;
            RegionInfoList retry_regions;

            regions.emplace(req_context.region_id(), DB::RegionInfo(req_context.region_id(), req_context.region_epoch().version(), req_context.region_epoch().conf_ver(), DB::CoprocessorHandler::GenCopKeyRange(req.ranges()), nullptr));
            DAGContext dag_context(dag_request);
            dag_context.regions_for_local_read = std::move(regions);
            context.setDAGContext(&dag_context);
            DAGDriver driver(context, pd_client->getTS(), DEFAULT_UNSPECIFIED_SCHEMA_VERSION, &dag_response);
            std::cout << res.SerializeAsString() << "\n" << dag_response.SerializeAsString() << std::endl;
        }
    }
    
    std::unordered_map<TableID, TableData> table_data;
    std::vector<TableID> table_of_interest;
    std::vector<std::pair<uint16_t, std::pair<coprocessor::Request, coprocessor::Response>>> request_data;
    RegionInfoMap regions;
    Context context;
};
namespace tests
{


TEST(Copgen, Test)
try
{
    ASSERT_EQ(1 + 1, 2);
    auto t = CopGenTester("/tmp/copgen_test_data.json");
    t.prepare();
    t.execute();
    ASSERT_EQ(t.table_data.size(), t.table_of_interest.size());
    
}
CATCH
}
}
