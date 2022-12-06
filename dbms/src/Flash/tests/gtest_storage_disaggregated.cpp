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

#include <Storages/StorageDisaggregated.h>
#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{

class StorageDisaggregatedTest : public DB::tests::ExecutorTest
{
public:
    void initializeContext() override
    {
        ExecutorTest::initializeContext();
    }
};

TEST_F(StorageDisaggregatedTest, BasicTest)
try
{
    ::mpp::DispatchTaskRequest dispatch_req;
    // +------------------------------+----------+--------------+---------------+---------------------------------+
    // | id                           | estRows  | task         | access object | operator info                   |
    // +------------------------------+----------+--------------+---------------+---------------------------------+
    // | HashAgg_21                   | 1.00     | root         |               | funcs:count(Column#6)->Column#4 |
    // | └─TableReader_23             | 1.00     | root         |               | data:ExchangeSender_22          |
    // |   └─ExchangeSender_22        | 1.00     | mpp[tiflash] |               | ExchangeType: PassThrough       |
    // |     └─HashAgg_9              | 1.00     | mpp[tiflash] |               | funcs:count(1)->Column#6        |
    // |       └─TableFullScan_20     | 10000.00 | mpp[tiflash] | table:t1      | keep order:false, stats:pseudo  |
    // +------------------------------+----------+--------------+---------------+---------------------------------+
    const std::string json_str = R"({"meta":{"start_ts":437520160532463617,"task_id":1,"address":"127.0.0.1:3930"},"encoded_plan":"GIDhASDiASgAQAFaDUFzaWEvU2hhbmdoYWlgAXICCACKAZUCCAlSEUV4Y2hhbmdlU2VuZGVyXzE5YvYBCAASFQiBgPC0yciYiQYQ////////////ASK9AQgDKqcBElcIuRcaLggBEgiAAAAAAAAAASAAKhwICBCBARgBIAAowf//////////ATIGYmluYXJ5OAAgACocCAgQgQEYFSAAKMH//////////wEyBmJpbmFyeTgASAIYADJKCAASLgi3ARIhCP///////////wEQCBjB//////////8BIBQoADADqAEBGAAoAEAASABSEFRhYmxlRnVsbFNjYW5fMTeIAQCQAQBSCUhhc2hBZ2dfOYgBAJABADIbCAgQABgVIAAowf//////////ATIGYmluYXJ5iAEAkAGAQA==","timeout":60,"regions":[{"region_id":7009,"region_epoch":{"conf_ver":6,"version":79},"ranges":[{"start":"dIAAAAAAAAC3X3IAAAAAAAAAAA==","end":"dIAAAAAAAAC3X3L//////////wA="}]}],"schema_ver":131})";

    auto status = google::protobuf::util::JsonStringToMessage(json_str, &dispatch_req);

    ASSERT_TRUE(status.ok());

    // Check if dag_req is correct(Sender -> HashAgg -> TableScan).
    ::tipb::DAGRequest dag_req;
    dag_req.ParseFromString(dispatch_req.encoded_plan());

    const auto & sender = dag_req.root_executor();
    ASSERT_EQ(sender.tp(), ::tipb::TypeExchangeSender);

    const auto & hash_agg = sender.exchange_sender().child();
    ASSERT_EQ(hash_agg.tp(), ::tipb::TypeAggregation);

    const auto & table_scan = hash_agg.aggregation().child();
    ASSERT_EQ(table_scan.tp(), ::tipb::TypeTableScan);

    auto dag_context = std::make_shared<DAGContext>(dag_req, dispatch_req.meta(), true);
    TiFlashTestEnv::getGlobalContext().setDAGContext(dag_context.get());
    TiDBTableScan tidb_table_scan(&table_scan, table_scan.executor_id(), *dag_context);
    // It's ok to be empty, because buildDispatchMPPTaskRequest() doesn't use it.
    auto remote_requests = std::vector<RemoteRequest>{RemoteRequest(::tipb::DAGRequest(), DAGSchema(), std::vector<pingcap::coprocessor::KeyRange>(), 0)};
    StorageDisaggregated storage(
        TiFlashTestEnv::getGlobalContext(),
        tidb_table_scan,
        remote_requests);

    ::pingcap::coprocessor::RegionInfo mock_region_info;
    ::pingcap::coprocessor::BatchCopTask mock_batch_cop_task;
    mock_batch_cop_task.store_addr = "127.0.0.1:9000";
    mock_batch_cop_task.region_infos = std::vector<::pingcap::coprocessor::RegionInfo>{mock_region_info};
    std::shared_ptr<::mpp::DispatchTaskRequest> tiflash_storage_dispatch_req;
    std::tie(tiflash_storage_dispatch_req, std::ignore, std::ignore) = storage.buildDispatchMPPTaskRequest(mock_batch_cop_task);

    // Check if field number of DispatchTaskRequest and DAGRequest is correct.
    // In case we add/remove filed but forget to update build processing of StorageDisaggregated.
    const auto * dispatch_req_desc = tiflash_storage_dispatch_req->GetDescriptor();
    ASSERT_EQ(dispatch_req_desc->field_count(), 6);

    ::tipb::DAGRequest sender_dag_req;
    sender_dag_req.ParseFromString(tiflash_storage_dispatch_req->encoded_plan());
    const auto * sender_dag_req_desc = sender_dag_req.GetDescriptor();
    ASSERT_EQ(sender_dag_req_desc->field_count(), 17);

    TiFlashTestEnv::getGlobalContext().setDAGContext(nullptr);
}
CATCH

} // namespace tests
} // namespace DB
