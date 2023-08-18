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

#include <Core/TiFlashDisaggregatedMode.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>
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
        db_name = "test_db";
        table_name = "test_table";
        ExecutorTest::initializeContext();
        context.addMockTable({db_name, table_name}, {{"s1", TiDB::TP::TypeString}, {"s2", TiDB::TP::TypeString}});
    }

    String db_name;
    String table_name;
};

TEST_F(StorageDisaggregatedTest, BasicTest)
try
{
    ::mpp::DispatchTaskRequest dispatch_req;
    auto dag_req = context.scan(db_name, table_name).aggregation({Count(col("s1"))}, {}).exchangeSender(tipb::PassThrough).build(context);
    const auto & sender = dag_req->root_executor();
    ASSERT_EQ(sender.tp(), ::tipb::TypeExchangeSender);
    const auto & hash_agg = sender.exchange_sender().child();
    ASSERT_EQ(hash_agg.tp(), ::tipb::TypeAggregation);
    const auto & table_scan = hash_agg.aggregation().child();
    ASSERT_EQ(table_scan.tp(), ::tipb::TypeTableScan);

    // Mock batch_cop_task.
    ::pingcap::coprocessor::RegionInfo mock_region_info;
    mock_region_info.region_id = pingcap::kv::RegionVerID{100, 1, 1};
    ::pingcap::coprocessor::BatchCopTask mock_batch_cop_task;
    mock_batch_cop_task.store_addr = "127.0.0.1:9000";
    mock_batch_cop_task.store_id = 1;
    mock_batch_cop_task.region_infos = std::vector<::pingcap::coprocessor::RegionInfo>{mock_region_info};

    // Mock DispatchTaskRequest.Meta.
    auto meta = ::mpp::TaskMeta();
    meta.set_start_ts(437520160532463617);
    meta.set_task_id(1);
    meta.set_address("127.0.0.1:3930");

    // Setup dag_context, becase StorageDisaggregated will use it to get ::mpp::TaskMeta.
    auto dag_context = std::make_shared<DAGContext>(*dag_req, meta, true);
    auto * ori_dag_context = TiFlashTestEnv::getGlobalContext().getDAGContext();
    TiFlashTestEnv::getGlobalContext().setDAGContext(dag_context.get());
    TiDBTableScan tidb_table_scan(&table_scan, table_scan.executor_id(), *dag_context);

    FilterConditions filter_conditions;
    StorageDisaggregated storage(TiFlashTestEnv::getGlobalContext(), tidb_table_scan, filter_conditions);

    uint64_t store_id;
    std::vector<pingcap::kv::RegionVerID> region_ids;
    std::shared_ptr<::mpp::DispatchTaskRequest> tiflash_storage_dispatch_req;
    std::tie(tiflash_storage_dispatch_req, region_ids, store_id) = storage.buildDispatchMPPTaskRequest(mock_batch_cop_task);
    ASSERT_EQ(region_ids.size(), 1);
    ASSERT_EQ(region_ids[0].id, 100);
    ASSERT_EQ(store_id, 1);

    // Check if field number of DispatchTaskRequest and DAGRequest is correct.
    // In case we add/remove filed but forget to update build processing of StorageDisaggregated.
    const auto * dispatch_req_desc = tiflash_storage_dispatch_req->GetDescriptor();
    ASSERT_EQ(dispatch_req_desc->field_count(), 6);

    ::tipb::DAGRequest sender_dag_req;
    sender_dag_req.ParseFromString(tiflash_storage_dispatch_req->encoded_plan());
    const auto * sender_dag_req_desc = sender_dag_req.GetDescriptor();
    ASSERT_EQ(sender_dag_req_desc->field_count(), 17);

    const auto & sender1 = sender_dag_req.root_executor();
    ASSERT_EQ(sender1.tp(), ::tipb::TypeExchangeSender);
    const auto & table_scan1 = sender1.exchange_sender().child();
    ASSERT_EQ(table_scan1.tp(), ::tipb::TypeTableScan);

    TiFlashTestEnv::getGlobalContext().setDAGContext(ori_dag_context);
}
CATCH

TEST_F(StorageDisaggregatedTest, LabelTest)
try
{
    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::Compute), "tiflash_compute");
    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::Compute), DISAGGREGATED_MODE_COMPUTE_PROXY_LABEL);

    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::Storage), "tiflash");
    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::Storage), DEF_PROXY_LABEL);

    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::None), "tiflash");
    ASSERT_EQ(getProxyLabelByDisaggregatedMode(DisaggregatedMode::None), DEF_PROXY_LABEL);
}
CATCH

} // namespace tests
} // namespace DB
