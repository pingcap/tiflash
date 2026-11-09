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

#include <DataTypes/DataTypesNumber.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Interpreters/Join.h>
#include <Operators/HashJoinProbeTransformOp.h>
#include <Operators/HashProbeTransformExec.h>
#include <Operators/SharedQueue.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
constexpr auto * test_key_name = "k";

Block makeSampleBlock(const DataTypePtr & key_type)
{
    return Block{{key_type->createColumn(), key_type, test_key_name}};
}

JoinPtr makeTestJoin(const DataTypePtr & key_type, const ProbeStopContextPtr & probe_stop_context = nullptr)
{
    SpillConfig build_spill_config("/tmp", "join_probe_stop_build", 0, 0, 0, nullptr);
    SpillConfig probe_spill_config("/tmp", "join_probe_stop_probe", 0, 0, 0, nullptr);
    return std::make_shared<Join>(
        Names{test_key_name},
        Names{test_key_name},
        ASTTableJoin::Kind::Inner,
        "join_probe_stop_test",
        0,
        0,
        build_spill_config,
        probe_spill_config,
        RestoreConfig{1, 0, 0},
        NamesAndTypes{{test_key_name, key_type}},
        RegisterOperatorSpillContext{},
        nullptr,
        TiDB::TiDBCollators{},
        JoinNonEqualConditions{},
        1024,
        0,
        "",
        "",
        0,
        true,
        dummy_runtime_filter_list,
        probe_stop_context);
}

} // namespace

TEST(JoinProbeStopTest, CancelsAllRestoreProbeQueues)
{
    auto join = makeTestJoin(std::make_shared<DataTypeInt32>());
    auto registered_queue = SharedQueue::buildInternal(1, 1, -1, 1);
    join->addRestoreProbeQueue(registered_queue);

    join->stopProbePhase();

    Block block;
    ASSERT_EQ(registered_queue->tryPush(std::move(block)), MPMCQueueResult::CANCELLED);

    // Registration can race with stopProbePhase. A queue registered after the state transition must be cancelled too.
    auto late_registered_queue = SharedQueue::buildInternal(1, 1, -1, 1);
    join->addRestoreProbeQueue(late_registered_queue);
    ASSERT_EQ(late_registered_queue->tryPop(block), MPMCQueueResult::CANCELLED);
}

TEST(JoinProbeStopTest, PropagatesStopToRestoreJoin)
{
    auto probe_stop_context = std::make_shared<ProbeStopContext>();
    auto parent_join = makeTestJoin(std::make_shared<DataTypeInt32>(), probe_stop_context);
    auto restore_join = makeTestJoin(std::make_shared<DataTypeInt32>(), probe_stop_context);
    auto restore_probe_queue = SharedQueue::buildInternal(1, 1, -1, 1);
    restore_join->addRestoreProbeQueue(restore_probe_queue);

    parent_join->stopProbePhase();

    ASSERT_TRUE(parent_join->isProbeStopped());
    ASSERT_TRUE(restore_join->isProbeStopped());
    ASSERT_TRUE(restore_join->isProbeFinishedForPipeline());

    Block block;
    ASSERT_EQ(restore_probe_queue->tryPop(block), MPMCQueueResult::CANCELLED);
}

TEST(JoinProbeStopTest, StopsProbeOperatorBeforeReadingOrStartingRestore)
{
    auto key_type = std::make_shared<DataTypeInt32>();
    auto join = makeTestJoin(key_type);
    auto input_header = makeSampleBlock(key_type);
    join->initBuild(makeSampleBlock(key_type), 1);
    join->initProbe(input_header, 1);
    join->finalize(Names{test_key_name});

    PipelineExecutorContext exec_context;
    HashJoinProbeTransformOp probe_op(exec_context, "join_probe_stop_test", join, 0, 1024, input_header);
    HashProbeTransformExec restore_probe_exec("join_probe_stop_test", exec_context, 0, join, nullptr, 1024);

    join->stopProbePhase();
    ASSERT_FALSE(restore_probe_exec.startRestoreProbe());

    Block block;
    ASSERT_EQ(probe_op.tryOutput(block), OperatorStatus::HAS_OUTPUT);
    ASSERT_FALSE(block);

    block = input_header;
    ASSERT_EQ(probe_op.transform(block), OperatorStatus::HAS_OUTPUT);
    ASSERT_FALSE(block);
}

} // namespace DB::tests
