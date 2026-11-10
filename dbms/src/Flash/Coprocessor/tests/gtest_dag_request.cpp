// Copyright 2026 PingCAP, Inc.
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
#include <Flash/Coprocessor/DAGRequest.h>
#include <gtest/gtest.h>

#include <vector>

namespace DB::tests
{
namespace
{
tipb::DAGRequest makeListRequest(const std::vector<tipb::ExecType> & types, bool withIds)
{
    tipb::DAGRequest request;
    for (size_t i = 0; i < types.size(); ++i)
    {
        auto * executor = request.add_executors();
        executor->set_tp(types[i]);
        if (withIds)
            executor->set_executor_id(fmt::format("executor_{}", i));
    }
    return request;
}
} // namespace

TEST(DAGRequestTest, RejectsUnbalancedExecutorLists)
{
    const std::vector<std::vector<tipb::ExecType>> invalidLists{
        {},
        {tipb::TypeLimit},
        {tipb::TypeTableScan, tipb::TypeTableScan},
        {tipb::TypeTableScan, tipb::TypeLimit, tipb::TypeTableScan, tipb::TypeLimit},
        {tipb::TypeLimit, tipb::TypeTableScan},
        {tipb::TypeExchangeReceiver, tipb::TypeTableScan, tipb::TypeProjection},
    };
    for (bool withIds : {false, true})
    {
        for (const auto & types : invalidLists)
        {
            auto request = makeListRequest(types, withIds);
            SCOPED_TRACE(request.ShortDebugString());
            try
            {
                (void)DAGRequest{&request};
                ADD_FAILURE() << "Expected an unbalanced executor list to be rejected";
            }
            catch (const TiFlashException & e)
            {
                EXPECT_TRUE(e.getError().is(Errors::Coprocessor::BadRequest)) << e.standardText();
            }
        }
    }
}

TEST(DAGRequestTest, AcceptsBalancedExecutorLists)
{
    const std::vector<std::vector<tipb::ExecType>> validLists{
        {tipb::TypeTableScan},
        {tipb::TypePartitionTableScan, tipb::TypeSelection, tipb::TypeLimit},
        {tipb::TypeExchangeReceiver,
         tipb::TypeProjection,
         tipb::TypeAggregation,
         tipb::TypeTopN,
         tipb::TypeExchangeSender},
    };
    for (bool withIds : {false, true})
    {
        for (const auto & types : validLists)
        {
            auto request = makeListRequest(types, withIds);
            SCOPED_TRACE(request.ShortDebugString());
            EXPECT_NO_THROW((void)DAGRequest{&request});
        }
    }
}

TEST(DAGRequestTest, RejectsJoinInExecutorLists)
{
    const std::vector<std::vector<tipb::ExecType>> joinLists{
        {tipb::TypeJoin},
        {tipb::TypeTableScan, tipb::TypeJoin},
        {tipb::TypeJoin, tipb::TypeTableScan, tipb::TypeTableScan},
        {tipb::TypeTableScan, tipb::TypeTableScan, tipb::TypeJoin},
        {tipb::TypeTableScan, tipb::TypeTableScan, tipb::TypeJoin, tipb::TypeJoin},
        {tipb::TypeTableScan, tipb::TypeTableScan, tipb::TypeTableScan, tipb::TypeJoin},
        {tipb::TypeTableScan,
         tipb::TypeTableScan,
         tipb::TypeJoin,
         tipb::TypeTableScan,
         tipb::TypeJoin,
         tipb::TypeLimit},
    };
    for (bool withIds : {false, true})
    {
        for (bool withChildren : {false, true})
        {
            for (const auto & types : joinLists)
            {
                auto request = makeListRequest(types, withIds);
                if (withChildren)
                {
                    for (auto & executor : *request.mutable_executors())
                    {
                        if (executor.tp() == tipb::TypeJoin)
                        {
                            executor.mutable_join()->add_children()->set_tp(tipb::TypeTableScan);
                            executor.mutable_join()->add_children()->set_tp(tipb::TypeTableScan);
                        }
                    }
                }
                SCOPED_TRACE(request.ShortDebugString());
                try
                {
                    (void)DAGRequest{&request};
                    ADD_FAILURE() << "Expected a list-based Join to be rejected";
                }
                catch (const TiFlashException & e)
                {
                    EXPECT_TRUE(e.getError().is(Errors::Coprocessor::BadRequest)) << e.standardText();
                }
            }
        }
    }
}

TEST(DAGRequestTest, AcceptsTreeBasedJoin)
{
    tipb::DAGRequest request;
    auto * root = request.mutable_root_executor();
    root->set_tp(tipb::TypeJoin);
    root->set_executor_id("join");
    for (const auto * id : {"left", "right"})
    {
        auto * child = root->mutable_join()->add_children();
        child->set_tp(tipb::TypeTableScan);
        child->set_executor_id(id);
    }
    EXPECT_NO_THROW((void)DAGRequest{&request});
}
} // namespace DB::tests
