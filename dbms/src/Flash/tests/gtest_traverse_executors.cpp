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
#include <Flash/Coprocessor/collectOutputFieldTypes.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
template <typename F>
void expectBadRequest(F && action)
{
    try
    {
        action();
        FAIL() << "Expected malformed Join children to be rejected";
    }
    catch (const TiFlashException & e)
    {
        EXPECT_TRUE(e.getError().is(Errors::Coprocessor::BadRequest)) << e.standardText();
    }
}
} // namespace

TEST(TraverseExecutorsTest, RejectsMissingJoin)
{
    tipb::Executor executor;
    executor.set_tp(tipb::TypeJoin);
    ASSERT_FALSE(executor.has_join());
    expectBadRequest([&] { getChildren(executor); });
}

TEST(TraverseExecutorsTest, RejectsInvalidJoinChildren)
{
    for (int child_count : {0, 1, 3})
    {
        SCOPED_TRACE(child_count);
        tipb::DAGRequest request;
        auto * root = request.mutable_root_executor();
        root->set_tp(tipb::TypeJoin);
        auto * join = root->mutable_join();
        join->set_join_type(tipb::TypeInnerJoin);
        for (int i = 0; i < child_count; ++i)
            join->add_children()->set_tp(tipb::TypeTableScan);

        expectBadRequest([&] { getChildren(*root); });
        expectBadRequest([&] { traverseExecutors(&request, [](const tipb::Executor &) { return true; }); });
        expectBadRequest([&] { traverseExecutorsReverse(&request, [](const tipb::Executor &) {}); });
        expectBadRequest([&] { collectOutputFieldTypes(request); });
    }
}

TEST(TraverseExecutorsTest, TraversesValidJoin)
{
    tipb::DAGRequest request;
    auto * root = request.mutable_root_executor();
    root->set_tp(tipb::TypeJoin);
    root->set_executor_id("join");
    auto * join = root->mutable_join();
    join->set_join_type(tipb::TypeInnerJoin);
    auto * left = join->add_children();
    left->set_tp(tipb::TypeTableScan);
    left->set_executor_id("left");
    left->mutable_tbl_scan()->add_columns()->set_tp(TiDB::TypeLongLong);
    auto * right = join->add_children();
    right->set_tp(tipb::TypeTableScan);
    right->set_executor_id("right");
    right->mutable_tbl_scan()->add_columns()->set_tp(TiDB::TypeString);

    std::vector<String> preorder;
    traverseExecutors(&request, [&](const tipb::Executor & executor) {
        preorder.push_back(executor.executor_id());
        return true;
    });
    EXPECT_EQ(preorder, (std::vector<String>{"join", "left", "right"}));

    std::vector<String> postorder;
    traverseExecutorsReverse(&request, [&](const tipb::Executor & executor) {
        postorder.push_back(executor.executor_id());
    });
    EXPECT_EQ(postorder, (std::vector<String>{"left", "right", "join"}));

    const auto field_types = collectOutputFieldTypes(request);
    ASSERT_EQ(field_types.size(), 2);
    EXPECT_EQ(field_types[0].tp(), TiDB::TypeLongLong);
    EXPECT_EQ(field_types[1].tp(), TiDB::TypeString);
}
} // namespace DB::tests
