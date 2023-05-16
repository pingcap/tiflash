// Copyright 2023 PingCAP, Ltd.
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

#include <Operators/ConcatSourceOp.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
enum class status
{
    
};

class TestSourceOp : public SourceOp
{
public:
    TestSourceOp(
        PipelineExecutorStatus & exec_status_,
        const String & req_id)
        : SourceOp(exec_status_, req_id)
    {
    }

    String getName() const override
    {
        return "Test";
    }

protected:
    OperatorStatus readImpl(Block & block) override
    {

    }

    OperatorStatus awaitImpl() override
    {

    }

    OperatorStatus executeIOImpl() override
    {

    }
};
}

TEST_F(ConcatSourceOpTest, pool)
{
    {
        MultiPartitionSourcePool source_pool{2};
        ASSERT_TRUE(source_pool.gen().empty());
    }
    {
        MultiPartitionSourcePool source_pool{2};
        PipelineExecutorStatus exec_status;
        source_pool.add(std::make_unique<TestSourceOp>(exec_status, ""));
        ASSERT_TRUE(source_pool.gen().size() == 1);
        ASSERT_TRUE(source_pool.gen().empty());
    }
    {
        MultiPartitionSourcePool source_pool{2};
        PipelineExecutorStatus exec_status;
        source_pool.add(std::make_unique<TestSourceOp>(exec_status, ""));
        source_pool.add(std::make_unique<TestSourceOp>(exec_status, ""));
        ASSERT_TRUE(source_pool.gen().size() == 1);
        ASSERT_TRUE(source_pool.gen().size() == 1);
        ASSERT_TRUE(source_pool.gen().empty());
    }
    {
        MultiPartitionSourcePool source_pool{2};
        PipelineExecutorStatus exec_status;
        source_pool.add(std::make_unique<TestSourceOp>(exec_status, ""));
        source_pool.add(std::make_unique<TestSourceOp>(exec_status, ""));
        source_pool.add(std::make_unique<TestSourceOp>(exec_status, ""));
        ASSERT_TRUE(source_pool.gen().size() == 2);
        ASSERT_TRUE(source_pool.gen().size() == 1);
        ASSERT_TRUE(source_pool.gen().empty());
    }
}

TEST_F(ConcatSourceOpTest, run)
{
    MultiPartitionSourcePool source_pool{1};
    source_pool.add(std::make_unique<TestSourceOp>(exec_status, ""));
    source_pool.add(std::make_unique<TestSourceOp>(exec_status, ""));
    source_pool.add(std::make_unique<TestSourceOp>(exec_status, ""));

    
}

} // namespace DB::tests
