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

#include <Common/Stopwatch.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Operators/ConcatSourceOp.h>
#include <TestUtils/ColumnGenerator.h>
#include <gtest/gtest.h>

#include <memory>

namespace DB::tests
{
namespace
{
class MockSourceOp : public SourceOp
{
public:
    MockSourceOp(PipelineExecutorContext & exec_context_, const Block & output_)
        : SourceOp(exec_context_, "mock")
        , output(output_)
    {
        setHeader(output.cloneEmpty());
    }

    String getName() const override { return "MockSourceOp"; }

protected:
    OperatorStatus readImpl(Block & block) override
    {
        std::swap(block, output);
        return OperatorStatus::HAS_OUTPUT;
    }

private:
    Block output;
};

class MockSourceWithLatencyOp : public SourceOp
{
public:
    MockSourceWithLatencyOp(PipelineExecutorContext & exec_context_, const Blocks & output_)
        : SourceOp(exec_context_, "mock")
        , blks(output_)
        , cur_idx(0)
    {
        assert(!blks.empty());
        setHeader(blks[cur_idx].cloneEmpty());
    }

    String getName() const override { return "MockSourceOp"; }

protected:
    OperatorStatus readImpl(Block & block) override
    {
        if (cur_idx < blks.size())
        {
            auto ms = watch.elapsedMilliseconds();
            if (ms < 100)
            {
                LOG_INFO(Logger::get(), "MockSourceWithLatencyOp sleeping for {} ms", 100 - ms);
                return OperatorStatus::WAIT_FOR_NOTIFY;
            }
            LOG_INFO(Logger::get(), "MockSourceWithLatencyOp woke up after {} ms, return block at {}", ms, cur_idx);
            watch.restart();
            std::swap(block, blks[cur_idx++]);
            return OperatorStatus::HAS_OUTPUT;
        }
        // return HAS_OUTPUT with empty block to indicate end of stream
        return OperatorStatus::HAS_OUTPUT;
    }

private:
    Blocks blks;
    size_t cur_idx;
    Stopwatch watch;
};
} // namespace

class TestConcatSource : public ::testing::Test
{
};

TEST_F(TestConcatSource, setBlockSink)
{
    Block res;
    ASSERT_FALSE(res);

    PipelineExecutorContext exec_context;
    SetBlockSinkOp set_block_sink{exec_context, "test", res};
    Block header{ColumnGenerator::instance().generate({0, "Int32", DataDistribution::RANDOM})};
    set_block_sink.setHeader(header);
    Block block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})};
    set_block_sink.operatePrefix();
    set_block_sink.write(std::move(block));
    ASSERT_TRUE(res);
    set_block_sink.operateSuffix();
}

TEST_F(TestConcatSource, concatSink)
{
    size_t block_cnt = 10;
    std::vector<PipelineExecBuilder> builders;
    PipelineExecutorContext exec_context;
    for (size_t i = 0; i < block_cnt; ++i)
    {
        PipelineExecBuilder builder;
        Block block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})};
        builder.setSourceOp(std::make_unique<MockSourceOp>(exec_context, block));
        builders.push_back(std::move(builder));
    }

    ConcatSourceOp concat_source{exec_context, "test", builders};
    size_t actual_block_cnt = 0;
    concat_source.operatePrefix();
    while (true)
    {
        Block tmp;
        concat_source.read(tmp);
        if (tmp)
            ++actual_block_cnt;
        else
            break;
    }
    concat_source.operateSuffix();
    ASSERT_EQ(actual_block_cnt, block_cnt);
}

TEST_F(TestConcatSource, unorderedConcatSink)
{
    Blocks blks{
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
    };

    size_t num_builders = 10;
    std::vector<PipelineExecBuilder> builders;
    PipelineExecutorContext exec_context;
    for (size_t i = 0; i < num_builders; ++i)
    {
        PipelineExecBuilder builder;
        builder.setSourceOp(std::make_unique<MockSourceWithLatencyOp>(exec_context, blks));
        builders.push_back(std::move(builder));
    }

    UnorderedConcatSourceOp concat_source{exec_context, "test", builders};
    size_t actual_block_cnt = 0;
    concat_source.operatePrefix();
    while (true)
    {
        Block tmp;
        concat_source.read(tmp);
        if (tmp)
            ++actual_block_cnt;
        else
            break;
    }
    concat_source.operateSuffix();
    ASSERT_EQ(actual_block_cnt, num_builders);
}
} // namespace DB::tests
