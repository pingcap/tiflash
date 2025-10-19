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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Operators/ConcatSourceOp.h>
#include <Storages/DeltaMerge/ReadThread/WorkQueue.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>
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

namespace
{
class SyncBlocks
{
public:
    explicit SyncBlocks(Blocks blks_)
        : blocks(std::move(blks_))
        , current(blocks.begin())
    {}

    Block getNext()
    {
        std::lock_guard lock(mtx);
        if (current == blocks.end())
            return Block{};
        Block res = *current;
        ++current;
        return res;
    }

private:
    std::mutex mtx;
    Blocks blocks;
    Blocks::iterator current;
};

class MockSourceOpFromQueue : public SourceOp
{
public:
    MockSourceOpFromQueue(
        PipelineExecutorContext & exec_context_,
        const std::shared_ptr<SyncBlocks> & sync_blocks_,
        const Block & header)
        : SourceOp(exec_context_, "mock")
        , sync_blocks(sync_blocks_)
    {
        setHeader(header);
    }

    String getName() const override { return "MockSourceOpFromQueue"; }

protected:
    OperatorStatus readImpl(Block & block) override
    {
        block = sync_blocks->getNext();
        return OperatorStatus::HAS_OUTPUT;
    }

private:
    std::shared_ptr<SyncBlocks> sync_blocks;
};
class SimpleGetResultSinkOp : public SinkOp
{
public:
    SimpleGetResultSinkOp(PipelineExecutorContext & exec_context_, const String & req_id, ResultHandler result_handler_)
        : SinkOp(exec_context_, req_id)
        , result_handler(std::move(result_handler_))
    {
        assert(result_handler);
    }

    String getName() const override { return "SimpleGetResultSinkOp"; }

protected:
    OperatorStatus writeImpl(Block && block) override
    {
        if (!block)
            return OperatorStatus::FINISHED;

        result_handler(block);
        return OperatorStatus::NEED_INPUT;
    }

private:
    ResultHandler result_handler;
};

class MockReadTaskPool;
using MockReadTaskPoolPtr = std::shared_ptr<MockReadTaskPool>;

class MockReadTaskPool : public NotifyFuture
{
public:
    explicit MockReadTaskPool(size_t idx_, size_t slot_limit)
        : idx(idx_)
        , q(slot_limit)
    {}

    void pushBlock(Block && block) { q.push(std::move(block), nullptr); }
    void finish() { q.finish(); }

    bool tryPopBlock(Block & block) { return q.tryPop(block); }

    void registerTask(TaskPtr && task) override
    {
        q.registerPipeTask(std::move(task), NotifyType::WAIT_ON_TABLE_SCAN_READ);
    }

    const size_t idx;

private:
    DB::DM::WorkQueue<Block> q;
};

class MockSourceOnReadTaskPoolOp : public SourceOp
{
public:
    MockSourceOnReadTaskPoolOp(PipelineExecutorContext & exec_context_, Block header_, const MockReadTaskPoolPtr & pool)
        : SourceOp(exec_context_, "mock")
        , done(false)
        , task_pool(pool)
    {
        setHeader(header_);
    }

    String getName() const override { return "MockSourceOp"; }

protected:
    OperatorStatus readImpl(Block & block) override
    {
        if (done)
            return OperatorStatus::HAS_OUTPUT;

        while (true)
        {
            if (!task_pool->tryPopBlock(block))
            {
                setNotifyFuture(task_pool.get());
                return OperatorStatus::WAIT_FOR_NOTIFY;
            }

            if (block)
            {
                if (block.rows() == 0)
                {
                    block.clear();
                    continue;
                }
                LOG_INFO(Logger::get(), "A block is popped from pool_idx={}", task_pool->idx);
                return OperatorStatus::HAS_OUTPUT;
            }
            else
            {
                done = true;
                LOG_INFO(Logger::get(), "No more blocks in pool_idx={}", task_pool->idx);
                // return HAS_OUTPUT with empty block to indicate end of stream
                return OperatorStatus::HAS_OUTPUT;
            }
        }
    }

private:
    bool done;
    MockReadTaskPoolPtr task_pool;
};
} // namespace

TEST_F(TestConcatSource, ConcatBuilderPoolWithDifferentConcurrency)
try
{
    LoggerPtr log = Logger::get();

    Blocks blks{
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
    };

    Block header = blks[0].cloneEmpty();


    PipelineExecutorContext exec_context;
    size_t num_concurrency = 8;
    ConcatBuilderPool builder_pool{num_concurrency};
    // Mock that for each partition (physical table), there is a read task pool and multiple source ops reading from it.
    size_t num_partitions = 2;
    for (size_t idx_part = 0; idx_part < num_partitions; ++idx_part)
    {
        // Mock that different partitions have different concurrency.
        size_t partition_concurrency = num_concurrency;
        if (idx_part == 0)
            partition_concurrency = 4;
        else if (idx_part == 1)
            partition_concurrency = 2;
        // Mock a queue shared on one partition
        auto blks_queue = std::make_shared<SyncBlocks>(blks);
        PipelineExecGroupBuilder group_builder;
        for (size_t i = 0; i < partition_concurrency; ++i)
        {
            group_builder.addConcurrency(std::make_unique<MockSourceOpFromQueue>(exec_context, blks_queue, header));
        }
        builder_pool.add(group_builder);
    }

    std::atomic<size_t> received_blocks = 0;
    ResultHandler h([&](const Block & /*block*/) { received_blocks.fetch_add(1, std::memory_order_relaxed); });

    PipelineExecGroupBuilder result_builder;
    builder_pool.generate(result_builder, exec_context, "test");
    result_builder.transform(
        [&](auto & builder) { builder.setSinkOp(std::make_unique<SimpleGetResultSinkOp>(exec_context, "test", h)); });
    auto op_pipeline_grp = result_builder.build(false);


    auto mgr = newThreadPoolManager(num_concurrency);

    for (const auto & pipe : op_pipeline_grp)
    {
        mgr->schedule(false, [&pipe, &log]() {
            pipe->executePrefix();
            while (true)
            {
                auto s = pipe->execute();
                if (s == OperatorStatus::FINISHED)
                {
                    LOG_INFO(log, "ConcatPipelineExec is finished");
                    break;
                }
            }
            pipe->executeSuffix();
        });
    }
    mgr->wait();

    LOG_INFO(log, "ConcatPipelineExec is built and executed, received_blocks={}", received_blocks.load());
    ASSERT_EQ(received_blocks.load(), blks.size() * num_partitions);
}
CATCH

TEST_F(TestConcatSource, unorderedConcatSink)
try
{
    LoggerPtr log = Logger::get();

    Blocks blks{
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
        Block{ColumnGenerator::instance().generate({2, "Int32", DataDistribution::RANDOM})},
    };

    Block header = blks[0].cloneEmpty();

    std::vector<MockReadTaskPoolPtr> pools;

    PipelineExecutorContext exec_context;
    size_t num_concurrency = 1;
    ConcatBuilderPool builder_pool{num_concurrency};
    // Mock that for each partition (physical table), there is a read task pool and multiple source ops reading from it.
    size_t num_partitions = 3;
    for (size_t idx_part = 0; idx_part < num_partitions; ++idx_part)
    {
        PipelineExecGroupBuilder group_builder;
        // MockReadTaskPoolPtr pool = std::make_shared<MockReadTaskPool>(std::ceil(num_concurrency * 1.5));
        MockReadTaskPoolPtr pool = std::make_shared<MockReadTaskPool>(idx_part, 0);
        pools.push_back(pool);
        for (size_t i = 0; i < num_concurrency; ++i)
        {
            group_builder.addConcurrency(std::make_unique<MockSourceOnReadTaskPoolOp>(exec_context, header, pool));
        }
        builder_pool.add(group_builder);
    }

    auto thread_func = [](const MockReadTaskPoolPtr & pool, const Blocks & blks, const LoggerPtr & log) {
        for (size_t i = 0; i < blks.size(); ++i)
        {
            Block blk = blks[i];
            pool->pushBlock(std::move(blk));
            LOG_INFO(log, "A block is pushed to pool, pool_idx={} n_pushed={}", pool->idx, i + 1);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        pool->finish();
        LOG_INFO(log, "All blocks are pushed to pool_idx={} n_pushed={}", pool->idx, blks.size());
    };

    auto thread_mgr = DB::newThreadManager();
    for (const auto & pool : pools)
    {
        thread_mgr->schedule(false, "", [pool, &blks, &log, &thread_func]() { thread_func(pool, blks, log); });
    }

    std::atomic<size_t> received_blocks = 0;
        received_blocks.fetch_add(1, std::memory_order_relaxed);

    PipelineExecGroupBuilder result_builder;
    builder_pool.generate(result_builder, exec_context, "test");
    result_builder.transform(
        [&](auto & builder) { builder.setSinkOp(std::make_unique<SimpleGetResultSinkOp>(exec_context, "test", h)); });
    auto op_pipeline_grp = result_builder.build(false);
    op_pipeline_grp[0]->executePrefix();
    while (true)
    {
        auto s = op_pipeline_grp[0]->execute();
        if (s == OperatorStatus::FINISHED)
        {
            LOG_INFO(log, "ConcatPipelineExec is finished");
            break;
        }
    }
    op_pipeline_grp[0]->executeSuffix();
    LOG_INFO(log, "ConcatPipelineExec is built and executed");
}
CATCH
} // namespace DB::tests
