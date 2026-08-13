// Copyright 2025 PingCAP, Inc.
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

#include <Columns/ColumnVector.h>
#include <Core/SpillConfig.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/CTEManager.h>
#include <IO/Encryption/MockKeyManager.h>
#include <IO/FileProvider/FileProvider.h>
#include <Interpreters/Context.h>
#include <Operators/CTE.h>
#include <Operators/CTEPartition.h>
#include <Operators/CTEReader.h>
#include <Poco/File.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

namespace DB
{
namespace tests
{
namespace
{
constexpr size_t MAX_BLOCK_ROW_NUM = 10;
constexpr size_t PARTITION_NUM = 3;
constexpr size_t EXPECTED_SINK_NUM = 2;
constexpr size_t EXPECTED_SOURCE_NUM = 2;
constexpr size_t BLOCK_BYTES = MAX_BLOCK_ROW_NUM * sizeof(Int32);
const String QUERY_ID_AND_CTE_ID = "cte_spill_test";

class TestCTESpill : public testing::Test
{
protected:
    void SetUp() override
    {
        Poco::File spiller_dir(spill_dir);
        if (spiller_dir.exists())
            spiller_dir.remove(true);
        spiller_dir.createDirectories();

        context = TiFlashTestEnv::getContext();
        dag_context = std::make_unique<DAGContext>(1024);
        dag_context->setQueryIDAndCTEIDForSink(QUERY_ID_AND_CTE_ID);
        context->setDAGContext(dag_context.get());

        auto key_manager = std::make_shared<MockKeyManager>(false);
        auto file_provider = std::make_shared<FileProvider>(key_manager, false);
        spill_config = std::make_unique<SpillConfig>(spill_dir, "cte", 1024 * 1024, 0, 0, file_provider);
    }

    void TearDown() override
    {
        context->setDAGContext(nullptr);
        dag_context.reset();

        Poco::File spiller_dir(spill_dir);
        if (spiller_dir.exists())
            spiller_dir.remove(true);
    }

    std::shared_ptr<CTE> createCTE(CTEManager & manager, UInt64 operator_spill_threshold, const Block & schema)
    {
        auto cte = manager.getOrCreateCTE(QUERY_ID_AND_CTE_ID, PARTITION_NUM, EXPECTED_SINK_NUM, EXPECTED_SOURCE_NUM);
        cte->initForTest();
        cte->initCTESpillContextAndPartitionConfig(*spill_config, schema, operator_spill_threshold, *context);
        return cte;
    }

    static void assertSpillFilesExist()
    {
        std::vector<String> files;
        Poco::File(spill_dir).list(files);
        ASSERT_FALSE(files.empty());
    }

    static const String spill_dir;
    ContextPtr context;
    std::unique_ptr<DAGContext> dag_context;
    std::unique_ptr<SpillConfig> spill_config;
};

const String TestCTESpill::spill_dir = TiFlashTestEnv::getTemporaryPath("cte_spill_test");

Blocks generateSpillTestBlocks(size_t start_i, size_t row_num)
{
    Blocks blocks;
    blocks.reserve((row_num + MAX_BLOCK_ROW_NUM - 1) / MAX_BLOCK_ROW_NUM);

    auto data_type = std::make_shared<DataTypeInt32>();
    size_t i = start_i;
    while (i < start_i + row_num)
    {
        ColumnsWithTypeAndName cols{ColumnWithTypeAndName(data_type, "col0")};
        Block block(cols);
        auto * col = static_cast<ColumnVector<Int32> *>(block.getByPosition(0).column->assumeMutable().get());
        for (size_t j = 0; j < MAX_BLOCK_ROW_NUM && i < start_i + row_num; ++j, ++i)
            col->insert(i);
        blocks.push_back(std::move(block));
    }

    return blocks;
}

std::vector<std::unique_ptr<CTEReader>> createReaders(CTEManager & manager, const std::shared_ptr<CTE> & cte)
{
    std::vector<std::unique_ptr<CTEReader>> readers;
    readers.reserve(EXPECTED_SOURCE_NUM);
    for (size_t i = 0; i < EXPECTED_SOURCE_NUM; ++i)
        readers.push_back(std::make_unique<CTEReader>(QUERY_ID_AND_CTE_ID, &manager, cte));
    return readers;
}

void pushBlockAndSpill(CTE & cte, size_t partition_id, const Block & block)
{
    const auto status = cte.pushBlock<true>(partition_id, block);
    ASSERT_TRUE(status == CTEOpStatus::OK || status == CTEOpStatus::NEED_SPILL);
    if (status == CTEOpStatus::NEED_SPILL)
        ASSERT_EQ(cte.spillBlocks(partition_id), CTEOpStatus::OK);
}

void fetchAllBlocks(CTEReader & reader, Blocks & received_blocks)
{
    for (size_t partition_id = 0; partition_id < PARTITION_NUM; ++partition_id)
    {
        while (true)
        {
            Block block;
            const auto status = reader.fetchNextBlock(partition_id, block);
            if (status == CTEOpStatus::OK)
            {
                received_blocks.push_back(std::move(block));
                continue;
            }
            if (status == CTEOpStatus::IO_IN)
            {
                ASSERT_EQ(reader.fetchBlockFromDisk(partition_id, block), CTEOpStatus::OK);
                ASSERT_TRUE(block);
                received_blocks.push_back(std::move(block));
                continue;
            }
            ASSERT_EQ(status, CTEOpStatus::END_OF_FILE);
            break;
        }
    }
}

void assertBlocksContainRange(const Blocks & blocks, size_t start_i, size_t row_num)
{
    std::vector<Int64> received_results;
    for (const auto & block : blocks)
    {
        const auto * col = static_cast<const ColumnVector<Int32> *>(block.getByPosition(0).column.get());
        for (size_t i = 0; i < col->size(); ++i)
            received_results.push_back(col->get64(i));
    }

    ASSERT_EQ(received_results.size(), row_num);
    std::sort(received_results.begin(), received_results.end());
    for (size_t i = 0; i < row_num; ++i)
        ASSERT_EQ(received_results[i], start_i + i);
}

[[noreturn]] void throwUnexpectedStatus(CTEOpStatus status)
{
    throw Exception(fmt::format("Unexpected CTEOpStatus: {}", static_cast<Int32>(status)));
}

TEST_F(TestCTESpill, Basic)
try
{
    constexpr size_t row_num = 15 * MAX_BLOCK_ROW_NUM;
    auto sink_blocks = generateSpillTestBlocks(0, row_num);

    CTEManager manager;
    // Each partition spills whenever it accumulates two blocks. Five blocks are
    // sent to every partition, so every partition has both disk and memory data.
    auto cte = createCTE(manager, PARTITION_NUM * BLOCK_BYTES * 2, sink_blocks.front().cloneEmpty());
    auto readers = createReaders(manager, cte);

    ASSERT_ANY_THROW(cte->getCTEReaderID());
    for (size_t i = 0; i < EXPECTED_SINK_NUM; ++i)
        cte->registerSink();

    for (size_t i = 0; i < sink_blocks.size(); ++i)
        pushBlockAndSpill(*cte, i % PARTITION_NUM, sink_blocks[i]);

    constexpr size_t expected_spilled_block_num = 12;
    ASSERT_EQ(cte->total_spilled_blocks.load(), expected_spilled_block_num);
    ASSERT_EQ(cte->total_spilled_rows.load(), expected_spilled_block_num * MAX_BLOCK_ROW_NUM);
    assertSpillFilesExist();

    for (size_t i = 0; i < EXPECTED_SINK_NUM; ++i)
        cte->sinkExit<true>();

    for (auto & reader : readers)
    {
        Blocks received_blocks;
        fetchAllBlocks(*reader, received_blocks);
        ASSERT_EQ(received_blocks.size(), sink_blocks.size());
        assertBlocksContainRange(received_blocks, 0, row_num);
        ASSERT_EQ(reader->total_fetch_from_disk.load(), expected_spilled_block_num);
        ASSERT_EQ(reader->total_fetch_from_mem.load(), sink_blocks.size() - expected_spilled_block_num);
    }

    readers.clear();
    ASSERT_FALSE(manager.hasCTEForTest(QUERY_ID_AND_CTE_ID));
}
CATCH

TEST_F(TestCTESpill, Concurrent)
try
{
    constexpr size_t row_num_per_sink = 60 * MAX_BLOCK_ROW_NUM;
    constexpr size_t pre_spill_block_num_per_sink = 12;
    std::vector<Blocks> sink_blocks{
        generateSpillTestBlocks(0, row_num_per_sink),
        generateSpillTestBlocks(row_num_per_sink, row_num_per_sink)};

    CTEManager manager;
    // Four blocks per partition reach the threshold.
    auto cte = createCTE(manager, PARTITION_NUM * BLOCK_BYTES * 4, sink_blocks.front().front().cloneEmpty());
    auto readers = createReaders(manager, cte);

    ASSERT_ANY_THROW(cte->getCTEReaderID());
    for (size_t i = 0; i < EXPECTED_SINK_NUM; ++i)
        cte->registerSink();

    // Spill a deterministic prefix before starting readers. This guarantees
    // that the concurrent test exercises disk restore even on a fast machine.
    for (size_t sink_id = 0; sink_id < EXPECTED_SINK_NUM; ++sink_id)
    {
        for (size_t block_id = 0; block_id < pre_spill_block_num_per_sink; ++block_id)
            pushBlockAndSpill(*cte, block_id % PARTITION_NUM, sink_blocks[sink_id][block_id]);
    }
    ASSERT_EQ(cte->total_spilled_blocks.load(), EXPECTED_SINK_NUM * pre_spill_block_num_per_sink);
    assertSpillFilesExist();

    std::array<std::array<Blocks, PARTITION_NUM>, EXPECTED_SOURCE_NUM> received_blocks;
    std::array<std::atomic_size_t, EXPECTED_SINK_NUM> exited_sink_partitions{};
    std::atomic_bool thread_failed = false;
    std::atomic_bool cancelled = false;
    std::mutex exception_mu;
    std::exception_ptr unexpected_thread_exception;
    const String cancel_message = "cte spill concurrent test cancelled";

    auto record_unexpected_thread_exception = [&](std::exception_ptr exception) {
        {
            std::lock_guard lock(exception_mu);
            if (unexpected_thread_exception == nullptr)
                unexpected_thread_exception = std::move(exception);
        }
        thread_failed.store(true);
        cte->notifyCancel<true>("cte spill test thread failed");
    };

    auto is_expected_cancel_exception = [&](const std::exception_ptr & exception) {
        if (!cancelled.load())
            return false;

        try
        {
            std::rethrow_exception(exception);
        }
        catch (const Exception & e)
        {
            return e.message() == cancel_message;
        }
        catch (...)
        {
            return false;
        }
    };

    auto handle_thread_exception = [&](std::exception_ptr exception) {
        if (is_expected_cancel_exception(exception))
            return;

        record_unexpected_thread_exception(std::move(exception));
    };

    auto source_func = [&](size_t source_id, size_t partition_id) {
        try
        {
            auto & reader = *readers[source_id];
            auto & result = received_blocks[source_id][partition_id];
            while (!thread_failed.load())
            {
                Block block;
                const auto status = reader.fetchNextBlock(partition_id, block);
                switch (status)
                {
                case CTEOpStatus::OK:
                    result.push_back(std::move(block));
                    break;
                case CTEOpStatus::IO_IN:
                    while (!thread_failed.load())
                    {
                        block.clear();
                        const auto io_status = reader.fetchBlockFromDisk(partition_id, block);
                        if (io_status == CTEOpStatus::WAIT_SPILL)
                        {
                            std::this_thread::yield();
                            continue;
                        }
                        if (io_status == CTEOpStatus::CANCELLED)
                            return;
                        if (io_status != CTEOpStatus::OK)
                            throwUnexpectedStatus(io_status);
                        RUNTIME_CHECK(block);
                        result.push_back(std::move(block));
                        break;
                    }
                    break;
                case CTEOpStatus::WAIT_SPILL:
                case CTEOpStatus::BLOCK_NOT_AVAILABLE:
                case CTEOpStatus::SINK_NOT_REGISTERED:
                    std::this_thread::yield();
                    break;
                case CTEOpStatus::END_OF_FILE:
                case CTEOpStatus::CANCELLED:
                    return;
                default:
                    throwUnexpectedStatus(status);
                }
            }
        }
        catch (...)
        {
            handle_thread_exception(std::current_exception());
        }
    };

    auto sink_func = [&](size_t sink_id, size_t partition_id) {
        try
        {
            for (size_t block_id = pre_spill_block_num_per_sink; block_id < sink_blocks[sink_id].size(); ++block_id)
            {
                if (block_id % PARTITION_NUM != partition_id)
                    continue;
                if (thread_failed.load())
                    return;

                const auto status = cte->pushBlock<true>(partition_id, sink_blocks[sink_id][block_id]);
                if (status == CTEOpStatus::CANCELLED)
                    return;
                if (status != CTEOpStatus::OK && status != CTEOpStatus::NEED_SPILL && status != CTEOpStatus::WAIT_SPILL)
                    throwUnexpectedStatus(status);

                if (status == CTEOpStatus::NEED_SPILL || status == CTEOpStatus::WAIT_SPILL)
                {
                    while (!thread_failed.load())
                    {
                        const auto spill_status = cte->spillBlocks(partition_id);
                        if (spill_status == CTEOpStatus::OK)
                            break;
                        if (spill_status == CTEOpStatus::WAIT_SPILL)
                        {
                            std::this_thread::yield();
                            continue;
                        }
                        if (spill_status == CTEOpStatus::CANCELLED)
                            return;
                        throwUnexpectedStatus(spill_status);
                    }
                }

                if (block_id % 4 == 0)
                    std::this_thread::yield();
            }

            if (exited_sink_partitions[sink_id].fetch_add(1) + 1 == PARTITION_NUM)
                cte->sinkExit<true>();
        }
        catch (...)
        {
            handle_thread_exception(std::current_exception());
        }
    };

    auto cancel_func = [&] {
        std::random_device random_device;
        std::default_random_engine random_engine(random_device());
        std::uniform_int_distribution<size_t> random_delay_ms(1, 20);
        const auto delay_ms = random_delay_ms(random_engine);
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        if (delay_ms % 10 == 0)
        {
            cancelled.store(true);
            cte->notifyCancel<true>(cancel_message);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(EXPECTED_SOURCE_NUM * PARTITION_NUM + EXPECTED_SINK_NUM * PARTITION_NUM + 1);
    for (size_t source_id = 0; source_id < EXPECTED_SOURCE_NUM; ++source_id)
        for (size_t partition_id = 0; partition_id < PARTITION_NUM; ++partition_id)
            threads.emplace_back(source_func, source_id, partition_id);
    for (size_t sink_id = 0; sink_id < EXPECTED_SINK_NUM; ++sink_id)
        for (size_t partition_id = 0; partition_id < PARTITION_NUM; ++partition_id)
            threads.emplace_back(sink_func, sink_id, partition_id);
    threads.emplace_back(cancel_func);

    for (auto & thread : threads)
        thread.join();
    if (unexpected_thread_exception != nullptr)
        std::rethrow_exception(unexpected_thread_exception);

    if (cancelled.load())
    {
        manager.releaseCTE(QUERY_ID_AND_CTE_ID);
        readers.clear();
        ASSERT_FALSE(manager.hasCTEForTest(QUERY_ID_AND_CTE_ID));
        return;
    }

    const size_t total_row_num = EXPECTED_SINK_NUM * row_num_per_sink;
    const size_t total_block_num = sink_blocks[0].size() + sink_blocks[1].size();
    ASSERT_GE(cte->total_spilled_blocks.load(), EXPECTED_SINK_NUM * pre_spill_block_num_per_sink);
    for (size_t source_id = 0; source_id < EXPECTED_SOURCE_NUM; ++source_id)
    {
        Blocks all_blocks;
        for (size_t partition_id = 0; partition_id < PARTITION_NUM; ++partition_id)
        {
            auto & partition_blocks = received_blocks[source_id][partition_id];
            all_blocks.insert(
                all_blocks.end(),
                std::make_move_iterator(partition_blocks.begin()),
                std::make_move_iterator(partition_blocks.end()));
        }
        ASSERT_EQ(all_blocks.size(), total_block_num);
        assertBlocksContainRange(all_blocks, 0, total_row_num);
        ASSERT_EQ(readers[source_id]->total_fetch_blocks.load(), total_block_num);
        ASSERT_EQ(readers[source_id]->total_fetch_rows.load(), total_row_num);
        ASSERT_GE(readers[source_id]->total_fetch_from_disk.load(), EXPECTED_SINK_NUM * pre_spill_block_num_per_sink);
    }

    readers.clear();
    ASSERT_FALSE(manager.hasCTEForTest(QUERY_ID_AND_CTE_ID));
}
CATCH

} // namespace
} // namespace tests
} // namespace DB
