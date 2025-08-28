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
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Mpp/CTEManager.h>
#include <Operators/CTE.h>
#include <Operators/CTEPartition.h>
#include <Operators/CTEReader.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

namespace DB
{
namespace tests
{
constexpr size_t MAX_BLOCK_ROW_NUM = 10;
constexpr size_t PARTITION_NUM = 3;
constexpr size_t EXPECTED_SINK_NUM = 2;
constexpr size_t EXPECTED_SOURCE_NUM = 2;

class TestCTE : public testing::Test
{
};

Blocks generateBlocks(size_t start_i, size_t row_num)
{
    Blocks blocks;
    blocks.reserve(row_num / MAX_BLOCK_ROW_NUM);

    auto data_type = std::make_shared<DataTypeInt32>();
    size_t i = start_i;
    while (i < start_i + row_num)
    {
        ColumnsWithTypeAndName cols{ColumnWithTypeAndName(data_type, "col0")};
        Block block(cols);
        auto * col = static_cast<ColumnVector<Int32> *>(block.getByPosition(0).column->assumeMutable().get());
        for (size_t j = 0; j < MAX_BLOCK_ROW_NUM && i < start_i + row_num; j++, i++)
            col->insert(i);

        blocks.push_back(block);
    }

    return blocks;
}

TEST_F(TestCTE, Basic)
try
{
    String query_id_and_cte_id("pingcap");
    CTEManager manager;
    auto cte = manager.getOrCreateCTE(query_id_and_cte_id, PARTITION_NUM, EXPECTED_SINK_NUM, EXPECTED_SOURCE_NUM);
    cte->initForTest();

    std::vector<std::unique_ptr<CTEReader>> readers;
    readers.reserve(EXPECTED_SOURCE_NUM);
    for (size_t i = 0; i < EXPECTED_SOURCE_NUM; i++)
    {
        auto reader_ptr = std::make_unique<CTEReader>(query_id_and_cte_id, &manager, cte);
        readers.push_back(std::move(reader_ptr));
    }

    ASSERT_ANY_THROW(cte->getCTEReaderID());

    for (size_t i = 0; i < EXPECTED_SINK_NUM; i++)
        cte->registerSink();

    // Push blocks and get blocks
    std::random_device r;
    std::default_random_engine dre(r());
    std::uniform_int_distribution<Int64> di(0, 10000);
    size_t row_num = di(dre);
    Blocks sink_blocks = generateBlocks(0, row_num);
    for (size_t i = 0; i < sink_blocks.size(); i++)
        ASSERT_TRUE(cte->pushBlock<true>(i % PARTITION_NUM, sink_blocks[i]));

    Blocks received_blocks;
    std::vector<Int64> received_results;
    received_blocks.reserve(row_num / MAX_BLOCK_ROW_NUM);
    received_results.reserve(row_num);

    Block block;
    for (auto & reader : readers)
    {
        received_blocks.clear();
        received_results.clear();
        for (size_t i = 0; i < PARTITION_NUM; i++)
        {
            while (reader->fetchNextBlock(i, block) == CTEOpStatus::OK)
                received_blocks.push_back(block);
        }

        ASSERT_EQ(received_blocks.size(), sink_blocks.size());

        for (const auto & item : received_blocks)
        {
            const auto * col = static_cast<const ColumnVector<Int32> *>(item.getByPosition(0).column.get());
            for (size_t i = 0; i < col->size(); i++)
                received_results.push_back(col->get64(i));
        }

        ASSERT_EQ(received_results.size(), row_num);

        std::sort(received_results.begin(), received_results.end());
        for (size_t i = 0; i < row_num; i++)
            ASSERT_EQ(received_results[i], i);
    }
}
CATCH

void concurrentTest()
{
    std::random_device r;
    std::default_random_engine dre(r());
    std::uniform_int_distribution<size_t> di1(0, 3);
    std::uniform_int_distribution<size_t> di2(100, 200);
    std::uniform_int_distribution<size_t> di3(100, 200);

    std::vector<size_t> row_num{di1(dre) * di2(dre) * di3(dre), di1(dre) * di2(dre) * di3(dre)};
    size_t total_row_num = row_num[0] + row_num[1];
    std::vector<Blocks> sink_blocks{generateBlocks(0, row_num[0]), generateBlocks(row_num[0], row_num[1])};
    size_t total_block_num = sink_blocks[0].size() + sink_blocks[1].size();
    std::vector<size_t> next_sink_idxs{0, 0};
    std::vector<std::shared_ptr<std::mutex>> sink_mus{std::make_shared<std::mutex>(), std::make_shared<std::mutex>()};
    ASSERT_EQ(row_num.size(), EXPECTED_SINK_NUM);
    ASSERT_EQ(sink_blocks.size(), EXPECTED_SINK_NUM);
    ASSERT_EQ(next_sink_idxs.size(), EXPECTED_SINK_NUM);
    ASSERT_EQ(sink_mus.size(), EXPECTED_SINK_NUM);

    String query_id_and_cte_id("pingcap");
    CTEManager manager;
    auto cte = manager.getOrCreateCTE(query_id_and_cte_id, PARTITION_NUM, EXPECTED_SINK_NUM, EXPECTED_SOURCE_NUM);
    cte->initForTest();

    std::vector<std::unique_ptr<CTEReader>> readers;
    std::vector<std::shared_ptr<std::mutex>> source_mus{std::make_shared<std::mutex>(), std::make_shared<std::mutex>()};
    std::vector<Blocks> received_blocks;
    ASSERT_EQ(source_mus.size(), EXPECTED_SOURCE_NUM);
    readers.reserve(EXPECTED_SOURCE_NUM);
    received_blocks.resize(EXPECTED_SOURCE_NUM);
    ASSERT_EQ(received_blocks.size(), EXPECTED_SOURCE_NUM);
    for (size_t i = 0; i < EXPECTED_SOURCE_NUM; i++)
    {
        auto reader_ptr = std::make_unique<CTEReader>(query_id_and_cte_id, &manager, cte);
        readers.push_back(std::move(reader_ptr));
        received_blocks[i].reserve(sink_blocks[i].size());
    }
    ASSERT_EQ(readers.size(), EXPECTED_SOURCE_NUM);

    ASSERT_ANY_THROW(cte->getCTEReaderID());

    size_t working_thread_num = EXPECTED_SINK_NUM * PARTITION_NUM + EXPECTED_SOURCE_NUM * PARTITION_NUM;
    size_t thread_num = working_thread_num + 1;
    std::vector<std::thread> threads;
    threads.reserve(thread_num);

    auto source_func = [&](size_t source_idx, size_t partition_idx) {
        auto * reader = readers[source_idx].get();
        Block block;
        bool exit = false;
        std::uniform_int_distribution<size_t> di1(1, 10);
        std::uniform_int_distribution<size_t> di2(10, 50);
        while (!exit)
        {
            if unlikely (di1(dre) == 5)
                std::this_thread::sleep_for(std::chrono::microseconds(di2(dre)));

            auto status = reader->fetchNextBlock(partition_idx, block);
            switch (status)
            {
            case CTEOpStatus::CANCELLED:
            case CTEOpStatus::END_OF_FILE:
            {
                exit = true;
                break;
            }
            case CTEOpStatus::SINK_NOT_REGISTERED:
            {
                while (!reader->areAllSinksRegistered())
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                break;
            }
            case CTEOpStatus::BLOCK_NOT_AVAILABLE:
            {
                auto status = reader->waitForBlockAvailableForTest(partition_idx);
                switch (status)
                {
                case CTEOpStatus::CANCELLED:
                case CTEOpStatus::END_OF_FILE:
                    exit = true;
                case CTEOpStatus::OK:
                    break;
                default:
                    throw Exception("Should not reach here");
                }
                break;
            }
            case CTEOpStatus::OK:
            {
                std::lock_guard<std::mutex> lock(*(source_mus[source_idx]));
                received_blocks[source_idx].push_back(block);
            }
            }
        }
        if (partition_idx == 0)
            cte->sourceExit();
    };

    std::vector<size_t> exit_num_for_each_partition;
    exit_num_for_each_partition.resize(EXPECTED_SINK_NUM, 0);

    auto sink_func = [&](size_t sink_idx, size_t partition_idx) {
        std::uniform_int_distribution<size_t> di1(1, 10);
        std::uniform_int_distribution<size_t> di2(10, 50);

        std::this_thread::sleep_for(std::chrono::milliseconds(di2(dre)));

        if (partition_idx == 0)
            cte->registerSink();

        while (true)
        {
            size_t next_sink_idx;
            {
                std::lock_guard<std::mutex> lock(*(sink_mus[sink_idx]));
                next_sink_idx = next_sink_idxs[sink_idx]++;
                if unlikely (next_sink_idx >= sink_blocks[sink_idx].size())
                {
                    ++exit_num_for_each_partition[sink_idx];
                    if (exit_num_for_each_partition[sink_idx] == PARTITION_NUM)
                        cte->sinkExit<true>();
                    break;
                }
            }

            if unlikely (di1(dre) == 5)
                std::this_thread::sleep_for(std::chrono::microseconds(di2(dre)));
            cte->pushBlock<true>(partition_idx, sink_blocks[sink_idx][next_sink_idx]);
        }
    };

    std::atomic_bool cancelled = false;
    auto cancel_func = [&]() {
        std::uniform_int_distribution<size_t> di(1, 200);
        auto random_val = di(dre);
        std::this_thread::sleep_for(std::chrono::milliseconds(random_val));
        if (random_val % 10 == 0)
        {
            cancelled.exchange(true);
            cte->notifyCancel<true>("");
        }
    };

    for (size_t i = 0; i < EXPECTED_SOURCE_NUM; i++)
        for (size_t j = 0; j < PARTITION_NUM; j++)
            threads.push_back(std::thread(source_func, i, j));

    for (size_t i = 0; i < EXPECTED_SINK_NUM; i++)
        for (size_t j = 0; j < PARTITION_NUM; j++)
            threads.push_back(std::thread(sink_func, i, j));

    threads.push_back(std::thread(cancel_func));

    for (auto & thd : threads)
        thd.join();

    ASSERT_TRUE(cte->allExit());

    if unlikely (cancelled.load())
        return;

    for (const auto & blocks : received_blocks)
        ASSERT_EQ(blocks.size(), total_block_num);

    std::vector<std::vector<Int64>> received_results;
    received_results.resize(EXPECTED_SOURCE_NUM);

    for (size_t i = 0; i < EXPECTED_SOURCE_NUM; i++)
    {
        for (const auto & block : received_blocks[i])
        {
            const auto * col = static_cast<const ColumnVector<Int32> *>(block.getByPosition(0).column.get());
            for (size_t j = 0; j < col->size(); j++)
                received_results[i].push_back(col->get64(j));
        }

        ASSERT_EQ(received_results[i].size(), total_row_num);
        std::sort(received_results[i].begin(), received_results[i].end());
    }

    for (const auto & results : received_results)
        for (size_t i = 0; i < total_row_num; i++)
            ASSERT_EQ(results[i], i);
}

TEST_F(TestCTE, Concurrent)
try
{
    for (size_t i = 0; i < 10; i++)
        concurrentTest();
}
CATCH

} // namespace tests
} // namespace DB
