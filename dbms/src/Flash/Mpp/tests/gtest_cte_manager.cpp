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

#include <Flash/Mpp/CTEManager.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <utility>

namespace DB
{
namespace tests
{
class TestCTEManager : public testing::Test
{
};

TEST_F(TestCTEManager, Basic)
try
{
    CTEManager manager;
    String query_id_and_cte_id = "pingcap";
    Int32 concurrency = 10;
    Int32 expected_sink_num = 1;
    Int32 expected_source_num = 1;
    auto cte = manager.getOrCreateCTE(query_id_and_cte_id, concurrency, expected_sink_num, expected_source_num);
    auto cte2 = manager.getOrCreateCTE(query_id_and_cte_id, concurrency, expected_sink_num, expected_source_num);
    ASSERT_EQ(cte.get(), cte2.get());

    tipb::SelectResponse resp;
    manager.releaseCTEBySink(resp, query_id_and_cte_id);
    ASSERT_TRUE(manager.hasCTEForTest(query_id_and_cte_id));
    manager.releaseCTEBySource(query_id_and_cte_id);
    ASSERT_FALSE(manager.hasCTEForTest(query_id_and_cte_id));

    cte = manager.getOrCreateCTE(query_id_and_cte_id, concurrency, expected_sink_num, expected_source_num);
    ASSERT_NE(cte.get(), cte2.get());
    ASSERT_TRUE(manager.hasCTEForTest(query_id_and_cte_id));
    manager.releaseCTE(query_id_and_cte_id);
    ASSERT_FALSE(manager.hasCTEForTest(query_id_and_cte_id));

    // Release with no error
    manager.releaseCTE(query_id_and_cte_id);
    manager.releaseCTEBySink(resp, query_id_and_cte_id);
    manager.releaseCTEBySource(query_id_and_cte_id);
    ASSERT_FALSE(manager.hasCTEForTest(query_id_and_cte_id));
}
CATCH

TEST_F(TestCTEManager, Concurrent)
try
{
    struct Meta
    {
        Meta(bool is_source_, String query_id_and_cte_id_)
            : is_source(is_source_)
            , query_id_and_cte_id(query_id_and_cte_id_)
        {}

        bool is_source;
        String query_id_and_cte_id;
    };

    std::mutex mu;
    std::set<String> cancelled_ctes;
    std::unordered_map<String, std::shared_ptr<CTE>> ctes;
    CTEManager manager;
    constexpr Int32 concurrency = 5;
    constexpr Int32 expected_sink_num = 2;
    constexpr Int32 expected_source_num = 2;

    auto working_func = [&](const Meta & meta) {
        std::random_device r;
        std::default_random_engine dre(r());
        std::uniform_int_distribution<uint64_t> di(1, 10);

        // Mock time elapsed before mpp task is received
        std::this_thread::sleep_for(std::chrono::microseconds(di(dre) * 100));

        const auto & query_id_and_cte_id = meta.query_id_and_cte_id;
        {
            std::lock_guard<std::mutex> lock(mu);

            // We will not keep testing if the cte has been cancelled
            if (cancelled_ctes.find(query_id_and_cte_id) == cancelled_ctes.end())
            {
                auto cte
                    = manager.getOrCreateCTE(query_id_and_cte_id, concurrency, expected_sink_num, expected_source_num);
                auto iter = ctes.find(query_id_and_cte_id);
                if (iter == ctes.end())
                {
                    ctes.insert(std::make_pair(query_id_and_cte_id, cte));
                }
                else
                {
                    // We must get same cte with other threads
                    ASSERT_EQ(iter->second.get(), cte.get());
                }
            }
        }

        // Mock working time
        std::this_thread::sleep_for(std::chrono::milliseconds(di(dre)));

        {
            std::lock_guard<std::mutex> lock(mu);
            auto iter = cancelled_ctes.find(query_id_and_cte_id);
            if (iter == cancelled_ctes.end())
                ASSERT_TRUE(manager.hasCTEForTest(query_id_and_cte_id));
            else
                ASSERT_FALSE(manager.hasCTEForTest(query_id_and_cte_id));
        }

        if (meta.is_source)
            manager.releaseCTEBySource(query_id_and_cte_id);
        else
        {
            tipb::SelectResponse resp;
            manager.releaseCTEBySink(resp, query_id_and_cte_id);
        }
    };

    auto cancel_func = [&]() {
        std::random_device r;
        std::default_random_engine dre(r());
        std::uniform_int_distribution<uint64_t> di(1, 10);

        for (size_t i = 0; i < 3; i++)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(di(dre) * 300));
            std::lock_guard<std::mutex> lock(mu);
            for (auto & item : ctes)
            {
                if (di(dre) <= 3)
                {
                    if (manager.hasCTEForTest(item.first))
                    {
                        manager.releaseCTE(item.first);
                        cancelled_ctes.insert(item.first);
                    }
                }
            }
        }
    };

    std::vector<Meta> metas;
    std::vector<String> query_id_and_cte_ids{"tidb", "tikv", "tiflash"};
    for (const auto & item : query_id_and_cte_ids)
    {
        for (int i = 0; i < 2; i++)
        {
            metas.emplace_back(i, item);
            metas.emplace_back(i, item);
        }
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(metas.begin(), metas.end(), g);

    constexpr Int32 test_num = 10;
    for (Int32 i = 0; i < test_num; i++)
    {
        auto working_thread_num = metas.size();
        std::vector<std::thread> threads;
        threads.reserve(working_thread_num + 1);

        for (size_t j = 0; j < working_thread_num; j++)
            threads.push_back(std::thread(working_func, metas[j]));
        threads.push_back(std::thread(cancel_func));
        for (auto & thd : threads)
            thd.join();

        for (const auto & item : query_id_and_cte_ids)
            ASSERT_FALSE(manager.hasCTEForTest(item));

        ctes.clear();
        cancelled_ctes.clear();
    }
}
CATCH

} // namespace tests
} // namespace DB
