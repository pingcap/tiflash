// Copyright 2022 PingCAP, Ltd.
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
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaTree.h>
#include <Storages/DeltaMerge/DeltaTreeMemory.h>
#include <gtest/gtest.h>

#include <set>
#include <thread>

namespace DB::DM
{
TEST(DeltaTreeMemory, GlobalRecycle)
{
    auto global = NumaAwareMemoryHierarchy::GlobalPagePool{};
    auto & list = global.getFreeList();
    auto * a = list.allocate();
    list.recycle(a);
    auto * b = list.allocate();
    list.recycle(b);
    EXPECT_EQ(a, b);
}
TEST(DeltaTreeMemory, GlobalMTSafe)
{
    auto size = std::thread::hardware_concurrency();
    auto global = NumaAwareMemoryHierarchy::GlobalPagePool{};
    std::vector<std::thread> handles;

    for (size_t i = 0; i < size; ++i)
    {
        handles.emplace_back([&] {
            auto & list = global.getFreeList();
            for (int k = 0; k < 1000; ++k)
            {
                auto * p = static_cast<int *>(list.allocate());
                *p = k;
                std::this_thread::yield();
                std::this_thread::yield();
                std::this_thread::yield();
                std::this_thread::yield();
                std::this_thread::yield();
                EXPECT_EQ(*p, k);
                list.recycle(p);
            }
        });
    }

    for (auto & i : handles)
    {
        i.join();
    }
}

TEST(DeltaTreeMemory, BasicAllocation)
{
    EXPECT_EQ(NumaAwareMemoryHierarchy::Client::alignedSize(1000, 8), 1024ull);
    EXPECT_EQ(NumaAwareMemoryHierarchy::Client::alignedSize(2011, 8), 2048ul);
    auto client = getClient(sizeof(DefaultDeltaTree::Intern), alignof(DefaultDeltaTree::Intern));
    client.deallocate(client.allocate());
}

TEST(DeltaTreeMemory, MTAllocation)
{
    auto size = std::thread::hardware_concurrency();
    std::vector<std::thread> handles;

    for (size_t i = 0; i < size; ++i)
    {
        handles.emplace_back([&] {
            for (int k = 0; k < 1000; ++k)
            {
                auto client = getClient(1023, 8);
                auto * m = static_cast<int *>(client.allocate());
                *m = 0x123456;
                for (int n = 0; n < 1000; ++n)
                {
                    auto * p = static_cast<int *>(client.allocate());
                    *p = k;
                    EXPECT_EQ(*p, k);
                    EXPECT_EQ(*m, 0x123456);
                    client.deallocate(p);
                }
                client.deallocate(m);
                std::this_thread::yield();
            }
        });
    }

    for (auto & i : handles)
    {
        i.join();
    }
}
} // namespace DB::DM