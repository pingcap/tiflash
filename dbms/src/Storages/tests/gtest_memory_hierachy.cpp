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

#include <Storages/DeltaMerge/DTMemoryHierachy.h>
#include <gtest/gtest.h>

#include <thread>

namespace DB::DM::tests
{
using namespace Memory;
TEST(DTMemoryHierachy, SingleThreadAllocation)
{
    auto buffer = LocalAllocatorBuffer::create();
    for (int i = 0; i < 100; ++i)
    {
        buffer.allocate(1024, 16);
        buffer.allocate(32, 4);
    }
}
TEST(DTMemoryHierachy, MultiThreadAllocation)
{
    std::vector<std::thread> handles;

    for (int n = 0; n < 60; ++n)
    {
        handles.clear();
        for (int i = 0; i < 10; ++i)
        {
            handles.emplace_back([] {
                for (int k = 0; k < 50; ++k)
                {
                    auto buffer = LocalAllocatorBuffer::create();
                    for (int i = 0; i < 20; ++i)
                    {
                        buffer.allocate(1024, 16);
                        buffer.allocate(32, 4);
                    }
                }
            });
        }
        for (auto & i : handles)
        {
            i.join();
        }
    }
}
} // namespace DB::DM::tests