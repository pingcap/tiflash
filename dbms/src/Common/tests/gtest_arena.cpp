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

#include <Common/Arena.h>
#include <Interpreters/JoinHashMap.h>
#include <gtest/gtest.h>

namespace DB::tests
{
TEST(ArenaTest, AlignedAlloc)
{
    auto pool = DB::Arena(1024);
    auto * p = pool.alloc(3);
    p = pool.alloc(2);
    // the first allocation is not aligned
    ASSERT_NE(reinterpret_cast<uintptr_t>(p) % 16, 0);
    // the second allocation is aligned
    p = pool.alignedAlloc(sizeof(CachedColumnInfo), 16);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(p) % 16, 0);
}
} // namespace DB::tests
