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

#include <Common/Arena.h>
#include <Interpreters/JoinHashMap.h>
#include <gtest/gtest.h>

#include <mutex>
#include <type_traits>

namespace DB::tests
{
TEST(ArenaTest, AlignedAlloc)
{
    auto pool = DB::Arena(1024);
    auto * p = pool.alloc(3);
    p = pool.alloc(2);
    auto align = alignof(CachedColumnInfo);
    // the first allocation is not aligned
    ASSERT_NE(reinterpret_cast<uintptr_t>(p) % align, 0);
    // the second allocation is aligned
    p = pool.alignedAlloc(sizeof(CachedColumnInfo), align);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(p) % align, 0);
    auto * cached_column_info = reinterpret_cast<CachedColumnInfo *>(p);
    new (cached_column_info) CachedColumnInfo(nullptr);
    // double check the alignment, will raise bus error if not aligned in some platforms
    cached_column_info->mu.lock();
    cached_column_info->mu.unlock();
}
} // namespace DB::tests
