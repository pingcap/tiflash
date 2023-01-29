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

#include <Storages/Page/V3/PageEntry.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>

namespace DB::PS::V3::tests
{
TEST(PageEntry, TightOrLoose)
{
    std::cout << "size of PageEntryV3Tight: " << sizeof(PageEntryV3Tight) << std::endl;
    std::cout << "size of PageEntryV3Loose: " << sizeof(PageEntryV3Tight) << std::endl;

    auto entry1 = makePageEntry(0, 0, 0, 0, 0, 0);
    auto entry2 = makePageEntry(0, 0, 1, 0, 0, 0);
    auto entry3 = makePageEntry(0, 0, 0, 1, 0, 0);
    auto entry4 = makePageEntry(4294967296, 0, 0, 0, 0, 0);
    auto entry5 = makePageEntry(0, 4294967296, 0, 0, 0, 0);
    auto entry6 = makePageEntry(0, 0, 0, 0, 4294967296, 0);
    auto entry7 = makePageEntry(4294967295, 0, 0, 0, 0, 0);
    auto entry8 = makePageEntry(0, 4294967295, 0, 0, 0, 0);
    auto entry9 = makePageEntry(0, 0, 0, 0, 4294967295, 0);
    auto entry10 = makePageEntry(1, 0, 0, 0, 0, 0);
    auto entry11 = makePageEntry(1, 0, 0, 0, 0, 0, PageFieldOffsetChecksums{{0, 0}});

    ASSERT_TRUE(entry1->isTight());
    ASSERT_TRUE(!entry2->isTight());
    ASSERT_TRUE(!entry3->isTight());
    ASSERT_TRUE(!entry4->isTight());
    ASSERT_TRUE(!entry5->isTight());
    ASSERT_TRUE(!entry6->isTight());
    ASSERT_TRUE(entry7->isTight());
    ASSERT_TRUE(entry8->isTight());
    ASSERT_TRUE(entry9->isTight());
    ASSERT_TRUE(entry10->isTight());
    ASSERT_TRUE(!entry11->isTight());
}
} // namespace DB::PS::V3::tests