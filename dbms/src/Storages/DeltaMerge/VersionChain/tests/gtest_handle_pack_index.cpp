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

#include <Storages/DeltaMerge/VersionChain/ColumnView.h>
#include <Storages/DeltaMerge/VersionChain/VersionChain.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <gtest/gtest.h>

using namespace DB::tests;

namespace DB::DM::tests
{
TEST(HandleIndexTest, Basic)
{
    [[maybe_unused]] VersionChain<Int64> version_chain_int;
    [[maybe_unused]] VersionChain<String> version_chain_str;
}

TEST(ColumnView, Basic)
{
    auto str_col = ColumnGenerator::instance().generate({1024, "String", RANDOM}).column;

    //auto str_col = makeColumn<String>(MutSup::getExtraHandleColumnStringType(), {"0000", "11", "222", "33333"});
    ColumnView<String> str_cv(*str_col);
    for (auto s_itr = str_cv.begin(); s_itr != str_cv.end(); ++s_itr)
    {
        ASSERT_EQ(*s_itr, str_col->getDataAt(s_itr - str_cv.begin()).toStringView());
    }
    //fmt::println("=======================");
    //auto itr = std::lower_bound(str_cv.begin(), str_cv.end(), "hello");
    //std::ignore = itr;
}
} // namespace DB::DM::tests
