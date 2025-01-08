// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/VersionChain/VersionChain.h>
#include <Storages/DeltaMerge/VersionChain/HandleColumnView.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{
TEST(HandleIndexTest, Basic)
{
    [[maybe_unused]] VersionChain<Int64> version_chain_int;
}

TEST(HandleColumnView, Basic)
{
    [[maybe_unused]] HandleColumnView<Int64> handle_column_view_int;
    [[maybe_unused]] HandleColumnView<String> handle_column_view_string;
}
} // namespace DB::DM::tests
