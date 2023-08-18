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

#include <Flash/Coprocessor/DAGUtils.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
TEST(UniqueNameGenerator_test, DuplicateNames)
{
    std::vector<std::string> input_names = {"a", "a", "a_1", "a_2", "a", "a_3", "a_3", "a_3"};
    std::vector<std::string> output_names = {"a", "a_1", "a_1_1", "a_2", "a_2_1", "a_3", "a_3_1", "a_3_2"};
    UniqueNameGenerator unique_name_generator;
    for (size_t i = 0; i < input_names.size(); i++)
    {
        ASSERT_TRUE(unique_name_generator.toUniqueName(input_names[i]) == output_names[i]);
    }
}

} // namespace tests
} // namespace DB
