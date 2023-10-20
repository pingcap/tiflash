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

#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{

TEST(TestColumnGenerator, run)
try
{
    std::vector<String> type_vec
        = {"Int8",
           "Int16",
           "Int32",
           "Int64",
           "UInt8",
           "UInt16",
           "UInt32",
           "UInt64",
           "Float32",
           "Float64",
           "String",
           "MyDateTime",
           "MyDate",
           "Decimal"};
    for (size_t i = 10; i <= 100000; i *= 10)
    {
        for (auto type : type_vec)
            ASSERT_EQ(ColumnGenerator::instance().generate({i, type, RANDOM}).column->size(), i);
    }
}
CATCH

} // namespace tests

} // namespace DB
