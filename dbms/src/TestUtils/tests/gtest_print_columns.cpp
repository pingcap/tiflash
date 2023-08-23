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

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class PrintColumnsTest : public DB::tests::ExecutorTest
{
public:
    using ColStringType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColInt32Type = std::optional<typename TypeTraits<Int32>::FieldType>;
    using ColumnWithString = std::vector<ColStringType>;
    using ColumnWithInt32 = std::vector<ColInt32Type>;

    void initializeContext() override
    {
        test_cols.push_back(toNullableVec<Int32>("col1", ColumnWithInt32{36, 34, 32, 27, {}, {}}));
        test_cols.push_back(
            toNullableVec<String>("col2", ColumnWithString{"female", "male", "male", "female", "male", "female"}));
        col_len = test_cols[0].column->size();
    }

    ColumnsWithTypeAndName test_cols;
    size_t col_len;
    const String result1{"col1: (0: Int64_36, 1: Int64_34, 2: Int64_32, 3: Int64_27, 4: NULL, 5: NULL)\ncol2: (0: "
                         "'female', 1: 'male', 2: 'male', 3: 'female', 4: 'male', 5: 'female')\n"};
    const String result2{"col1: (0: Int64_36, 1: Int64_34, 2: Int64_32, 3: Int64_27, 4: NULL, 5: NULL)\ncol2: (0: "
                         "'female', 1: 'male', 2: 'male', 3: 'female', 4: 'male', 5: 'female')\n"};
    const String result3{"col1: (0: Int64_36)\ncol2: (0: 'female')\n"};
    const String result4{"col1: (1: Int64_34, 2: Int64_32, 3: Int64_27, 4: NULL)\ncol2: (1: 'male', 2: 'male', 3: "
                         "'female', 4: 'male')\n"};
};

TEST_F(PrintColumnsTest, SimpleTest)
try
{
    EXPECT_EQ(getColumnsContent(test_cols), result1);
    EXPECT_EQ(getColumnsContent(test_cols, 0, col_len), result2);
    EXPECT_EQ(getColumnsContent(test_cols, 0, 1), result3);
    EXPECT_EQ(getColumnsContent(test_cols, 1, col_len - 1), result4);
}
CATCH

} // namespace tests
} // namespace DB
