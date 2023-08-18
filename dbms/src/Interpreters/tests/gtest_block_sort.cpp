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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/sortBlock.h>
#include <TestUtils/FunctionTestUtils.h>


namespace DB
{
namespace tests
{

class BlockSort : public ::testing::Test
{
public:
    using ColStringType = typename TypeTraits<String>::FieldType;
    using ColInt64Type = typename TypeTraits<Int64>::FieldType;
    using ColUInt64Type = typename TypeTraits<UInt64>::FieldType;
    using ColumnWithString = std::vector<ColStringType>;
    using ColumnWithInt64 = std::vector<ColInt64Type>;
    using ColumnWithUInt64 = std::vector<ColUInt64Type>;

    const String single_col_name{"single_col"};
    const ColumnWithString
        col0_ori{"col0-1  ", "col0-7", "col0-0    ", "col0-3", "col0-4", "col0-6", "col0-2 ", "col0-5"};
    const std::vector<String> col_name{"age", "gender", "country", "int64", "uint64"};
};

TEST_F(BlockSort, Limit)
try
{
    {
        /// Test multi-columns

        const ColumnsWithTypeAndName ori_col
            = {toVec<Int64>(col_name[0], ColumnWithInt64{5, 1, 2, 3, 4, 6}),
               toVec<String>(col_name[1], ColumnWithString{"5", "1    ", "2  ", "2", "4", "6"}),
               toVec<String>(col_name[2], ColumnWithString{"1", "2", "3", "4", "1 ", "2"})};
        for (size_t limit : {3ul, ori_col[0].column->size()})
        {
            for (int direction : {-1, 1})
            {
                for (auto collator_default : {TiDB::ITiDBCollator::UTF8MB4_BIN, TiDB::ITiDBCollator::UTF8_GENERAL_CI})
                    for (int same_col : {0, 1})
                    {
                        Block block(ori_col);
                        SortDescription description;
                        const auto * collator_1_ptr = TiDB::ITiDBCollator::getCollator(collator_default);
                        const auto * collator_2_ptr = same_col
                            ? collator_1_ptr
                            : TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::UTF8_BIN);
                        description.emplace_back(ori_col[1].name, direction, 0, collator_1_ptr);
                        description.emplace_back(ori_col[2].name, direction, 0, collator_2_ptr);
                        sortBlock(block, description, limit);
                        auto cols = block.getColumnsWithTypeAndName();
                        ASSERT_EQ(ori_col.size(), cols.size());
                        const auto * col_0 = typeid_cast<const ColumnInt64 *>(cols[0].column.get());
                        ASSERT_TRUE(col_0);
                        ASSERT_EQ(col_0->size(), limit);
                        if (direction > 0)
                        {
                            for (size_t i = 0; i < limit; ++i)
                            {
                                ASSERT_EQ(col_0->getUInt(i), i + 1);
                            }
                        }
                        else
                        {
                            for (size_t i = 0; i < limit; ++i)
                            {
                                ASSERT_EQ(col_0->getUInt(i), 6 - i);
                            }
                        }
                    }
            }
        }
    }
    {
        const ColumnsWithTypeAndName ori_col = {
            toVec<Int64>(col_name[0], ColumnWithInt64{1, 0, -1}),
            toVec<String>(col_name[1], ColumnWithString{"1   ", "1  ", "1 "}),
            toVec<String>(col_name[2], ColumnWithString{"1", "2", "3"}),
            toVec<UInt64>(col_name[3], ColumnWithUInt64{1, 1, 0}),
        };
        for (bool no_collation : {true, false})
        {
            Block block(ori_col);
            SortDescription description;
            size_t limit = ori_col[0].column->size();
            const auto * collator_1_ptr = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
            const auto * collator_2_ptr = collator_1_ptr;
            if (no_collation)
            {
                collator_1_ptr = nullptr;
                collator_2_ptr = nullptr;
            }
            description.emplace_back(ori_col[1].name, 1, 0, collator_1_ptr);
            description.emplace_back(ori_col[2].name, 1, 0, collator_2_ptr);
            sortBlock(block, description, limit);
            auto cols = block.getColumnsWithTypeAndName();
            ASSERT_EQ(ori_col.size(), cols.size());
            const auto * col_0 = typeid_cast<const ColumnInt64 *>(cols[0].column.get());
            ASSERT_TRUE(col_0);
            ASSERT_EQ(col_0->size(), limit);

            for (int i = 0; i < int(limit); ++i)
            {
                ASSERT_EQ(col_0->getElement(i), i - 1);
            }
        }
        {
            Block block(ori_col);
            SortDescription description;
            size_t limit = ori_col[0].column->size();
            description.emplace_back(ori_col[3].name, 1, 0, nullptr);
            description.emplace_back(ori_col[0].name, 1, 0, nullptr);
            sortBlock(block, description, limit);
            auto cols = block.getColumnsWithTypeAndName();
            ASSERT_EQ(ori_col.size(), cols.size());
            const auto * col_0 = typeid_cast<const ColumnInt64 *>(cols[0].column.get());
            ASSERT_TRUE(col_0);
            ASSERT_EQ(col_0->size(), limit);

            for (size_t i = 0; i < limit; ++i)
            {
                ASSERT_EQ(col_0->getElement(i), i - 1);
            }
        }
    }
}
CATCH

} // namespace tests
} // namespace DB
