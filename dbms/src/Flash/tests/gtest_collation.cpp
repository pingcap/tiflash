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

#include <TestUtils/ExecutorTestUtils.h>
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{

class ExecutorCollation : public DB::tests::ExecutorTest
{
public:
    using ColStringNullableType = std::optional<typename TypeTraits<String>::FieldType>;
    using ColUInt64Type = typename TypeTraits<UInt64>::FieldType;

    using ColumnWithNullableString = std::vector<ColStringNullableType>;
    using ColumnWithUInt64 = std::vector<ColUInt64Type>;

    void initializeContext() override
    {
        ExecutorTest::initializeContext();

        context.addMockTable({db_name, table_name},
                             {{col_name, TiDB::TP::TypeString}},
                             {toNullableVec<String>(col_name, col)});

        context.addMockTable({db_name, chinese_table},
                             {{chinese_col_name, TiDB::TP::TypeString}},
                             {toNullableVec<String>(chinese_col_name, chinese_col)});
    }

    /// Prepare some names
    const String db_name{"test_db"};
    const String table_name{"collation_table"};
    const String col_name{"col"};
    const ColumnWithNullableString col{"china", "china", "china  ", "CHINA", "cHiNa ", "usa", "usa", "usa  ", "USA", "USA "};

    const String chinese_table{"chinese"};
    const String chinese_col_name{"col"};
    const ColumnWithNullableString chinese_col{"北京", "北京  ", "北bei京", "北Bei京", "北bei京  ", "上海", "上海  ", "shanghai  ", "ShangHai", "ShangHai  "};
};

/// Guarantee that test framework has correctly supported the collation.
TEST_F(ExecutorCollation, Verification)
try
{
    std::shared_ptr<tipb::DAGRequest> request;
    {
        /// Test default collation(utf8mb4_bin)
        request = context.scan(db_name, table_name).aggregation(MockAstVec{}, {col(col_name)}).project({col_name}).build(context);
        ASSERT_COLUMNS_EQ_UR(ColumnsWithTypeAndName{toNullableVec<String>(col_name, ColumnWithNullableString{"usa", "CHINA", "USA", "china", "cHiNa "})}, executeStreams(request, 1));

        request = context.scan(db_name, chinese_table).aggregation(MockAstVec{}, {col(chinese_col_name)}).project({chinese_col_name}).build(context);
        ASSERT_COLUMNS_EQ_UR(ColumnsWithTypeAndName{toNullableVec<String>(chinese_col_name, ColumnWithNullableString{"ShangHai", "北京", "北Bei京", "shanghai  ", "北bei京", "上海"})}, executeStreams(request, 1));
    }

    {
        /// Test utf8mb4_general_ci
        context.setCollation(TiDB::ITiDBCollator::UTF8_GENERAL_CI);
        request = context.scan(db_name, table_name).aggregation(MockAstVec{}, {col(col_name)}).project({col_name}).build(context);
        ASSERT_COLUMNS_EQ_UR(ColumnsWithTypeAndName{toNullableVec<String>(col_name, ColumnWithNullableString{"usa", "china"})}, executeStreams(request, 1));

        request = context.scan(db_name, chinese_table).aggregation(MockAstVec{}, {col(chinese_col_name)}).project({chinese_col_name}).build(context);
        ASSERT_COLUMNS_EQ_UR(ColumnsWithTypeAndName{toNullableVec<String>(chinese_col_name, ColumnWithNullableString{"北京", "shanghai  ", "北bei京", "上海"})}, executeStreams(request, 1));
    }

    {
        /// Test utf8_bin
        context.setCollation(TiDB::ITiDBCollator::UTF8_BIN);
        request = context.scan(db_name, table_name).aggregation(MockAstVec{}, {col(col_name)}).project({col_name}).build(context);
        ASSERT_COLUMNS_EQ_UR(ColumnsWithTypeAndName{toNullableVec<String>(col_name, ColumnWithNullableString{"USA", "CHINA", "usa", "china", "cHiNa "})}, executeStreams(request, 1));
    }

    {
        /// Test utf8_unicode_CI
        context.setCollation(TiDB::ITiDBCollator::UTF8_UNICODE_CI);
        request = context.scan(db_name, table_name).aggregation(MockAstVec{}, {col(col_name)}).project({col_name}).build(context);
        ASSERT_COLUMNS_EQ_UR(ColumnsWithTypeAndName{toNullableVec<String>(col_name, ColumnWithNullableString{"china", "usa"})}, executeStreams(request, 1));
    }
}
CATCH

} // namespace tests
} // namespace DB
