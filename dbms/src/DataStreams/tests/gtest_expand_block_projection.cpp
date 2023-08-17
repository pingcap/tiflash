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

#include <Core/Field.h>
#include <DataStreams/ExpandBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Interpreters/Expand2.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/ProjectionTestUtil.h>


namespace DB
{
namespace tests
{
class ExpandBlockInputStreamTest : public DB::tests::ProjectionTest
{
public:
    void SetUp() override {}
    static ColumnWithTypeAndName toVec(const std::vector<UInt64> & v) { return createColumn<UInt64>(v); }
    static ColumnWithTypeAndName toNullVec(const std::vector<std::optional<Int64>> & v)
    {
        return createColumn<Nullable<Int64>>(v);
    }
};

TEST_F(ExpandBlockInputStreamTest, testExpandProjection)
try
{
    // mock stream, col1<nullable<Int64>>, col2<UInt64>
    std::vector<std::optional<Int64>> values1{1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<UInt64> values2{1, 2, 3, 4, 5, 6, 7, 8, 9};
    Block source_block;
    auto col1 = toNullVec(values1);
    col1.name = "test_int_col_1";
    auto col2 = toVec(values2);
    col2.name = "test_int_col_2";
    source_block.insert(0, col1);
    source_block.insert(1, col2);
    auto one_block_input_stream = std::make_shared<OneBlockInputStream>(source_block);

    // level-1 projection: nullable<int64><col1> as literal(null), colref-col2
    auto [level_1_actions, level_1_names] = buildProjection(
        *context,
        source_block.getColumnsWithTypeAndName(),
        ColumnNumbers{0},
        std::vector<Field>{Field(Null())},
        ColumnNumbers{1});
    NamesWithAliases level_1_project_cols;
    level_1_project_cols.emplace_back(level_1_names[0], "test_int_col_1");
    level_1_project_cols.emplace_back(level_1_names[1], "test_int_col_2");

    // level-2 projection: colref-col1, <UInt64><col2> as literal(9)
    auto [level_2_actions, level_2_names] = buildProjection(
        *context,
        source_block.getColumnsWithTypeAndName(),
        ColumnNumbers{1},
        std::vector<Field>{Field(static_cast<UInt64>(9))},
        ColumnNumbers{0});
    NamesWithAliases level_2_project_cols;
    level_2_project_cols.emplace_back(level_2_names[1], "test_int_col_1");
    level_2_project_cols.emplace_back(level_2_names[0], "test_int_col_2");

    // construct leveled-projections.
    ExpressionActionsPtrVec level_projections;
    level_projections.emplace_back(level_1_actions);
    level_projections.emplace_back(level_2_actions);
    NamesWithAliasesVec level_alias;
    level_alias.emplace_back(level_1_project_cols);
    level_alias.emplace_back(level_2_project_cols);

    // construct before-expand-actions.
    ExpressionActionsPtr before_expand_actions
        = buildChangeNullable(*context, source_block.getColumnsWithTypeAndName(), ColumnNumbers{0});

    auto expand2 = std::make_shared<Expand2>(level_projections, before_expand_actions, level_alias);

    // mock header.
    auto header = one_block_input_stream->getHeader();
    before_expand_actions->execute(header);
    ExpandBlockInputStream expand_block_input_stream(one_block_input_stream, expand2, header, "test_request_id");

    // read stream
    auto res1 = expand_block_input_stream.read();
    ASSERT_EQ(res1.rows(), 9);
    ASSERT_EQ(res1.columns(), 2);
    auto l1_column1 = res1.getColumnsWithTypeAndName()[0].column;
    ASSERT_EQ(l1_column1->isColumnNullable(), true);
    ASSERT_EQ(l1_column1->isNullAt(0), true);
    ASSERT_EQ(l1_column1->isNullAt(1), true);
    ASSERT_EQ(l1_column1->isNullAt(2), true);
    ASSERT_EQ(l1_column1->isNullAt(3), true);
    ASSERT_EQ(l1_column1->isNullAt(4), true);
    ASSERT_EQ(l1_column1->isNullAt(5), true);
    ASSERT_EQ(l1_column1->isNullAt(6), true);
    ASSERT_EQ(l1_column1->isNullAt(7), true);
    ASSERT_EQ(l1_column1->isNullAt(8), true);
    auto l1_column2 = res1.getColumnsWithTypeAndName()[1].column;
    ASSERT_EQ(l1_column2->isColumnNullable(), false);
    ASSERT_EQ(l1_column2->getUInt(0), 1);
    ASSERT_EQ(l1_column2->getUInt(1), 2);
    ASSERT_EQ(l1_column2->getUInt(2), 3);
    ASSERT_EQ(l1_column2->getUInt(3), 4);
    ASSERT_EQ(l1_column2->getUInt(4), 5);
    ASSERT_EQ(l1_column2->getUInt(5), 6);
    ASSERT_EQ(l1_column2->getUInt(6), 7);
    ASSERT_EQ(l1_column2->getUInt(7), 8);
    ASSERT_EQ(l1_column2->getUInt(8), 9);


    auto res2 = expand_block_input_stream.read();
    ASSERT_EQ(res2.rows(), 9);
    ASSERT_EQ(res2.columns(), 2);
    auto l2_column1 = res2.getColumnsWithTypeAndName()[0].column;
    ASSERT_EQ(l2_column1->isColumnNullable(), true);
    ASSERT_EQ(l2_column1->isNullAt(0), false);
    ASSERT_EQ(l2_column1->isNullAt(1), false);
    ASSERT_EQ(l2_column1->isNullAt(2), false);
    ASSERT_EQ(l2_column1->isNullAt(3), false);
    ASSERT_EQ(l2_column1->isNullAt(4), false);
    ASSERT_EQ(l2_column1->isNullAt(5), false);
    ASSERT_EQ(l2_column1->isNullAt(6), false);
    ASSERT_EQ(l2_column1->isNullAt(7), false);
    ASSERT_EQ(l2_column1->isNullAt(8), false);
    const auto * nullable_col = typeid_cast<const ColumnNullable *>(l2_column1.get());
    auto l2_nested_column1 = nullable_col->getNestedColumnPtr();
    ASSERT_EQ(l2_nested_column1->getInt(0), 1);
    ASSERT_EQ(l2_nested_column1->getInt(1), 2);
    ASSERT_EQ(l2_nested_column1->getInt(2), 3);
    ASSERT_EQ(l2_nested_column1->getInt(3), 4);
    ASSERT_EQ(l2_nested_column1->getInt(4), 5);
    ASSERT_EQ(l2_nested_column1->getInt(5), 6);
    ASSERT_EQ(l2_nested_column1->getInt(6), 7);
    ASSERT_EQ(l2_nested_column1->getInt(7), 8);
    ASSERT_EQ(l2_nested_column1->getInt(8), 9);

    auto l2_column2 = res2.getColumnsWithTypeAndName()[1].column;
    ASSERT_EQ(l2_column2->isColumnNullable(), false);
    ASSERT_EQ(l2_column2->getUInt(0), 9);
    ASSERT_EQ(l2_column2->getUInt(1), 9);
    ASSERT_EQ(l2_column2->getUInt(2), 9);
    ASSERT_EQ(l2_column2->getUInt(3), 9);
    ASSERT_EQ(l2_column2->getUInt(4), 9);
    ASSERT_EQ(l2_column2->getUInt(5), 9);
    ASSERT_EQ(l2_column2->getUInt(6), 9);
    ASSERT_EQ(l2_column2->getUInt(7), 9);
    ASSERT_EQ(l2_column2->getUInt(8), 9);

    auto res3 = expand_block_input_stream.read();
    ASSERT_EQ(res3.rows(), 0);
    ASSERT_EQ(res3.columns(), 0);
}
CATCH

} // namespace tests
} // namespace DB
