// Copyright 2026 PingCAP, Inc.
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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TiDB/Schema/TiDB.h>

#include <limits>

namespace DB::tests
{
namespace
{
template <typename F>
void expectBadRequest(F && action)
{
    try
    {
        action();
        FAIL() << "Expected a malformed request to be rejected";
    }
    catch (const TiFlashException & e)
    {
        EXPECT_TRUE(e.getError().is(Errors::Coprocessor::BadRequest)) << e.standardText();
    }
}
} // namespace

class DAGExpressionAnalyzerTest : public FunctionTest
{
};

TEST_F(DAGExpressionAnalyzerTest, RejectsFinalProjectionOffsetsOutsideChildSchema)
{
    tipb::FieldType field_type;
    field_type.set_tp(TiDB::TypeLongLong);
    field_type.set_flag(TiDB::ColumnFlagNotNull);
    const std::vector<tipb::FieldType> schema(2, field_type);
    const std::vector<std::vector<Int32>>
        invalid_offsets{{-1}, {std::numeric_limits<Int32>::min()}, {1}, {std::numeric_limits<Int32>::max()}, {0, 1}};
    for (const auto & output_offsets : invalid_offsets)
    {
        SCOPED_TRACE(output_offsets.back());
        Block block({createColumn<Int64>({1}, "c0")});
        DAGExpressionAnalyzer analyzer(block, *context);
        auto actions = std::make_shared<ExpressionActions>(block.getColumnsWithTypeAndName());
        expectBadRequest([&] { analyzer.buildFinalProjection(actions, schema, output_offsets, "", true); });
    }
}

TEST_F(DAGExpressionAnalyzerTest, RejectsFinalProjectionOffsetsOutsideRequiredSchema)
{
    tipb::FieldType field_type;
    field_type.set_tp(TiDB::TypeLongLong);
    field_type.set_flag(TiDB::ColumnFlagNotNull);
    for (size_t schema_size : {0, 1})
    {
        SCOPED_TRACE(schema_size);
        const std::vector<tipb::FieldType> schema(schema_size, field_type);
        Block block({createColumn<Int64>({1}, "c0"), createColumn<Int64>({2}, "c1")});
        DAGExpressionAnalyzer analyzer(block, *context);
        auto actions = std::make_shared<ExpressionActions>(block.getColumnsWithTypeAndName());
        expectBadRequest(
            [&] { analyzer.buildFinalProjection(actions, schema, {static_cast<Int32>(schema_size)}, "", true); });
    }
}

TEST_F(DAGExpressionAnalyzerTest, FinalProjectionPreservesOutputOffsets)
{
    const std::vector<std::vector<Int32>> valid_offsets{{1}, {1, 0}, {1, 0, 1}};
    for (const auto & output_offsets : valid_offsets)
    {
        SCOPED_TRACE(output_offsets.size());
        for (bool need_cast : {false, true})
        {
            SCOPED_TRACE(need_cast);
            tipb::FieldType field_type;
            field_type.set_tp(need_cast ? TiDB::TypeDouble : TiDB::TypeLongLong);
            field_type.set_flag(TiDB::ColumnFlagNotNull);
            const std::vector<tipb::FieldType> schema(2, field_type);
            Block block({createColumn<Int64>({10, 11}, "c0"), createColumn<Int64>({20, 21}, "c1")});
            DAGExpressionAnalyzer analyzer(block, *context);
            auto actions = std::make_shared<ExpressionActions>(block.getColumnsWithTypeAndName());
            auto aliases = analyzer.buildFinalProjection(actions, schema, output_offsets, "final_", true);
            actions->add(ExpressionAction::project(aliases));
            actions->execute(block);

            ASSERT_EQ(block.columns(), output_offsets.size());
            for (size_t i = 0; i < output_offsets.size(); ++i)
            {
                const Int64 value = (output_offsets[i] + 1) * 10;
                if (need_cast)
                    ASSERT_COLUMN_EQ(
                        createColumn<Float64>({static_cast<Float64>(value), static_cast<Float64>(value + 1)}),
                        block.getByPosition(i));
                else
                    ASSERT_COLUMN_EQ(createColumn<Int64>({value, value + 1}), block.getByPosition(i));
            }
        }
    }
}

} // namespace DB::tests
