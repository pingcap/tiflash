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

#include <Common/MyTime.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Writer.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <array>

namespace DB::DM::tests
{
namespace
{
class InvertedIndexNotTest : public ::testing::Test
{
protected:
    const Attr attr{"n", 5, std::make_shared<DataTypeInt64>()};
    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> indexes;
    InvertedIndexReaderPtr reader;

    void SetUp() override
    {
        auto * info = indexes.Add();
        info->set_index_type(tipb::ColumnarIndexType::TypeInverted);
        info->mutable_inverted_query_info()->set_column_id(attr.col_id);
        info->mutable_inverted_query_info()->set_index_id(50);
        InvertedIndexWriterInMemory<Int64> writer(50);
        const auto column = DB::tests::createColumn<Int64>({1, 1, 2}).column;
        writer.addBlock(*column, nullptr, [] { return true; });
        WriteBufferFromOwnString buffer;
        writer.saveToBuffer(buffer);
        const auto data = buffer.releaseStr();
        ReadBufferFromString input(data);
        reader = std::make_shared<InvertedIndexMemoryReader<Int64>>(input, data.size());
    }

    BitmapFilterPtr check(const ColumnRangePtr & range)
    {
        return range->check(
            [&](const SingleColumnRangePtr & single) {
                EXPECT_EQ(single->column_id, attr.col_id);
                return single->set->search(reader, 3);
            },
            3);
    }

    BitmapFilterPtr check(const RSOperatorPtr & op) { return check(op->buildSets(indexes)); }

    RSOperatorPtr equals(Int64 value) { return createEqual(attr, Field(value)); }
};

TEST_F(InvertedIndexNotTest, NotAndUnsupported)
{
    const auto unknown = createUnsupported("not indexable");
    const auto condition = equals(1);
    for (const auto & children : {RSOperators{unknown, condition}, RSOperators{condition, unknown}})
    {
        // A can be false while n=1: NOT(A AND n=1) must retain those rows.
        auto bitmap = check(createNot(createAnd(children)));
        ASSERT_NE(bitmap, nullptr);
        EXPECT_EQ(bitmap->toDebugString(), "111");
    }
}

TEST_F(InvertedIndexNotTest, SupportedPredicatesStillPrune)
{
    const auto unknown = createUnsupported("not indexable");
    const auto condition = equals(1);
    EXPECT_EQ(check(createAnd({unknown, condition}))->toDebugString(), "110");
    EXPECT_EQ(check(createNot(condition))->toDebugString(), "001");
    const auto known_and = createAnd({condition, createLess(attr, Field(Int64(3)))});
    EXPECT_EQ(check(createNot(known_and))->toDebugString(), "001");
    EXPECT_EQ(check(createAnd({unknown, unknown}))->toDebugString(), "111");
    EXPECT_EQ(check(createNot(createAnd({unknown, unknown})))->toDebugString(), "111");
}

TEST_F(InvertedIndexNotTest, NestedExpressionsRetainMatchingRows)
{
    struct Expression
    {
        RSOperatorPtr op;
        std::array<bool, 3> truth;
    };
    const auto negate = [](const Expression & e) {
        return Expression{createNot(e.op), {!e.truth[0], !e.truth[1], !e.truth[2]}};
    };
    const auto combine = [](const Expression & a, const Expression & b, bool is_and) {
        Expression result{is_and ? createAnd({a.op, b.op}) : createOr({a.op, b.op}), {}};
        for (size_t row = 0; row < 3; ++row)
            result.truth[row] = is_and ? a.truth[row] && b.truth[row] : a.truth[row] || b.truth[row];
        return result;
    };
    const Expression b{equals(1), {true, true, false}};
    const Expression c{equals(2), {false, false, true}};
    for (unsigned mask = 0; mask < 8; ++mask)
    {
        const Expression a{createUnsupported("unknown predicate"), {bool(mask & 1), bool(mask & 2), bool(mask & 4)}};
        const std::vector<Expression> expressions{
            a,
            b,
            c,
            combine(a, b, true),
            combine(a, b, false),
            negate(combine(a, b, true)),
            negate(combine(a, b, false)),
            combine(combine(a, b, true), b, true), // Flattening and same-index merging.
            combine(combine(a, b, true), c, false),
            combine(negate(combine(a, b, true)), c, true),
            negate(negate(combine(a, b, true)))};
        for (const auto & left : expressions)
            for (const auto & right : expressions)
                for (bool is_and : {false, true})
                {
                    const auto combined = combine(left, right, is_and);
                    for (const auto & expr : {combined, negate(combined)})
                    {
                        SCOPED_TRACE(expr.op->toDebugString());
                        auto bitmap = check(expr.op);
                        ASSERT_NE(bitmap, nullptr);
                        for (size_t row = 0; row < 3; ++row)
                            if (expr.truth[row])
                                ASSERT_TRUE(bitmap->get(row)) << "mask=" << mask << " row=" << row;
                    }
                }
    }
}

TEST_F(InvertedIndexNotTest, SessionTimezoneNotAnd)
{
    const auto function = [](tipb::ScalarFuncSig sig, std::initializer_list<tipb::Expr> children) {
        tipb::Expr expr;
        expr.set_tp(tipb::ScalarFunc);
        expr.set_sig(sig);
        for (const auto & child : children)
            *expr.add_children() = child;
        return expr;
    };
    const auto column = [](int offset, int type) {
        tipb::Expr expr;
        expr.set_tp(tipb::ColumnRef);
        WriteBufferFromOwnString buffer;
        encodeDAGInt64(offset, buffer);
        expr.set_val(buffer.releaseStr());
        expr.mutable_field_type()->set_tp(type);
        return expr;
    };
    const UInt64 literal = MyDateTime(2026, 11, 1, 9, 0, 0, 0).toPackedUInt();
    const auto a = function(tipb::EQTime, {column(0, TiDB::TypeTimestamp), constructDateTimeLiteralTiExpr(literal)});
    const auto b = function(tipb::EQInt, {column(1, TiDB::TypeLongLong), constructInt64LiteralTiExpr(1)});
    google::protobuf::RepeatedPtrField<tipb::Expr> filters, pushed;
    *filters.Add() = function(tipb::UnaryNotInt, {function(tipb::LogicalAnd, {a, b})});
    const Attr ts{"ts", 4, std::make_shared<DataTypeMyDateTime>(6)};
    TiDB::ColumnInfos columns(2);
    columns[0].id = ts.col_id;
    columns[1].id = attr.col_id;
    auto * info = indexes.Add();
    info->set_index_type(tipb::ColumnarIndexType::TypeInverted);
    info->mutable_inverted_query_info()->set_column_id(ts.col_id);
    info->mutable_inverted_query_info()->set_index_id(40);
    TimezoneInfo timezone;
    timezone.resetByTimezoneOffset(8 * 3600);
    const tipb::ANNQueryInfo ann;
    const tipb::FTSQueryInfo fts;
    const std::vector<int> runtime_filters;
    DAGQueryInfo dag(filters, ann, fts, pushed, indexes, columns, runtime_filters, 0, timezone);
    auto op = FilterParser::parseDAGQuery(dag, columns, {{ts.col_id, ts}, {attr.col_id, attr}}, Logger::get());
    ASSERT_NE(op->toDebugString().find("session_timezone"), String::npos);

    // Row 0 has n=1 and UTC 02:00, which projects to 10:00 in this session.
    // Thus A=false, B=true, NOT(A AND B)=true: the index must retain row 0.
    UInt64 projected = 0;
    convertTimeZoneByOffset(MyDateTime(2026, 11, 1, 2, 0, 0, 0).toPackedUInt(), projected, true, 8 * 3600);
    ASSERT_NE(projected, literal);
    auto bitmap = check(op);
    ASSERT_NE(bitmap, nullptr);
    EXPECT_TRUE(bitmap->get(0));
    EXPECT_EQ(bitmap->toDebugString(), "111");
}
} // namespace
} // namespace DB::DM::tests
