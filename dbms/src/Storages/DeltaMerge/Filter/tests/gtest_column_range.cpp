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

#include <Storages/DeltaMerge/Filter/ColumnRange.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::DM::tests
{
namespace
{
ColumnRangePtr valueRange(ColumnID column_id, IndexID index_id, const String & type_name)
{
    auto set = IntegerSet::createValueSet(DB::tests::typeFromString(type_name), {Field(static_cast<Int64>(1))});
    RUNTIME_CHECK(set);
    return SingleColumnRange::create(column_id, index_id, set);
}

void expectColumnMismatch(const std::function<ColumnRangePtr()> & build)
{
    try
    {
        build();
        FAIL() << "different columns sharing one index_id should be rejected";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS) << e.message();
    }
}
} // namespace

TEST(ColumnRangeMergeTest, SameColumnRangesStillMerge)
{
    const ColumnID column_id = 10;
    const IndexID index_id = 1;
    const auto type = DB::tests::typeFromString("Int64");
    auto greater = IntegerSet::createGreaterRangeSet(type, Field(static_cast<Int64>(1)), /*not_included=*/true);
    auto less = IntegerSet::createLessRangeSet(type, Field(static_cast<Int64>(10)), /*not_included=*/true);
    RUNTIME_CHECK(greater && less);

    auto merged = AndColumnRange::create(
        {SingleColumnRange::create(column_id, index_id, greater),
         SingleColumnRange::create(column_id, index_id, less)});
    const auto * single = dynamic_cast<const SingleColumnRange *>(merged.get());
    ASSERT_NE(single, nullptr);
    ASSERT_EQ(single->column_id, column_id);
    ASSERT_EQ(single->index_id, index_id);

    auto unioned = OrColumnRange::create(
        {SingleColumnRange::create(column_id, index_id, greater),
         SingleColumnRange::create(column_id, index_id, less)});
    const auto * union_single = dynamic_cast<const SingleColumnRange *>(unioned.get());
    ASSERT_NE(union_single, nullptr);
    ASSERT_EQ(union_single->column_id, column_id);
    ASSERT_EQ(union_single->index_id, index_id);
}

TEST(ColumnRangeMergeTest, RejectsDifferentColumnsSharingIndexId)
{
    const IndexID index_id = 7;
    expectColumnMismatch([&] {
        return AndColumnRange::create({valueRange(1, index_id, "Int8"), valueRange(2, index_id, "Int64")});
    });
    expectColumnMismatch([&] {
        return OrColumnRange::create({valueRange(1, index_id, "Int8"), valueRange(2, index_id, "Int64")});
    });
    expectColumnMismatch([&] {
        return AndColumnRange::create({valueRange(1, index_id, "Int64"), valueRange(2, index_id, "Int64")});
    });
    expectColumnMismatch([&] {
        return OrColumnRange::create({valueRange(1, index_id, "Int64"), valueRange(2, index_id, "Int64")});
    });
}

// Creating an inverted index on a hidden column is not supported today, and inverted
// indexes themselves are not a publicly supported feature. This test covers the
// null-type path only as defensive code.
TEST(ColumnRangeMergeTest, NullColumnTypeDoesNotBuildIntegerSet)
{
    const Field value(static_cast<Int64>(1));
    ASSERT_EQ(IntegerSet::createValueSet(nullptr, {value}), nullptr);
    ASSERT_EQ(IntegerSet::createLessRangeSet(nullptr, value, /*not_included=*/true), nullptr);
    ASSERT_EQ(IntegerSet::createGreaterRangeSet(nullptr, value, /*not_included=*/true), nullptr);

    const ColumnID stored_column_id = 10;
    const IndexID index_id = 1;
    Attr stored{.col_name = "a", .col_id = stored_column_id, .type = DB::tests::typeFromString("Int64")};
    Attr missing{.col_name = "", .col_id = -3, .type = nullptr};

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    auto * inverted = used_indexes.Add();
    inverted->set_index_type(tipb::ColumnarIndexType::TypeInverted);
    inverted->mutable_inverted_query_info()->set_index_id(index_id);
    inverted->mutable_inverted_query_info()->set_column_id(stored_column_id);

    auto missing_range = createEqual(missing, value)->buildSets(used_indexes);
    ASSERT_EQ(missing_range->type, ColumnRangeType::Unsupported);

    auto merged = createAnd({createEqual(stored, value), createEqual(missing, value)})->buildSets(used_indexes);
    const auto * single = dynamic_cast<const SingleColumnRange *>(merged.get());
    ASSERT_NE(single, nullptr);
    ASSERT_EQ(single->column_id, stored_column_id);
    ASSERT_EQ(single->index_id, index_id);
}
} // namespace DB::DM::tests
