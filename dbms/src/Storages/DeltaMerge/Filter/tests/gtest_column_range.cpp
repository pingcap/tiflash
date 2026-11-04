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
} // namespace DB::DM::tests
