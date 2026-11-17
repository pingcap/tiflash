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

namespace DB::DM::tests
{
TEST(ColumnRangeMergeTest, UnsupportedPredicateIsRetainedForInversion)
{
    const ColumnID stored_column_id = 10;
    const IndexID index_id = 1;
    Attr stored{.col_name = "a", .col_id = stored_column_id, .type = DB::tests::typeFromString("Int64")};

    google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> used_indexes;
    auto * inverted = used_indexes.Add();
    inverted->set_index_type(tipb::ColumnarIndexType::TypeInverted);
    inverted->mutable_inverted_query_info()->set_index_id(index_id);
    inverted->mutable_inverted_query_info()->set_column_id(stored_column_id);

    auto merged = createAnd({createEqual(stored, Field(Int64(1))), createUnsupported("not indexable")})
                      ->buildSets(used_indexes);
    size_t searches = 0;
    const auto search = [&](const SingleColumnRangePtr & single) {
        ++searches;
        EXPECT_EQ(single->column_id, stored_column_id);
        EXPECT_EQ(single->index_id, index_id);
        return std::make_shared<BitmapFilter>(std::initializer_list<UInt8>{1, 0});
    };
    // The indexed column still prunes, while the unsupported predicate is retained for inversion.
    EXPECT_EQ(merged->check(search, 2)->toDebugString(), "10");
    EXPECT_EQ(searches, 1);
    EXPECT_EQ(merged->invert()->check(search, 2)->toDebugString(), "11");
    EXPECT_EQ(searches, 1);
}
} // namespace DB::DM::tests
