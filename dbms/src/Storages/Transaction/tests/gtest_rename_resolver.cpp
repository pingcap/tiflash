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

#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/SchemaBuilder-internal.h>
#include <TiDB/Schema/SchemaBuilder.h>

namespace DB::tests
{

TEST(CyclicRenameResolver_test, resolve_normal)
{
    using Resolver = CyclicRenameResolver<String, TmpColNameGenerator>;
    std::map<String, String> rename_map;
    rename_map["a"] = "aa";
    rename_map["b"] = "bb";

    typename Resolver::NamePairs rename_result = Resolver().resolve(std::move(rename_map));

    ASSERT_EQ(rename_result.size(), 2UL);
    // a -> aa
    ASSERT_EQ(rename_result[0].first, "a");
    ASSERT_EQ(rename_result[0].second, "aa");
    // b -> bb
    ASSERT_EQ(rename_result[1].first, "b");
    ASSERT_EQ(rename_result[1].second, "bb");
}

TEST(CyclicRenameResolver_test, resolve_linked)
{
    using Resolver = CyclicRenameResolver<String, TmpColNameGenerator>;
    std::map<String, String> rename_map;
    rename_map["a"] = "c";
    rename_map["b"] = "a";

    typename Resolver::NamePairs rename_result = Resolver().resolve(std::move(rename_map));

    ASSERT_EQ(rename_result.size(), 2UL);
    // a -> c
    ASSERT_EQ(rename_result[0].first, "a");
    ASSERT_EQ(rename_result[0].second, "c");
    // b -> a
    ASSERT_EQ(rename_result[1].first, "b");
    ASSERT_EQ(rename_result[1].second, "a");
}

TEST(CyclicRenameResolver_test, resolve_linked_2)
{
    using Resolver = CyclicRenameResolver<String, TmpColNameGenerator>;
    std::map<String, String> rename_map;
    rename_map["b"] = "c";
    rename_map["c"] = "d";

    typename Resolver::NamePairs rename_result = Resolver().resolve(std::move(rename_map));

    ASSERT_EQ(rename_result.size(), 2UL);
    // c -> d
    ASSERT_EQ(rename_result[0].first, "c");
    ASSERT_EQ(rename_result[0].second, "d");
    // b -> c
    ASSERT_EQ(rename_result[1].first, "b");
    ASSERT_EQ(rename_result[1].second, "c");
}

namespace
{
template <typename T>
bool isEqualPairs(const std::pair<T, T> & lhs, const std::pair<T, T> & rhs)
{
    return lhs == rhs;
}
} // namespace

TEST(CyclicRenameResolver_test, resolve_long_linked)
{
    using Resolver = CyclicRenameResolver<String, TmpColNameGenerator>;
    std::map<String, String> rename_map;
    rename_map["a"] = "b";
    rename_map["b"] = "c";
    rename_map["c"] = "d";
    rename_map["d"] = "e";
    rename_map["e"] = "z";
    rename_map["z"] = "h";

    typename Resolver::NamePairs rename_result = Resolver().resolve(std::move(rename_map));

    ASSERT_EQ(rename_result.size(), 6UL);
    ASSERT_TRUE(isEqualPairs(rename_result[0], std::make_pair(String("z"), String("h"))));
    ASSERT_TRUE(isEqualPairs(rename_result[1], std::make_pair(String("e"), String("z"))));
    ASSERT_TRUE(isEqualPairs(rename_result[2], std::make_pair(String("d"), String("e"))));
    ASSERT_TRUE(isEqualPairs(rename_result[3], std::make_pair(String("c"), String("d"))));
    ASSERT_TRUE(isEqualPairs(rename_result[4], std::make_pair(String("b"), String("c"))));
    ASSERT_TRUE(isEqualPairs(rename_result[5], std::make_pair(String("a"), String("b"))));
}

TEST(CyclicRenameResolver_test, resolve_simple_cycle)
{
    using Resolver = CyclicRenameResolver<String, TmpColNameGenerator>;
    std::map<String, String> rename_map;
    rename_map["a"] = "b";
    rename_map["b"] = "a";

    typename Resolver::NamePairs rename_result = Resolver().resolve(std::move(rename_map));

    TmpColNameGenerator generator;

    ASSERT_EQ(rename_result.size(), 3UL);
    // a -> tmp_a
    ASSERT_EQ(rename_result[0].first, "a");
    ASSERT_EQ(rename_result[0].second, generator("a"));
    // b -> a
    ASSERT_EQ(rename_result[1].first, "b");
    ASSERT_EQ(rename_result[1].second, "a");
    // tmp_a -> b
    ASSERT_EQ(rename_result[2].first, generator("a"));
    ASSERT_EQ(rename_result[2].second, "b");
}


inline ::testing::AssertionResult ColumnNameWithIDPairsCompare( //
    const char * lhs_expr,
    const char * rhs_expr,
    const std::pair<ColumnNameWithID, ColumnNameWithID> & lhs,
    const std::pair<ColumnNameWithID, ColumnNameWithID> & rhs)
{
    if (lhs.first.equals(rhs.first) && lhs.second.equals(rhs.second))
        return ::testing::AssertionSuccess();
    else
        return ::testing::internal::EqFailure(lhs_expr,
                                              rhs_expr,
                                              "<" + lhs.first.toString() + "," + lhs.second.toString() + ">",
                                              "<" + rhs.first.toString() + "," + rhs.second.toString() + ">",
                                              false);
}
#define ASSERT_COLUMN_NAME_ID_PAIR_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::tests::ColumnNameWithIDPairsCompare, val1, val2)

TEST(CyclicRenameResolver_test, resolve_id_simple_cycle)
{
    using Resolver = CyclicRenameResolver<ColumnNameWithID, TmpColNameWithIDGenerator>;
    std::map<ColumnNameWithID, ColumnNameWithID> rename_map;
    rename_map[ColumnNameWithID{"a", 1}] = ColumnNameWithID{"b", 1};
    rename_map[ColumnNameWithID{"b", 2}] = ColumnNameWithID{"a", 2};

    typename Resolver::NamePairs rename_result = Resolver().resolve(std::move(rename_map));

    TmpColNameWithIDGenerator generator;

    ASSERT_EQ(rename_result.size(), 3UL);
    // a -> tmp_a
    ASSERT_COLUMN_NAME_ID_PAIR_EQ(rename_result[0], std::make_pair(ColumnNameWithID{"a", 1L}, generator(ColumnNameWithID{"a", 1})));
    // b -> a
    ASSERT_COLUMN_NAME_ID_PAIR_EQ(rename_result[1], std::make_pair(ColumnNameWithID{"b", 2L}, ColumnNameWithID{"a", 2L}));
    // tmp_a -> b
    ASSERT_COLUMN_NAME_ID_PAIR_EQ(rename_result[2], std::make_pair(generator(ColumnNameWithID{"a", 1}), ColumnNameWithID{"b", 1}));
}

} // namespace DB::tests
