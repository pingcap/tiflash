#include <test_utils/TiflashTestBasic.h>

#include <Storages/Transaction/SchemaBuilder-internal.h>
#include <Storages/Transaction/SchemaBuilder.h>

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
    ASSERT_EQ(rename_result[0].first, ColumnNameWithID("a", 1L));
    ASSERT_EQ(rename_result[0].second, generator(ColumnNameWithID{"a", 1}));
    // b -> a
    ASSERT_EQ(rename_result[1].first, ColumnNameWithID("b", 2L));
    ASSERT_EQ(rename_result[1].second, ColumnNameWithID("a", 2L));
    // tmp_a -> b
    ASSERT_EQ(rename_result[2].first, generator(ColumnNameWithID{"a", 1}));
    ASSERT_EQ(rename_result[2].second, ColumnNameWithID("b", 1));
}

} // namespace DB
