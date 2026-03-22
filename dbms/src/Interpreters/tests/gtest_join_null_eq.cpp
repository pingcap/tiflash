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

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Join.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <gtest/gtest.h>

#include <mutex>
#include <optional>

namespace DB::tests
{
namespace
{
constexpr auto * test_key_name = "k";
constexpr auto * probe_eq_key_name = "eq_k";
constexpr auto * probe_null_eq_key_name = "null_eq_k";
constexpr auto * probe_filter_name = "filter";
constexpr auto * outer_probe_key_name = "probe_k";
constexpr auto * outer_probe_value_name = "probe_v";
constexpr auto * outer_probe_filter_name = "probe_filter";
constexpr auto * outer_build_key_name = "build_k";
constexpr auto * outer_build_value_name = "build_v";
constexpr auto * outer_build_filter_name = "build_filter";
constexpr auto * mixed_probe_key1_name = "probe_k1";
constexpr auto * mixed_probe_key2_name = "probe_k2";
constexpr auto * mixed_probe_value_name = "probe_multi_v";
constexpr auto * mixed_build_key1_name = "build_k1";
constexpr auto * mixed_build_key2_name = "build_k2";
constexpr auto * mixed_build_value_name = "build_multi_v";
constexpr auto * full_other_cond_name = "full_other_cond";
constexpr auto * full_flag_helper_name = "__full_flag_helper";

void ensureFunctionsRegistered()
{
    static std::once_flag once;
    std::call_once(once, [] {
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Another test suite may have already registered the functions.
        }
    });
}

Block makeSampleBlock(const DataTypePtr & key_type)
{
    return Block{{key_type->createColumn(), key_type, test_key_name}};
}

JoinPtr makeTestJoin(const DataTypePtr & key_type, const std::vector<UInt8> & is_null_eq)
{
    SpillConfig build_spill_config("/tmp", "join_null_eq_build", 0, 0, 0, nullptr);
    SpillConfig probe_spill_config("/tmp", "join_null_eq_probe", 0, 0, 0, nullptr);
    return std::make_shared<Join>(
        Names{test_key_name},
        Names{test_key_name},
        is_null_eq,
        ASTTableJoin::Kind::Inner,
        "join_null_eq_test",
        0,
        0,
        build_spill_config,
        probe_spill_config,
        RestoreConfig{1, 0, 0},
        NamesAndTypes{{test_key_name, key_type}},
        RegisterOperatorSpillContext{},
        nullptr,
        TiDB::TiDBCollators{},
        JoinNonEqualConditions{},
        1024,
        0,
        "",
        "",
        0,
        true);
}

JoinNonEqualConditions makeFullJoinOtherCondition()
{
    ensureFunctionsRegistered();

    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto actions = std::make_shared<ExpressionActions>(NamesAndTypes{
        {outer_probe_key_name, nullable_int_type},
        {outer_probe_value_name, nullable_int_type},
        {outer_build_key_name, nullable_int_type},
        {outer_build_value_name, nullable_int_type},
    });
    auto equals_builder = FunctionFactory::instance().get("equals", *TiFlashTestEnv::getContext());
    actions->add(ExpressionAction::applyFunction(
        equals_builder,
        {outer_probe_value_name, outer_build_value_name},
        full_other_cond_name));

    JoinNonEqualConditions conditions;
    conditions.other_cond_name = full_other_cond_name;
    conditions.other_cond_expr = actions;
    return conditions;
}

JoinNonEqualConditions makeOuterJoinSideConditions(
    const String & left_filter_column = "",
    const String & right_filter_column = "")
{
    JoinNonEqualConditions conditions;
    conditions.left_filter_column = left_filter_column;
    conditions.right_filter_column = right_filter_column;
    return conditions;
}

JoinPtr makeOuterJoinTestJoin(
    ASTTableJoin::Kind kind,
    const JoinNonEqualConditions & non_equal_conditions = JoinNonEqualConditions{},
    const String & flag_helper_name = "")
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto nullable_value_type = makeNullable(std::make_shared<DataTypeInt32>());
    SpillConfig build_spill_config("/tmp", "join_null_eq_build", 0, 0, 0, nullptr);
    SpillConfig probe_spill_config("/tmp", "join_null_eq_probe", 0, 0, 0, nullptr);
    return std::make_shared<Join>(
        Names{outer_probe_key_name},
        Names{outer_build_key_name},
        std::vector<UInt8>{1},
        kind,
        "join_null_eq_outer_test",
        0,
        0,
        build_spill_config,
        probe_spill_config,
        RestoreConfig{1, 0, 0},
        NamesAndTypes{
            {outer_probe_key_name, nullable_int_type},
            {outer_probe_value_name, nullable_value_type},
            {outer_build_key_name, nullable_int_type},
            {outer_build_value_name, nullable_value_type},
        },
        RegisterOperatorSpillContext{},
        nullptr,
        TiDB::TiDBCollators{},
        non_equal_conditions,
        1024,
        0,
        "",
        flag_helper_name,
        0,
        true);
}

JoinPtr makeSemiJoinTestJoin(ASTTableJoin::Kind kind)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    SpillConfig build_spill_config("/tmp", "join_null_eq_build", 0, 0, 0, nullptr);
    SpillConfig probe_spill_config("/tmp", "join_null_eq_probe", 0, 0, 0, nullptr);
    return std::make_shared<Join>(
        Names{outer_probe_key_name},
        Names{outer_build_key_name},
        std::vector<UInt8>{1},
        kind,
        "join_null_eq_semi_test",
        0,
        0,
        build_spill_config,
        probe_spill_config,
        RestoreConfig{1, 0, 0},
        NamesAndTypes{
            {outer_probe_key_name, nullable_int_type},
            {outer_probe_value_name, int_type},
        },
        RegisterOperatorSpillContext{},
        nullptr,
        TiDB::TiDBCollators{},
        JoinNonEqualConditions{},
        1024,
        0,
        "",
        "",
        0,
        true);
}

JoinPtr makeMixedKeyJoin(const std::vector<UInt8> & is_null_eq, const DataTypePtr & key_type)
{
    auto int_type = std::make_shared<DataTypeInt32>();
    SpillConfig build_spill_config("/tmp", "join_null_eq_build", 0, 0, 0, nullptr);
    SpillConfig probe_spill_config("/tmp", "join_null_eq_probe", 0, 0, 0, nullptr);
    return std::make_shared<Join>(
        Names{mixed_probe_key1_name, mixed_probe_key2_name},
        Names{mixed_build_key1_name, mixed_build_key2_name},
        is_null_eq,
        ASTTableJoin::Kind::Inner,
        "join_null_eq_mixed_test",
        0,
        0,
        build_spill_config,
        probe_spill_config,
        RestoreConfig{1, 0, 0},
        NamesAndTypes{
            {mixed_probe_key1_name, key_type},
            {mixed_probe_key2_name, key_type},
            {mixed_probe_value_name, int_type},
            {mixed_build_key1_name, key_type},
            {mixed_build_key2_name, key_type},
            {mixed_build_value_name, int_type},
        },
        RegisterOperatorSpillContext{},
        nullptr,
        TiDB::TiDBCollators{},
        JoinNonEqualConditions{},
        1024,
        0,
        "",
        "",
        0,
        true);
}

JoinPtr makeMixedKeyJoin(const std::vector<UInt8> & is_null_eq)
{
    return makeMixedKeyJoin(is_null_eq, makeNullable(std::make_shared<DataTypeInt32>()));
}

Block makeOuterProbeSampleBlock(bool include_filter = false)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto block = Block{
        {nullable_int_type->createColumn(), nullable_int_type, outer_probe_key_name},
        {int_type->createColumn(), int_type, outer_probe_value_name},
    };
    if (include_filter)
    {
        auto uint8_type = std::make_shared<DataTypeUInt8>();
        block.insert({uint8_type->createColumn(), uint8_type, outer_probe_filter_name});
    }
    return block;
}

Block makeOuterBuildSampleBlock(bool include_filter = false)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto block = Block{
        {nullable_int_type->createColumn(), nullable_int_type, outer_build_key_name},
        {int_type->createColumn(), int_type, outer_build_value_name},
    };
    if (include_filter)
    {
        auto uint8_type = std::make_shared<DataTypeUInt8>();
        block.insert({uint8_type->createColumn(), uint8_type, outer_build_filter_name});
    }
    return block;
}

Block makeMixedProbeSampleBlock(const DataTypePtr & key_type)
{
    auto int_type = std::make_shared<DataTypeInt32>();
    return Block{
        {key_type->createColumn(), key_type, mixed_probe_key1_name},
        {key_type->createColumn(), key_type, mixed_probe_key2_name},
        {int_type->createColumn(), int_type, mixed_probe_value_name},
    };
}

Block makeMixedProbeSampleBlock()
{
    return makeMixedProbeSampleBlock(makeNullable(std::make_shared<DataTypeInt32>()));
}

Block makeMixedBuildSampleBlock(const DataTypePtr & key_type)
{
    auto int_type = std::make_shared<DataTypeInt32>();
    return Block{
        {key_type->createColumn(), key_type, mixed_build_key1_name},
        {key_type->createColumn(), key_type, mixed_build_key2_name},
        {int_type->createColumn(), int_type, mixed_build_value_name},
    };
}

Block makeMixedBuildSampleBlock()
{
    return makeMixedBuildSampleBlock(makeNullable(std::make_shared<DataTypeInt32>()));
}

ColumnPtr makeNullableInt32Column(std::initializer_list<std::optional<Int32>> values)
{
    auto nested = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    nested->reserve(values.size());
    null_map->reserve(values.size());
    auto & nested_data = nested->getData();
    auto & null_map_data = null_map->getData();
    for (const auto & value : values)
    {
        if (value.has_value())
        {
            nested_data.push_back(*value);
            null_map_data.push_back(0);
        }
        else
        {
            nested_data.push_back(0);
            null_map_data.push_back(1);
        }
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnPtr makeUInt8Column(std::initializer_list<UInt8> values)
{
    auto column = ColumnUInt8::create();
    column->reserve(values.size());
    auto & data = column->getData();
    for (auto value : values)
        data.push_back(value);
    return column;
}

ColumnPtr makeInt32Column(std::initializer_list<Int32> values)
{
    auto column = ColumnInt32::create();
    column->reserve(values.size());
    auto & data = column->getData();
    for (auto value : values)
        data.push_back(value);
    return column;
}

Block readAllBlocks(const BlockInputStreamPtr & stream)
{
    stream->readPrefix();
    Blocks blocks;
    while (true)
    {
        auto block = stream->read();
        if (!block)
            break;
        blocks.push_back(std::move(block));
    }
    stream->readSuffix();
    if (blocks.empty())
        return stream->getHeader().cloneEmpty();
    return vstackBlocks(std::move(blocks));
}

std::optional<Int32> getInt32Value(const Block & block, const String & name, size_t row)
{
    const auto & column = block.getByName(name).column;
    if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(column.get()); nullable_column != nullptr)
    {
        if (nullable_column->getNullMapData()[row] != 0)
            return std::nullopt;
        return checkAndGetColumn<ColumnInt32>(nullable_column->getNestedColumnPtr().get())->getData()[row];
    }
    return checkAndGetColumn<ColumnInt32>(column.get())->getData()[row];
}

void prepareAndFinalizeOuterJoin(
    const JoinPtr & join,
    bool include_probe_filter = false,
    bool include_build_filter = false)
{
    join->initBuild(makeOuterBuildSampleBlock(include_build_filter), 1);
    join->initProbe(makeOuterProbeSampleBlock(include_probe_filter), 1);
    join->finalize(Names{
        outer_probe_key_name,
        outer_probe_value_name,
        outer_build_key_name,
        outer_build_value_name,
    });
}

void prepareAndFinalizeSemiJoin(const JoinPtr & join)
{
    join->initBuild(makeOuterBuildSampleBlock(), 1);
    join->initProbe(makeOuterProbeSampleBlock(), 1);
    join->finalize(Names{
        outer_probe_key_name,
        outer_probe_value_name,
    });
}

void prepareAndFinalizeMixedJoin(const JoinPtr & join)
{
    join->initBuild(makeMixedBuildSampleBlock(), 1);
    join->initProbe(makeMixedProbeSampleBlock(), 1);
    join->finalize(Names{
        mixed_probe_key1_name,
        mixed_probe_key2_name,
        mixed_probe_value_name,
        mixed_build_key1_name,
        mixed_build_key2_name,
        mixed_build_value_name,
    });
}
} // namespace

TEST(JoinNullEqTest, NullableNullEqKeyUsesNullablePackedJoinMapMethod)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto join = makeTestJoin(nullable_int_type, {1});
    join->initBuild(makeSampleBlock(nullable_int_type), 1);

    ASSERT_EQ(join->getJoinMapMethod(), JoinMapMethod::nullable_keys128);
}

TEST(JoinNullEqTest, NullableMixedNullEqKeysCanUseNullableKeys256JoinMapMethod)
{
    auto nullable_int64_type = makeNullable(std::make_shared<DataTypeInt64>());
    auto join = makeMixedKeyJoin({1, 1}, nullable_int64_type);
    join->initBuild(makeMixedBuildSampleBlock(nullable_int64_type), 1);

    ASSERT_EQ(join->getJoinMapMethod(), JoinMapMethod::nullable_keys256);
}

TEST(JoinNullEqTest, DefaultMethodSelectionRemainsForOtherCases)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto nullable_non_null_eq_join = makeTestJoin(nullable_int_type, {0});
    nullable_non_null_eq_join->initBuild(makeSampleBlock(nullable_int_type), 1);
    ASSERT_EQ(nullable_non_null_eq_join->getJoinMapMethod(), JoinMapMethod::key32);

    auto int_type = std::make_shared<DataTypeInt32>();
    auto non_nullable_null_eq_join = makeTestJoin(int_type, {1});
    non_nullable_null_eq_join->initBuild(makeSampleBlock(int_type), 1);
    ASSERT_EQ(non_nullable_null_eq_join->getJoinMapMethod(), JoinMapMethod::key32);
}

TEST(JoinNullEqTest, NullableNullEqBuildRowsAreInsertedIntoHashMap)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto join = makeTestJoin(nullable_int_type, {1});
    join->initBuild(makeSampleBlock(nullable_int_type), 1);

    Block build_block{
        {makeNullableInt32Column({std::nullopt, 7}), nullable_int_type, test_key_name},
    };
    join->insertFromBlock(build_block, 0);

    ASSERT_EQ(join->getTotalRowCount(), 2);
}

TEST(JoinNullEqTest, ProbeRowFilterSkipsOnlyNonNullEqNullKeys)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto uint8_type = std::make_shared<DataTypeUInt8>();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({1, std::nullopt, 2}), nullable_int_type, probe_eq_key_name},
        {makeNullableInt32Column({std::nullopt, 10, 20}), nullable_int_type, probe_null_eq_key_name},
        {makeUInt8Column({1, 1, 0}), uint8_type, probe_filter_name},
    });

    probe_process_info.prepareForHashProbe(
        Names{probe_eq_key_name, probe_null_eq_key_name},
        std::vector<UInt8>{0, 1},
        probe_filter_name,
        ASTTableJoin::Kind::Inner,
        ASTTableJoin::Strictness::All,
        false,
        TiDB::TiDBCollators{},
        0);

    ASSERT_NE(probe_process_info.row_filter_map, nullptr);
    ASSERT_FALSE(probe_process_info.hash_join_data->key_columns[0]->isColumnNullable());
    ASSERT_TRUE(probe_process_info.hash_join_data->key_columns[1]->isColumnNullable());
    EXPECT_EQ((*probe_process_info.row_filter_map)[0], 0);
    EXPECT_EQ((*probe_process_info.row_filter_map)[1], 1);
    EXPECT_EQ((*probe_process_info.row_filter_map)[2], 1);
}

TEST(JoinNullEqTest, InnerJoinNullEqNullMatchProducesJoinedRow)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeOuterJoinTestJoin(ASTTableJoin::Kind::Inner);
    prepareAndFinalizeOuterJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 0), 100);
}

TEST(JoinNullEqTest, InnerJoinNullEqNullProbeRowDoesNotMatchNonNullBuildRow)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeOuterJoinTestJoin(ASTTableJoin::Kind::Inner);
    prepareAndFinalizeOuterJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({7}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    EXPECT_EQ(probe_result.rows(), 0);
}

TEST(JoinNullEqTest, LeftOuterNullEqNullMatchDoesNotBecomeUnmatched)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeOuterJoinTestJoin(ASTTableJoin::Kind::LeftOuter);
    prepareAndFinalizeOuterJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 0), 100);
}

TEST(JoinNullEqTest, LeftOuterNullEqNullProbeRowStaysUnmatchedAgainstNonNullBuildRow)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeOuterJoinTestJoin(ASTTableJoin::Kind::LeftOuter);
    prepareAndFinalizeOuterJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({7}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 0), std::nullopt);
}

TEST(JoinNullEqTest, LeftOuterNullEqLeftConditionOnlyFiltersConditionFailure)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto uint8_type = std::make_shared<DataTypeUInt8>();
    auto join = makeOuterJoinTestJoin(
        ASTTableJoin::Kind::LeftOuter,
        makeOuterJoinSideConditions(outer_probe_filter_name, ""));
    prepareAndFinalizeOuterJoin(join, true, false);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt, std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10, 20}), int_type, outer_probe_value_name},
        {makeUInt8Column({1, 0}), uint8_type, outer_probe_filter_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 2);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 0), 100);

    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 1), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 1), 20);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 1), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 1), std::nullopt);
}

TEST(JoinNullEqTest, SemiJoinNullEqKeepsOnlyProbeRowsThatMatch)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeSemiJoinTestJoin(ASTTableJoin::Kind::Semi);
    prepareAndFinalizeSemiJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt, 7}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100, 200}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt, 7, 8}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10, 20, 30}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 2);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 1), 7);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 1), 20);
}

TEST(JoinNullEqTest, AntiJoinNullEqKeepsOnlyProbeRowsThatDoNotMatch)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeSemiJoinTestJoin(ASTTableJoin::Kind::Anti);
    prepareAndFinalizeSemiJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt, 7}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100, 200}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt, 7, 8}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10, 20, 30}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), 8);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 30);
}

TEST(JoinNullEqTest, MixedJoinKeysNullEqThenEqOnlyFirstKeyIsNullSafe)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeMixedKeyJoin({1, 0});
    prepareAndFinalizeMixedJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt, std::nullopt}), nullable_int_type, mixed_build_key1_name},
            {makeNullableInt32Column({1, std::nullopt}), nullable_int_type, mixed_build_key2_name},
            {makeInt32Column({100, 200}), int_type, mixed_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt, std::nullopt}), nullable_int_type, mixed_probe_key1_name},
        {makeNullableInt32Column({1, std::nullopt}), nullable_int_type, mixed_probe_key2_name},
        {makeInt32Column({10, 20}), int_type, mixed_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, mixed_probe_key1_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, mixed_probe_key2_name, 0), 1);
    EXPECT_EQ(getInt32Value(probe_result, mixed_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, mixed_build_key1_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, mixed_build_key2_name, 0), 1);
    EXPECT_EQ(getInt32Value(probe_result, mixed_build_value_name, 0), 100);
}

TEST(JoinNullEqTest, MixedJoinKeysAllNullEqAllowAllNullPairToMatch)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeMixedKeyJoin({1, 1});
    prepareAndFinalizeMixedJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, mixed_build_key1_name},
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, mixed_build_key2_name},
            {makeInt32Column({100}), int_type, mixed_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, mixed_probe_key1_name},
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, mixed_probe_key2_name},
        {makeInt32Column({10}), int_type, mixed_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, mixed_probe_key1_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, mixed_probe_key2_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, mixed_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, mixed_build_key1_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, mixed_build_key2_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, mixed_build_value_name, 0), 100);
}

TEST(JoinNullEqTest, MixedJoinKeysEqThenNullEqOnlySecondKeyIsNullSafe)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeMixedKeyJoin({0, 1});
    prepareAndFinalizeMixedJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({7, std::nullopt}), nullable_int_type, mixed_build_key1_name},
            {makeNullableInt32Column({std::nullopt, std::nullopt}), nullable_int_type, mixed_build_key2_name},
            {makeInt32Column({100, 200}), int_type, mixed_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({7, std::nullopt}), nullable_int_type, mixed_probe_key1_name},
        {makeNullableInt32Column({std::nullopt, std::nullopt}), nullable_int_type, mixed_probe_key2_name},
        {makeInt32Column({10, 20}), int_type, mixed_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, mixed_probe_key1_name, 0), 7);
    EXPECT_EQ(getInt32Value(probe_result, mixed_probe_key2_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, mixed_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, mixed_build_key1_name, 0), 7);
    EXPECT_EQ(getInt32Value(probe_result, mixed_build_key2_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, mixed_build_value_name, 0), 100);
}

TEST(JoinNullEqTest, RightOuterNullEqNullMatchDoesNotLeakToScanAfterProbe)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeOuterJoinTestJoin(ASTTableJoin::Kind::RightOuter);
    prepareAndFinalizeOuterJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 0), 100);

    ASSERT_TRUE(join->finishOneProbe(0));
    join->finalizeProbe();

    Block scan_result = readAllBlocks(join->createScanHashMapAfterProbeStream(makeOuterProbeSampleBlock(), 0, 1, 1024));
    EXPECT_EQ(scan_result.rows(), 0);
}

TEST(JoinNullEqTest, RightOuterNullEqUnmatchedNullBuildRowStillScansFromHashMap)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeOuterJoinTestJoin(ASTTableJoin::Kind::RightOuter);
    prepareAndFinalizeOuterJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ASSERT_TRUE(join->finishOneProbe(0));
    join->finalizeProbe();

    Block scan_result = readAllBlocks(join->createScanHashMapAfterProbeStream(makeOuterProbeSampleBlock(), 0, 1, 1024));
    ASSERT_EQ(scan_result.rows(), 1);
    EXPECT_EQ(getInt32Value(scan_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(scan_result, outer_probe_value_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(scan_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(scan_result, outer_build_value_name, 0), 100);
}

TEST(JoinNullEqTest, RightOuterNullEqRightConditionFilteredBuildRowStillScansAfterProbe)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto uint8_type = std::make_shared<DataTypeUInt8>();
    auto join = makeOuterJoinTestJoin(
        ASTTableJoin::Kind::RightOuter,
        makeOuterJoinSideConditions("", outer_build_filter_name));
    prepareAndFinalizeOuterJoin(join, false, true);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt, std::nullopt}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100, 200}), int_type, outer_build_value_name},
            {makeUInt8Column({1, 0}), uint8_type, outer_build_filter_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 0), 100);

    ASSERT_TRUE(join->finishOneProbe(0));
    join->finalizeProbe();

    Block scan_result = readAllBlocks(join->createScanHashMapAfterProbeStream(makeOuterProbeSampleBlock(), 0, 1, 1024));
    ASSERT_EQ(scan_result.rows(), 1);
    EXPECT_EQ(getInt32Value(scan_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(scan_result, outer_probe_value_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(scan_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(scan_result, outer_build_value_name, 0), 200);
}

TEST(JoinNullEqTest, FullJoinNullEqNullMatchDoesNotSplitIntoTwoUnmatchedRows)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeOuterJoinTestJoin(ASTTableJoin::Kind::Full);
    prepareAndFinalizeOuterJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 0), 100);

    ASSERT_TRUE(join->finishOneProbe(0));
    join->finalizeProbe();

    Block scan_result = readAllBlocks(join->createScanHashMapAfterProbeStream(makeOuterProbeSampleBlock(), 0, 1, 1024));
    EXPECT_EQ(scan_result.rows(), 0);
}

TEST(JoinNullEqTest, FullJoinNullEqMatchWithOtherConditionFalseKeepsBuildRowForScanAfterProbe)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeOuterJoinTestJoin(ASTTableJoin::Kind::Full, makeFullJoinOtherCondition(), full_flag_helper_name);
    prepareAndFinalizeOuterJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({10}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 10);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 0), std::nullopt);

    ASSERT_TRUE(join->finishOneProbe(0));
    join->finalizeProbe();

    Block scan_result = readAllBlocks(join->createScanHashMapAfterProbeStream(makeOuterProbeSampleBlock(), 0, 1, 1024));
    ASSERT_EQ(scan_result.rows(), 1);
    EXPECT_EQ(getInt32Value(scan_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(scan_result, outer_probe_value_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(scan_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(scan_result, outer_build_value_name, 0), 100);
}

TEST(JoinNullEqTest, FullJoinNullEqMatchWithOtherConditionTrueConsumesBuildRow)
{
    auto nullable_int_type = makeNullable(std::make_shared<DataTypeInt32>());
    auto int_type = std::make_shared<DataTypeInt32>();
    auto join = makeOuterJoinTestJoin(ASTTableJoin::Kind::Full, makeFullJoinOtherCondition(), full_flag_helper_name);
    prepareAndFinalizeOuterJoin(join);

    join->setInitActiveBuildThreads();
    join->insertFromBlock(
        Block{
            {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_build_key_name},
            {makeInt32Column({100}), int_type, outer_build_value_name},
        },
        0);
    ASSERT_TRUE(join->finishOneBuild(0));
    join->finalizeBuild();

    ProbeProcessInfo probe_process_info(1024, 0);
    probe_process_info.resetBlock(Block{
        {makeNullableInt32Column({std::nullopt}), nullable_int_type, outer_probe_key_name},
        {makeInt32Column({100}), int_type, outer_probe_value_name},
    });
    Block probe_result = join->joinBlock(probe_process_info);

    ASSERT_EQ(probe_result.rows(), 1);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_probe_value_name, 0), 100);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_key_name, 0), std::nullopt);
    EXPECT_EQ(getInt32Value(probe_result, outer_build_value_name, 0), 100);

    ASSERT_TRUE(join->finishOneProbe(0));
    join->finalizeProbe();

    Block scan_result = readAllBlocks(join->createScanHashMapAfterProbeStream(makeOuterProbeSampleBlock(), 0, 1, 1024));
    EXPECT_EQ(scan_result.rows(), 0);
}

} // namespace DB::tests
