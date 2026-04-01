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

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <TestUtils/ColumnsToTiPBExpr.h>
#include <TestUtils/ExecutorTestUtils.h>

#include <Flash/Coprocessor/JoinInterpreterHelper.cpp>
#include <tuple>

namespace DB
{
namespace tests
{
class JoinKindAndBuildIndexTestRunner : public testing::Test
{
};

namespace
{
tipb::Expr makeJoinKeyWithFieldType()
{
    tipb::Expr expr;
    expr.mutable_field_type()->set_tp(TiDB::TypeLong);
    return expr;
}

tipb::Join makeFullOuterJoinForSchemaTest(size_t inner_index)
{
    tipb::Join join;
    join.set_join_type(tipb::JoinType::TypeFullOuterJoin);
    join.set_inner_idx(inner_index);
    *join.add_left_join_keys() = makeJoinKeyWithFieldType();
    *join.add_right_join_keys() = makeJoinKeyWithFieldType();
    return join;
}

tipb::Join makeNullAwareJoinWithNullEq()
{
    tipb::Join join;
    join.set_join_type(tipb::JoinType::TypeAntiSemiJoin);
    join.set_inner_idx(1);
    join.set_is_null_aware_semi_join(true);
    *join.add_left_join_keys() = makeJoinKeyWithFieldType();
    *join.add_right_join_keys() = makeJoinKeyWithFieldType();
    join.add_is_null_eq(true);
    return join;
}
} // namespace

bool invalidParams(tipb::JoinType tipb_join_type, size_t inner_index, bool is_null_aware, size_t join_keys_size)
{
    try
    {
        JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb_join_type, inner_index, is_null_aware, join_keys_size);
        return false;
    }
    catch (Exception & e)
    {
        return true;
    }
}

String getErrorMessage(tipb::JoinType tipb_join_type, size_t inner_index, bool is_null_aware, size_t join_keys_size)
{
    try
    {
        JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb_join_type, inner_index, is_null_aware, join_keys_size);
        return "";
    }
    catch (Exception & e)
    {
        return e.message();
    }
}

String getTiFlashJoinErrorMessage(const tipb::Join & join)
{
    try
    {
        JoinInterpreterHelper::TiFlashJoin tiflash_join(join, false);
        static_cast<void>(tiflash_join);
        return "";
    }
    catch (Exception & e)
    {
        return e.message();
    }
}

TEST(JoinKindAndBuildIndexTestRunner, TestNullAwareJoins)
{
    auto result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 1, true, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::NullAware_Anti && result.second == 1);

    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterSemiJoin, 1, true, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::NullAware_LeftOuterSemi && result.second == 1);

    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 1, true, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::NullAware_LeftOuterAnti && result.second == 1);

    /// NullAware join, expect join keys > 0
    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeAntiSemiJoin, 1, true, 0));

    /// NullAware join, expect right table as build table
    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeAntiSemiJoin, 0, true, 1));
    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeLeftOuterSemiJoin, 0, true, 1));
    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 0, true, 1));
}

TEST(JoinKindAndBuildIndexTestRunner, TestNullAwareJoinRejectsNullEqKeys)
{
    auto error_message = getTiFlashJoinErrorMessage(makeNullAwareJoinWithNullEq());
    ASSERT_FALSE(error_message.empty());
    ASSERT_NE(error_message.find("NullEQ"), String::npos);
}

TEST(JoinKindAndBuildIndexTestRunner, TestNullEqAlignsMixedNullabilityKeySchema)
{
    try
    {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto nullable_int_type = makeNullable(int_type);
        auto context = TiFlashTestEnv::getContext();

        ColumnWithTypeAndName probe_column{nullptr, int_type, "probe_k"};
        ColumnWithTypeAndName build_column{nullptr, nullable_int_type, "build_k"};

        tipb::Join join;
        join.set_join_type(tipb::JoinType::TypeInnerJoin);
        join.set_inner_idx(1);
        *join.add_left_join_keys() = columnToTiPBExpr(probe_column, 0);
        *join.add_right_join_keys() = columnToTiPBExpr(build_column, 0);
        join.add_is_null_eq(true);

        JoinInterpreterHelper::TiFlashJoin tiflash_join(join, true);

        NamesAndTypes probe_source_columns{{probe_column.name, probe_column.type}};
        NamesAndTypes build_source_columns{{build_column.name, build_column.type}};

        auto [probe_prepare_actions, probe_key_names, original_probe_key_names, probe_filter_column_name]
            = JoinInterpreterHelper::prepareJoin(
                *context,
                probe_source_columns,
                tiflash_join.getProbeJoinKeys(),
                tiflash_join.join_key_types,
                tiflash_join.getProbeConditions());
        auto [build_prepare_actions, build_key_names, original_build_key_names, build_filter_column_name]
            = JoinInterpreterHelper::prepareJoin(
                *context,
                build_source_columns,
                tiflash_join.getBuildJoinKeys(),
                tiflash_join.join_key_types,
                tiflash_join.getBuildConditions());

        ASSERT_FALSE(probe_prepare_actions->getSampleBlock().getByName(probe_key_names[0]).type->isNullable());
        ASSERT_TRUE(build_prepare_actions->getSampleBlock().getByName(build_key_names[0]).type->isNullable());

        JoinInterpreterHelper::alignNullEqKeyTypes(
            tiflash_join.is_null_eq,
            probe_prepare_actions,
            probe_key_names,
            build_prepare_actions,
            build_key_names);

        ASSERT_TRUE(probe_prepare_actions->getSampleBlock().getByName(probe_key_names[0]).type->isNullable());
        ASSERT_TRUE(build_prepare_actions->getSampleBlock().getByName(build_key_names[0]).type->isNullable());
        ASSERT_TRUE(probe_prepare_actions->getSampleBlock()
                        .getByName(probe_key_names[0])
                        .type->equals(*build_prepare_actions->getSampleBlock().getByName(build_key_names[0]).type));
    }
    catch (Exception & e)
    {
        FAIL() << e.message();
    }
}

TEST(JoinKindAndBuildIndexTestRunner, TestNullableNullEqDisablesRuntimeFilter)
{
    try
    {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto nullable_int_type = makeNullable(int_type);
        auto context = TiFlashTestEnv::getContext();

        ColumnWithTypeAndName probe_column{nullptr, int_type, "probe_k"};
        ColumnWithTypeAndName build_column{nullptr, nullable_int_type, "build_k"};

        tipb::Join join;
        join.set_join_type(tipb::JoinType::TypeInnerJoin);
        join.set_inner_idx(1);
        *join.add_left_join_keys() = columnToTiPBExpr(probe_column, 0);
        *join.add_right_join_keys() = columnToTiPBExpr(build_column, 0);
        join.add_is_null_eq(true);

        JoinInterpreterHelper::TiFlashJoin tiflash_join(join, true);

        NamesAndTypes probe_source_columns{{probe_column.name, probe_column.type}};
        NamesAndTypes build_source_columns{{build_column.name, build_column.type}};

        auto [probe_prepare_actions, probe_key_names, original_probe_key_names, probe_filter_column_name]
            = JoinInterpreterHelper::prepareJoin(
                *context,
                probe_source_columns,
                tiflash_join.getProbeJoinKeys(),
                tiflash_join.join_key_types,
                tiflash_join.getProbeConditions());
        auto [build_prepare_actions, build_key_names, original_build_key_names, build_filter_column_name]
            = JoinInterpreterHelper::prepareJoin(
                *context,
                build_source_columns,
                tiflash_join.getBuildJoinKeys(),
                tiflash_join.join_key_types,
                tiflash_join.getBuildConditions());

        JoinInterpreterHelper::alignNullEqKeyTypes(
            tiflash_join.is_null_eq,
            probe_prepare_actions,
            probe_key_names,
            build_prepare_actions,
            build_key_names);

        ASSERT_TRUE(tiflash_join.shouldDisableRuntimeFilter(build_prepare_actions, build_key_names));
    }
    catch (Exception & e)
    {
        FAIL() << e.message();
    }
}

TEST(JoinKindAndBuildIndexTestRunner, TestNonNullableNullEqKeepsRuntimeFilterEnabled)
{
    try
    {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto context = TiFlashTestEnv::getContext();

        ColumnWithTypeAndName probe_column{nullptr, int_type, "probe_k"};
        ColumnWithTypeAndName build_column{nullptr, int_type, "build_k"};

        tipb::Join join;
        join.set_join_type(tipb::JoinType::TypeInnerJoin);
        join.set_inner_idx(1);
        *join.add_left_join_keys() = columnToTiPBExpr(probe_column, 0);
        *join.add_right_join_keys() = columnToTiPBExpr(build_column, 0);
        join.add_is_null_eq(true);

        JoinInterpreterHelper::TiFlashJoin tiflash_join(join, true);

        NamesAndTypes probe_source_columns{{probe_column.name, probe_column.type}};
        NamesAndTypes build_source_columns{{build_column.name, build_column.type}};

        auto [probe_prepare_actions, probe_key_names, original_probe_key_names, probe_filter_column_name]
            = JoinInterpreterHelper::prepareJoin(
                *context,
                probe_source_columns,
                tiflash_join.getProbeJoinKeys(),
                tiflash_join.join_key_types,
                tiflash_join.getProbeConditions());
        auto [build_prepare_actions, build_key_names, original_build_key_names, build_filter_column_name]
            = JoinInterpreterHelper::prepareJoin(
                *context,
                build_source_columns,
                tiflash_join.getBuildJoinKeys(),
                tiflash_join.join_key_types,
                tiflash_join.getBuildConditions());

        JoinInterpreterHelper::alignNullEqKeyTypes(
            tiflash_join.is_null_eq,
            probe_prepare_actions,
            probe_key_names,
            build_prepare_actions,
            build_key_names);

        ASSERT_FALSE(tiflash_join.shouldDisableRuntimeFilter(build_prepare_actions, build_key_names));
    }
    catch (Exception & e)
    {
        FAIL() << e.message();
    }
}

TEST(JoinKindAndBuildIndexTestRunner, TestCrossJoins)
{
    /// Cross Inner Join, both sides supported
    auto result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeInnerJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeInnerJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross && result.second == 1);

    /// Cross LeftOuterJoin, uses right table as build side only
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuter && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuter && result.second == 1);

    /// Cross RightOuterJoin, uses left table as build side only
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeRightOuterJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuter && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeRightOuterJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuter && result.second == 0);

    /// Cross Semi/Anti, expects right table as build side only, otherwise throws exceptions
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeSemiJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_Semi && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_Anti && result.second == 1);

    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeSemiJoin, 0, false, 0));
    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeAntiSemiJoin, 0, false, 0));

    /// Cross LeftOuter Semi/Anti, expects right table as build side only, otherwise throws exceptions
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterSemiJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuterSemi && result.second == 1);
    result
        = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuterAnti && result.second == 1);

    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeLeftOuterSemiJoin, 0, false, 0));
    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 0, false, 0));

    /// Cross FullOuterJoin is out of scope in this round and should fail with a clear message.
    auto error_message = getErrorMessage(tipb::JoinType::TypeFullOuterJoin, 0, false, 0);
    ASSERT_FALSE(error_message.empty());
    ASSERT_NE(error_message.find("Cartesian full outer join"), String::npos);
}

TEST(JoinKindAndBuildIndexTestRunner, TestEqualJoins)
{
    /// Inner Join
    auto result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeInnerJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Inner && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeInnerJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Inner && result.second == 1);

    /// LeftOuterJoin
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuter && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::RightOuter && result.second == 0);

    /// RightOuterJoin
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeRightOuterJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuter && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeRightOuterJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::RightOuter && result.second == 1);

    /// FullOuterJoin, keep full join kind and respect inner_idx as build side.
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeFullOuterJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Full && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeFullOuterJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Full && result.second == 1);

    /// Semi/Anti
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeSemiJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Semi && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeSemiJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::RightSemi && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Anti && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::RightAnti && result.second == 0);

    /// LeftOuter Semi/Anti, expects right table as build side only, otherwise throws exceptions
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterSemiJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuterSemi && result.second == 1);
    result
        = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuterAnti && result.second == 1);

    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeLeftOuterSemiJoin, 0, false, 1));
    ASSERT_TRUE(invalidParams(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 0, false, 1));
}

TEST(JoinKindAndBuildIndexTestRunner, TestFullJoinOutputColumnsAreNullable)
{
    auto join = makeFullOuterJoinForSchemaTest(1);
    JoinInterpreterHelper::TiFlashJoin tiflash_join(join, false);

    auto int_type = std::make_shared<DataTypeInt32>();
    NamesAndTypes left_cols{{"l.a", int_type}, {"l.b", int_type}};
    NamesAndTypes right_cols{{"r.a", int_type}, {"r.b", int_type}};

    auto join_output_columns = tiflash_join.genJoinOutputColumns(left_cols, right_cols, "");
    ASSERT_EQ(join_output_columns.size(), 4);
    for (const auto & column : join_output_columns)
        ASSERT_TRUE(column.type->isNullable()) << column.name;
}

TEST(JoinKindAndBuildIndexTestRunner, TestFullJoinOtherConditionColumnsAreNullable)
{
    auto int_type = std::make_shared<DataTypeInt32>();
    NamesAndTypes left_cols{{"l.a", int_type}, {"l.b", int_type}};
    NamesAndTypes right_cols{{"r.a", int_type}, {"r.b", int_type}};

    for (size_t inner_index : {size_t{0}, size_t{1}})
    {
        auto join = makeFullOuterJoinForSchemaTest(inner_index);
        JoinInterpreterHelper::TiFlashJoin tiflash_join(join, false);

        NamesAndTypes probe_prepare_columns = inner_index == 1
            ? NamesAndTypes{{"l.a", int_type}, {"l.b", int_type}, {"probe_extra", int_type}}
            : NamesAndTypes{{"r.a", int_type}, {"r.b", int_type}, {"probe_extra", int_type}};
        auto probe_prepare_join_actions = std::make_shared<ExpressionActions>(probe_prepare_columns);

        auto columns_for_other_join_filter
            = tiflash_join.genColumnsForOtherJoinFilter(left_cols, right_cols, probe_prepare_join_actions);
        ASSERT_EQ(columns_for_other_join_filter.size(), 5);
        ASSERT_EQ(columns_for_other_join_filter.back().name, "probe_extra");
        for (const auto & column : columns_for_other_join_filter)
            ASSERT_TRUE(column.type->isNullable()) << column.name;
    }
}

TEST(JoinKindAndBuildIndexTestRunner, TestFullJoinAllowsLeftAndRightConditions)
{
    JoinNonEqualConditions full_conditions;
    full_conditions.left_filter_column = "left_cond";
    full_conditions.right_filter_column = "right_cond";
    ASSERT_EQ(full_conditions.validate(ASTTableJoin::Kind::Full), nullptr);

    JoinNonEqualConditions left_only_conditions;
    left_only_conditions.left_filter_column = "left_cond";
    ASSERT_EQ(left_only_conditions.validate(ASTTableJoin::Kind::LeftOuter), nullptr);
    ASSERT_STREQ(left_only_conditions.validate(ASTTableJoin::Kind::Inner), "non left join with left conditions");

    JoinNonEqualConditions right_only_conditions;
    right_only_conditions.right_filter_column = "right_cond";
    ASSERT_EQ(right_only_conditions.validate(ASTTableJoin::Kind::RightOuter), nullptr);
    ASSERT_STREQ(right_only_conditions.validate(ASTTableJoin::Kind::Inner), "non right join with right conditions");
}

} // namespace tests
} // namespace DB
