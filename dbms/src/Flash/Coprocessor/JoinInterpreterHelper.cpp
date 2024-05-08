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

#include <Common/TiFlashException.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Interpreters/Context.h>
#include <Interpreters/Join.h>
#include <TiDB/Decode/TypeMapping.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
extern const int NO_COMMON_TYPE;
} // namespace ErrorCodes

namespace JoinInterpreterHelper
{
namespace
{
struct JoinKindAndInnerIndexPairHash
{
    std::size_t operator()(const std::pair<tipb::JoinType, size_t> & pair) const
    {
        return (static_cast<size_t>(pair.first) << 1) | pair.second;
    }
};

std::pair<ASTTableJoin::Kind, size_t> getJoinKindAndBuildSideIndex(
    tipb::JoinType tipb_join_type,
    size_t inner_index,
    bool is_null_aware,
    size_t join_keys_size)
{
    /// in DAG request, inner part is the build side, however for TiFlash implementation,
    /// the build side must be the right side, so need to swap the join side if needed
    /// 1. for (cross) inner join, semi/anti-semi join, there is no problem in this swap.
    /// 2. for cross semi/anti-semi join, the build side is always right, needn't swap.
    /// 3. for non-cross left/right outer join, there is no problem in this swap.
    /// 4. for cross left outer join, the build side is always right, needn't and can't swap.
    /// 5. for cross right outer join, the build side is always left, so it will always swap and change to cross left outer join.
    /// note that whatever the build side is, we can't support cross-right-outer join now.
    static const std::unordered_map<
        std::pair<tipb::JoinType, size_t>,
        std::pair<ASTTableJoin::Kind, size_t>,
        JoinKindAndInnerIndexPairHash>
        equal_join_type_map{
            {{tipb::JoinType::TypeInnerJoin, 0}, {ASTTableJoin::Kind::Inner, 0}},
            {{tipb::JoinType::TypeInnerJoin, 1}, {ASTTableJoin::Kind::Inner, 1}},
            {{tipb::JoinType::TypeLeftOuterJoin, 0}, {ASTTableJoin::Kind::RightOuter, 0}},
            {{tipb::JoinType::TypeLeftOuterJoin, 1}, {ASTTableJoin::Kind::LeftOuter, 1}},
            {{tipb::JoinType::TypeRightOuterJoin, 0}, {ASTTableJoin::Kind::LeftOuter, 0}},
            {{tipb::JoinType::TypeRightOuterJoin, 1}, {ASTTableJoin::Kind::RightOuter, 1}},
            {{tipb::JoinType::TypeSemiJoin, 0}, {ASTTableJoin::Kind::RightSemi, 0}},
            {{tipb::JoinType::TypeSemiJoin, 1}, {ASTTableJoin::Kind::Inner, 1}},
            {{tipb::JoinType::TypeAntiSemiJoin, 0}, {ASTTableJoin::Kind::RightAnti, 0}},
            {{tipb::JoinType::TypeAntiSemiJoin, 1}, {ASTTableJoin::Kind::Anti, 1}},
            {{tipb::JoinType::TypeLeftOuterSemiJoin, 1}, {ASTTableJoin::Kind::LeftOuterSemi, 1}},
            {{tipb::JoinType::TypeAntiLeftOuterSemiJoin, 1}, {ASTTableJoin::Kind::LeftOuterAnti, 1}}};
    static const std::unordered_map<
        std::pair<tipb::JoinType, size_t>,
        std::pair<ASTTableJoin::Kind, size_t>,
        JoinKindAndInnerIndexPairHash>
        cartesian_join_type_map{
            {{tipb::JoinType::TypeInnerJoin, 0}, {ASTTableJoin::Kind::Cross, 0}},
            {{tipb::JoinType::TypeInnerJoin, 1}, {ASTTableJoin::Kind::Cross, 1}},
            {{tipb::JoinType::TypeLeftOuterJoin, 0}, {ASTTableJoin::Kind::Cross_LeftOuter, 1}},
            {{tipb::JoinType::TypeLeftOuterJoin, 1}, {ASTTableJoin::Kind::Cross_LeftOuter, 1}},
            {{tipb::JoinType::TypeRightOuterJoin, 0}, {ASTTableJoin::Kind::Cross_LeftOuter, 0}},
            {{tipb::JoinType::TypeRightOuterJoin, 1}, {ASTTableJoin::Kind::Cross_LeftOuter, 0}},
            {{tipb::JoinType::TypeSemiJoin, 1}, {ASTTableJoin::Kind::Cross, 1}},
            {{tipb::JoinType::TypeAntiSemiJoin, 1}, {ASTTableJoin::Kind::Cross_Anti, 1}},
            {{tipb::JoinType::TypeLeftOuterSemiJoin, 1}, {ASTTableJoin::Kind::Cross_LeftOuterSemi, 1}},
            {{tipb::JoinType::TypeAntiLeftOuterSemiJoin, 1}, {ASTTableJoin::Kind::Cross_LeftOuterAnti, 1}}};
    static const std::unordered_map<
        std::pair<tipb::JoinType, size_t>,
        std::pair<ASTTableJoin::Kind, size_t>,
        JoinKindAndInnerIndexPairHash>
        null_aware_join_type_map{
            {{tipb::JoinType::TypeAntiSemiJoin, 1}, {ASTTableJoin::Kind::NullAware_Anti, 1}},
            {{tipb::JoinType::TypeLeftOuterSemiJoin, 1}, {ASTTableJoin::Kind::NullAware_LeftOuterSemi, 1}},
            {{tipb::JoinType::TypeAntiLeftOuterSemiJoin, 1}, {ASTTableJoin::Kind::NullAware_LeftOuterAnti, 1}}};

    RUNTIME_ASSERT(inner_index == 0 || inner_index == 1);
    const auto & join_type_map = [is_null_aware, join_keys_size]() {
        if (is_null_aware)
        {
            if (unlikely(join_keys_size == 0))
                throw TiFlashException("null-aware semi join does not have join key", Errors::Coprocessor::BadRequest);
            return null_aware_join_type_map;
        }
        else if (join_keys_size > 0)
            return equal_join_type_map;
        else
            return cartesian_join_type_map;
    }();

    auto join_type_it = join_type_map.find(std::make_pair(tipb_join_type, inner_index));
    if (unlikely(join_type_it == join_type_map.end()))
        throw TiFlashException(
            Errors::Coprocessor::BadRequest,
            "Unknown join type in dag request {} {}",
            fmt::underlying(tipb_join_type),
            inner_index);
    return join_type_it->second;
}

std::pair<ASTTableJoin::Kind, size_t> getJoinKindAndBuildSideIndex(const tipb::Join & join)
{
    return getJoinKindAndBuildSideIndex(
        join.join_type(),
        join.inner_idx(),
        join.is_null_aware_semi_join(),
        join.left_join_keys_size());
}

JoinKeyType geCommonTypeForJoinOn(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    try
    {
        return {getLeastSupertype({left_type, right_type}), false};
    }
    catch (DB::Exception & e)
    {
        if (e.code() == ErrorCodes::NO_COMMON_TYPE && removeNullable(left_type)->isDecimal()
            && removeNullable(right_type)->isDecimal())
        {
            // fix https://github.com/pingcap/tiflash/issues/4519
            // String is the common type for all types, it is always safe to choose String.
            // But then we need to use `FunctionFormatDecimal` to format decimal.
            // For example 0.1000000000 is equal to 0.10000000000000000000, but the original strings are not equal.
            RUNTIME_ASSERT(!left_type->onlyNull() || !right_type->onlyNull());
            auto fall_back_type = std::make_shared<DataTypeString>();
            bool make_nullable = left_type->isNullable() || right_type->isNullable();
            return {make_nullable ? makeNullable(fall_back_type) : fall_back_type, true};
        }
        else
        {
            throw;
        }
    }
}

JoinKeyTypes getJoinKeyTypes(const tipb::Join & join)
{
    const auto & left_join_keys = join.left_join_keys();
    const auto & right_join_keys = join.right_join_keys();

    if (unlikely(join.left_join_keys_size() != join.right_join_keys_size()))
        throw TiFlashException(
            "size of join.left_join_keys != size of join.right_join_keys",
            Errors::Coprocessor::BadRequest);
    JoinKeyTypes join_key_types;
    for (int i = 0; i < join.left_join_keys_size(); ++i)
    {
        if (unlikely(!exprHasValidFieldType(left_join_keys[i]) || !exprHasValidFieldType(right_join_keys[i])))
            throw TiFlashException("Join key without field type", Errors::Coprocessor::BadRequest);
        auto left_type = getDataTypeByFieldTypeForComputingLayer(left_join_keys[i].field_type());
        auto right_type = getDataTypeByFieldTypeForComputingLayer(right_join_keys[i].field_type());
        join_key_types.emplace_back(geCommonTypeForJoinOn(left_type, right_type));
    }
    return join_key_types;
}

TiDB::TiDBCollators getJoinKeyCollators(const tipb::Join & join, const JoinKeyTypes & join_key_types, bool is_test)
{
    TiDB::TiDBCollators collators;
    size_t join_key_size = join_key_types.size();
    if (join.probe_types_size() == static_cast<int>(join_key_size)
        && join.build_types_size() == join.probe_types_size())
        for (size_t i = 0; i < join_key_size; ++i)
        {
            // Don't need to check the collate for decimal format string.
            if ((removeNullable(join_key_types[i].key_type)->isString() && !join_key_types[i].is_incompatible_decimal)
                || unlikely(is_test))
            {
                if (unlikely(join.probe_types(i).collate() != join.build_types(i).collate()))
                    throw TiFlashException(
                        "Join with different collators on the join key",
                        Errors::Coprocessor::BadRequest);
                collators.push_back(getCollatorFromFieldType(join.probe_types(i)));
            }
            else
                collators.push_back(nullptr);
        }
    return collators;
}
} // namespace

TiFlashJoin::TiFlashJoin(const tipb::Join & join_, bool is_test) // NOLINT(cppcoreguidelines-pro-type-member-init)
    : join(join_)
    , join_key_types(getJoinKeyTypes(join_))
    , join_key_collators(getJoinKeyCollators(join_, join_key_types, is_test))
{
    std::tie(kind, build_side_index) = getJoinKindAndBuildSideIndex(join);
    strictness
        = (isSemiJoin() && !isRightSemiFamily(kind)) ? ASTTableJoin::Strictness::Any : ASTTableJoin::Strictness::All;
}

String TiFlashJoin::genMatchHelperName(const Block & header1, const Block & header2) const
{
    if (!isLeftOuterSemiFamily())
    {
        return "";
    }

    size_t i = 0;
    String match_helper_name = fmt::format("{}{}", Join::match_helper_prefix, i);
    while (header1.has(match_helper_name) || header2.has(match_helper_name))
    {
        match_helper_name = fmt::format("{}{}", Join::match_helper_prefix, ++i);
    }
    return match_helper_name;
}

String TiFlashJoin::genFlagMappedEntryHelperName(const Block & header1, const Block & header2, bool has_other_condition)
    const
{
    if (!useRowFlaggedHashMap(kind, has_other_condition))
    {
        return "";
    }

    size_t i = 0;
    String helper_name = fmt::format("{}{}", Join::flag_mapped_entry_helper_prefix, i);
    while (header1.has(helper_name) || header2.has(helper_name))
    {
        helper_name = fmt::format("{}{}", Join::flag_mapped_entry_helper_prefix, ++i);
    }
    return helper_name;
}

NamesAndTypes TiFlashJoin::genColumnsForOtherJoinFilter(
    const Block & left_input_header,
    const Block & right_input_header,
    const ExpressionActionsPtr & probe_prepare_join_actions) const
{
#ifndef NDEBUG
    auto is_prepare_actions_valid = [](const Block & origin_block, const ExpressionActionsPtr & prepare_actions) {
        const Block & prepare_sample_block = prepare_actions->getSampleBlock();
        for (const auto & p : origin_block)
        {
            if (!prepare_sample_block.has(p.name))
                return false;
        }
        return true;
    };
    if (unlikely(!is_prepare_actions_valid(
            build_side_index == 1 ? left_input_header : right_input_header,
            probe_prepare_join_actions)))
    {
        throw TiFlashException("probe_prepare_join_actions isn't valid", Errors::Coprocessor::Internal);
    }
#endif

    /// columns_for_other_join_filter is a vector of columns used
    /// as the input columns when compiling other join filter.
    /// Note the order in the column vector is very important:
    /// first the columns in left_input_header, then followed
    /// by the columns in right_input_header, if there are other
    /// columns generated before compile other join filter, then
    /// append the extra columns afterwards. In order to figure out
    /// whether a given column is already in the column vector or
    /// not quickly, we use another set to store the column names.

    /// The order of columns must be {left_input, right_input, extra columns},
    /// because tidb requires the input schema of join to be {left_input, right_input}.
    /// Extra columns are appended to prevent extra columns from being repeatedly generated.

    NamesAndTypes columns_for_other_join_filter;
    std::unordered_set<String> column_set_for_origin_columns;

    auto append_origin_columns
        = [&columns_for_other_join_filter, &column_set_for_origin_columns](const Block & header, bool make_nullable) {
              for (const auto & p : header)
              {
                  columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
                  column_set_for_origin_columns.emplace(p.name);
              }
          };
    append_origin_columns(left_input_header, join.join_type() == tipb::JoinType::TypeRightOuterJoin);
    append_origin_columns(right_input_header, join.join_type() == tipb::JoinType::TypeLeftOuterJoin);

    /// append the columns generated by probe side prepare join actions.
    /// the new columns are
    /// - filter_column and related temporary columns
    /// - join keys and related temporary columns
    auto append_new_columns
        = [&columns_for_other_join_filter, &column_set_for_origin_columns](const Block & header, bool make_nullable) {
              for (const auto & p : header)
              {
                  if (column_set_for_origin_columns.find(p.name) == column_set_for_origin_columns.end())
                      columns_for_other_join_filter.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
              }
          };
    bool make_nullable = build_side_index == 1 ? join.join_type() == tipb::JoinType::TypeRightOuterJoin
                                               : join.join_type() == tipb::JoinType::TypeLeftOuterJoin;
    append_new_columns(probe_prepare_join_actions->getSampleBlock(), make_nullable);

    return columns_for_other_join_filter;
}

NamesAndTypes TiFlashJoin::genJoinOutputColumns(
    const Block & left_input_header,
    const Block & right_input_header,
    const String & match_helper_name) const
{
    NamesAndTypes join_output_columns;
    auto append_output_columns = [&join_output_columns](const Block & header, bool make_nullable) {
        for (auto const & p : header)
        {
            join_output_columns.emplace_back(p.name, make_nullable ? makeNullable(p.type) : p.type);
        }
    };

    append_output_columns(left_input_header, join.join_type() == tipb::JoinType::TypeRightOuterJoin);
    if (!isSemiJoin())
    {
        /// for semi join, the columns from right table will be ignored
        append_output_columns(right_input_header, join.join_type() == tipb::JoinType::TypeLeftOuterJoin);
    }

    if (!match_helper_name.empty())
    {
        join_output_columns.emplace_back(match_helper_name, Join::match_helper_type);
    }

    return join_output_columns;
}

void TiFlashJoin::fillJoinOtherConditionsAction(
    const Context & context,
    const Block & left_input_header,
    const Block & right_input_header,
    const ExpressionActionsPtr & probe_side_prepare_join,
    const Names & probe_key_names,
    const Names & build_key_names,
    JoinNonEqualConditions & join_non_equal_conditions) const
{
    auto columns_for_other_join_filter
        = genColumnsForOtherJoinFilter(left_input_header, right_input_header, probe_side_prepare_join);

    if (join.other_conditions_size() == 0 && join.other_eq_conditions_from_in_size() == 0
        && !join.is_null_aware_semi_join())
        return;

    DAGExpressionAnalyzer dag_analyzer(columns_for_other_join_filter, context);
    ExpressionActionsChain chain;

    if (join.other_conditions_size() > 0)
        join_non_equal_conditions.other_cond_name = dag_analyzer.appendWhere(chain, join.other_conditions());

    if (join.other_eq_conditions_from_in_size() > 0)
        join_non_equal_conditions.other_eq_cond_from_in_name
            = dag_analyzer.appendWhere(chain, join.other_eq_conditions_from_in());

    if (!chain.steps.empty())
    {
        join_non_equal_conditions.other_cond_expr = chain.getLastActions();
        chain.addStep();
    }

    if (join.is_null_aware_semi_join())
    {
        join_non_equal_conditions.null_aware_eq_cond_name
            = dag_analyzer.appendNullAwareSemiJoinEqColumn(chain, probe_key_names, build_key_names, join_key_collators);
        join_non_equal_conditions.null_aware_eq_cond_expr = chain.getLastActions();
    }
}

std::tuple<ExpressionActionsPtr, Names, Names, String> prepareJoin(
    const Context & context,
    const Block & input_header,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & keys,
    const JoinKeyTypes & join_key_types,
    bool left,
    bool is_right_out_join,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & filters)
{
    NamesAndTypes source_columns;
    for (auto const & p : input_header)
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    ExpressionActionsChain chain;
    Names key_names;
    Names original_key_names;
    String filter_column_name;
    dag_analyzer.appendJoinKeyAndJoinFilters(
        chain,
        keys,
        join_key_types,
        key_names,
        original_key_names,
        left,
        is_right_out_join,
        filters,
        filter_column_name);
    return {chain.getLastActions(), std::move(key_names), std::move(original_key_names), std::move(filter_column_name)};
}

std::vector<RuntimeFilterPtr> TiFlashJoin::genRuntimeFilterList(
    const Context & context,
    const Block & input_header,
    const LoggerPtr & log)
{
    std::vector<RuntimeFilterPtr> result;
    if (join.runtime_filter_list().empty())
    {
        return result;
    }
    result.reserve(join.runtime_filter_list().size());
    NamesAndTypes source_columns;
    source_columns.reserve(input_header.columns());
    for (auto const & p : input_header)
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    LOG_DEBUG(log, "before gen runtime filter, pb rf size:{}", join.runtime_filter_list().size());
    for (auto rf_pb : join.runtime_filter_list())
    {
        RuntimeFilterPtr runtime_filter = std::make_shared<RuntimeFilter>(rf_pb);
        // check if rs operator support runtime filter target expr type
        try
        {
            runtime_filter->build();
            dag_analyzer.appendRuntimeFilterProperties(runtime_filter);
        }
        catch (TiFlashException & e)
        {
            LOG_WARNING(log, "The runtime filter will not be register, reason:{}", e.message());
            continue;
        }
        LOG_DEBUG(log, "push back runtime filter, id:{}", runtime_filter->id);
        result.push_back(runtime_filter);
    }
    return result;
}
} // namespace JoinInterpreterHelper
} // namespace DB
