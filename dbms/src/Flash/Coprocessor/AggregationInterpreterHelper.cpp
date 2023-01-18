// Copyright 2022 PingCAP, Ltd.
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
#include <Core/ColumnNumbers.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>

namespace DB::AggregationInterpreterHelper
{
namespace
{
bool isFinalAggMode(const tipb::Expr & expr)
{
    if (!expr.has_aggfuncmode())
        /// set default value to true to make it compatible with old version of TiDB since before this
        /// change, all the aggregation in TiFlash is treated as final aggregation
        return true;
    return expr.aggfuncmode() == tipb::AggFunctionMode::FinalMode || expr.aggfuncmode() == tipb::AggFunctionMode::CompleteMode;
}

bool isAllowToUseTwoLevelGroupBy(size_t before_agg_streams_size, const Settings & settings)
{
    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    return before_agg_streams_size > 1 || settings.max_bytes_before_external_group_by != 0;
}
} // namespace

bool isSumOnPartialResults(const tipb::Expr & expr)
{
    if (!expr.has_aggfuncmode())
        return false;
    return getAggFunctionName(expr) == "sum" && (expr.aggfuncmode() == tipb::AggFunctionMode::FinalMode || expr.aggfuncmode() == tipb::AggFunctionMode::Partial2Mode);
}

bool isFinalAgg(const tipb::Aggregation & aggregation)
{
    /// set default value to true to make it compatible with old version of TiDB since before this
    /// change, all the aggregation in TiFlash is treated as final aggregation
    bool is_final_agg = true;
    if (aggregation.agg_func_size() > 0 && !isFinalAggMode(aggregation.agg_func(0)))
        is_final_agg = false;
    for (int i = 1; i < aggregation.agg_func_size(); ++i)
    {
        if (unlikely(is_final_agg != isFinalAggMode(aggregation.agg_func(i))))
            throw TiFlashException("Different aggregation mode detected", Errors::Coprocessor::BadRequest);
    }
    return is_final_agg;
}

bool isGroupByCollationSensitive(const Context & context)
{
    // todo now we can tell if the aggregation is final stage or partial stage,
    //  maybe we can do collation insensitive aggregation if the stage is partial

    /// collation sensitive group by is slower than normal group by, use normal group by by default
    return context.getSettingsRef().group_by_collation_sensitive || context.getDAGContext()->isMPPTask();
}

Aggregator::Params buildParams(
    const Context & context,
    const Block & before_agg_header,
    size_t before_agg_streams_size,
    const Names & key_names,
    const TiDB::TiDBCollators & collators,
    const AggregateDescriptions & aggregate_descriptions,
    bool is_final_agg,
    const SpillConfig & spill_config)
{
    ColumnNumbers keys;
    for (const auto & name : key_names)
    {
        keys.push_back(before_agg_header.getPositionByName(name));
    }

    const Settings & settings = context.getSettingsRef();

    bool allow_to_use_two_level_group_by = isAllowToUseTwoLevelGroupBy(before_agg_streams_size, settings);

    bool has_collator = std::any_of(begin(collators), end(collators), [](const auto & p) { return p != nullptr; });

    return Aggregator::Params(
        before_agg_header,
        keys,
        aggregate_descriptions,
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
        settings.max_bytes_before_external_group_by,
        !is_final_agg,
        spill_config,
        context.getSettingsRef().max_block_size,
        has_collator ? collators : TiDB::dummy_collators);
}

void fillArgColumnNumbers(AggregateDescriptions & aggregate_descriptions, const Block & before_agg_header)
{
    for (auto & descr : aggregate_descriptions)
    {
        if (descr.arguments.empty())
        {
            for (const auto & name : descr.argument_names)
            {
                descr.arguments.push_back(before_agg_header.getPositionByName(name));
            }
        }
    }
}
} // namespace DB::AggregationInterpreterHelper
