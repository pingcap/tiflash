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

#include <Common/ThresholdUtils.h>
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
    return expr.aggfuncmode() == tipb::AggFunctionMode::FinalMode
        || expr.aggfuncmode() == tipb::AggFunctionMode::CompleteMode;
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
    return getAggFunctionName(expr) == "sum"
        && (expr.aggfuncmode() == tipb::AggFunctionMode::FinalMode
            || expr.aggfuncmode() == tipb::AggFunctionMode::Partial2Mode);
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

std::shared_ptr<Aggregator::Params> buildParams(
    const Context & context,
    const Block & before_agg_header,
    size_t before_agg_streams_size,
    size_t agg_streams_size,
    const Names & key_names,
    const std::unordered_map<String, String> & key_from_agg_func,
    const TiDB::TiDBCollators & collators,
    const AggregateDescriptions & aggregate_descriptions,
    bool is_final_agg,
    const SpillConfig & spill_config)
{
    ColumnNumbers keys(key_names.size(), 0);
    size_t normal_key_idx = 0;
    size_t agg_func_as_key_idx = key_names.size() - key_from_agg_func.size();
    // Put group by key that reference aggregate func after original key. For example:
    // select sum(c0), first_row(c1), first_row(c3) group by c1, c2, c3
    // Before: keys: c1 | c2 | c3
    // After:  keys: c2 | c1 | c3
    // By doing this, when deserialize group by keys from HashMap to columns,
    // we only need to handle c2(normal_key_size == 1) and ignore c2/c3.
    assert(key_names.size() == collators.size());
    TiDB::TiDBCollators reordered_collators(collators.size(), nullptr);
    for (size_t i = 0; i < key_names.size(); ++i)
    {
        const auto & name = key_names[i];
        auto col_idx = before_agg_header.getPositionByName(name);
        if (key_from_agg_func.find(name) == key_from_agg_func.end())
        {
            keys[normal_key_idx] = col_idx;
            reordered_collators[normal_key_idx++] = collators[i];
        }
        else
        {
            keys[agg_func_as_key_idx] = col_idx;
            reordered_collators[agg_func_as_key_idx++] = collators[i];
        }
    }
    assert(normal_key_idx == key_names.size() - key_from_agg_func.size());
    assert(agg_func_as_key_idx == key_names.size());

    const Settings & settings = context.getSettingsRef();

    bool allow_to_use_two_level_group_by = isAllowToUseTwoLevelGroupBy(before_agg_streams_size, settings);
    auto total_two_level_threshold_bytes
        = allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0);

    bool has_collator = std::any_of(begin(reordered_collators), end(reordered_collators), [](const auto & p) { return p != nullptr; });

    return std::make_shared<Aggregator::Params>(
        before_agg_header,
        keys,
        key_from_agg_func,
        aggregate_descriptions,
        /// do not use the average value for key count threshold, because for a random distributed data, the key count
        /// in every threads should almost be the same
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        getAverageThreshold(total_two_level_threshold_bytes, agg_streams_size),
        getAverageThreshold(settings.max_bytes_before_external_group_by, agg_streams_size),
        !is_final_agg,
        spill_config,
        context.getSettingsRef().max_block_size,
        has_collator ? reordered_collators : TiDB::dummy_collators);
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
