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

#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/Filter/And.h>
#include <Storages/DeltaMerge/Filter/DateQueryDomain.h>
#include <Storages/DeltaMerge/Filter/DateRange.h>
#include <Storages/DeltaMerge/Filter/Greater.h>
#include <Storages/DeltaMerge/Filter/GreaterEqual.h>
#include <Storages/DeltaMerge/Filter/Less.h>
#include <Storages/DeltaMerge/Filter/LessEqual.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/Index/TrimMinMaxIndex.h>

#include <ranges>
#include <unordered_map>

namespace DB::DM
{
namespace
{
bool tryGetUInt64(const Field & f, UInt64 & out)
{
    if (f.isNull())
        return false;
    if (f.getType() == Field::Types::UInt64)
    {
        out = f.get<UInt64>();
        return true;
    }
    if (f.getType() == Field::Types::Int64)
    {
        const auto v = f.get<Int64>();
        if (v < 0)
            return false;
        out = static_cast<UInt64>(v);
        return true;
    }
    return false;
}

bool inHalfOpenRange(UInt64 value, UInt64 lower, UInt64 upper)
{
    return value >= lower && value < upper;
}

void flattenTopLevelAnd(const RSOperatorPtr & op, RSOperators & out)
{
    if (!op)
        return;

    // Iterative flatten of top-level And nesting to avoid deep call stacks on
    // left-associated And(And(And(...))) chains.
    RSOperators stack{op};
    while (!stack.empty())
    {
        auto cur = stack.back();
        stack.pop_back();
        if (auto and_op = std::dynamic_pointer_cast<And>(cur))
        {
            const auto & children = and_op->getChildren();
            // Push in reverse so that left-to-right child order is preserved in `out`.
            for (const auto & it : std::ranges::reverse_view(children))
                stack.push_back(it);
            continue;
        }
        out.push_back(cur);
    }
}

enum class BoundSide : UInt8
{
    LowerInclusive,
    LowerExclusive,
    UpperInclusive,
    UpperExclusive,
};

struct TemporalBound
{
    BoundSide side;
    Field value;
    Attr attr;
};

std::optional<TemporalBound> tryAsTemporalRangeBound(const RSOperatorPtr & op)
{
    auto take = [&](const Attr & attr, const Field & value, BoundSide side) -> std::optional<TemporalBound> {
        if (!attr.type || !TrimMinMax::isSupportedTemporalType(*attr.type))
            return std::nullopt;
        return TemporalBound{.side = side, .value = value, .attr = attr};
    };

    if (auto ge = std::dynamic_pointer_cast<GreaterEqual>(op))
        return take(ge->getAttr(), ge->getValue(), BoundSide::LowerInclusive);
    if (auto gt = std::dynamic_pointer_cast<Greater>(op))
        return take(gt->getAttr(), gt->getValue(), BoundSide::LowerExclusive);
    if (auto le = std::dynamic_pointer_cast<LessEqual>(op))
        return take(le->getAttr(), le->getValue(), BoundSide::UpperInclusive);
    if (auto lt = std::dynamic_pointer_cast<Less>(op))
        return take(lt->getAttr(), lt->getValue(), BoundSide::UpperExclusive);
    return std::nullopt;
}

struct BoundAccumulator
{
    Attr attr;
    std::optional<Field> lower;
    bool lower_inclusive = true;
    std::optional<Field> upper;
    bool upper_inclusive = true;
    bool failed = false;
    RSOperators originals;
};

bool applyBound(BoundAccumulator & acc, const TemporalBound & bound)
{
    UInt64 nv = 0;
    if (!tryGetUInt64(bound.value, nv))
        return false;

    switch (bound.side)
    {
    case BoundSide::LowerInclusive:
    case BoundSide::LowerExclusive:
    {
        const bool inclusive = bound.side == BoundSide::LowerInclusive;
        if (!acc.lower)
        {
            acc.lower = bound.value;
            acc.lower_inclusive = inclusive;
            return true;
        }
        UInt64 ov = 0;
        if (!tryGetUInt64(*acc.lower, ov))
            return false;
        // Stronger lower: larger value; on tie prefer exclusive.
        if (nv > ov || (nv == ov && !inclusive && acc.lower_inclusive))
        {
            acc.lower = bound.value;
            acc.lower_inclusive = inclusive;
        }
        break;
    }
    case BoundSide::UpperInclusive:
    case BoundSide::UpperExclusive:
    {
        const bool inclusive = bound.side == BoundSide::UpperInclusive;
        if (!acc.upper)
        {
            acc.upper = bound.value;
            acc.upper_inclusive = inclusive;
            return true;
        }
        UInt64 ov = 0;
        if (!tryGetUInt64(*acc.upper, ov))
            return false;
        // Stronger upper: smaller value; on tie prefer exclusive.
        if (nv < ov || (nv == ov && !inclusive && acc.upper_inclusive))
        {
            acc.upper = bound.value;
            acc.upper_inclusive = inclusive;
        }
        break;
    }
    }
    return true;
}
} // namespace

bool DateQueryDomain::isTrimEligible(UInt64 stored_lower, UInt64 stored_upper) const
{
    auto endpoint_in_e = [&](const Field & f) {
        UInt64 v = 0;
        return tryGetUInt64(f, v) && inHalfOpenRange(v, stored_lower, stored_upper);
    };

    switch (predicate_class)
    {
    case TrimPredicateClass::EqualityOrInOrBounded:
    {
        if (!values.empty())
        {
            for (const auto & v : values)
            {
                if (!endpoint_in_e(v))
                    return false;
            }
            return true;
        }
        // Bounded range described by lower/upper endpoints: require Q ⊆ E by requiring
        // both finite endpoints to lie in E (conservative for inclusive bounds).
        if (!lower || !upper)
            return false;
        return endpoint_in_e(*lower) && endpoint_in_e(*upper);
    }
    case TrimPredicateClass::LowerBounded:
        return lower.has_value() && endpoint_in_e(*lower);
    case TrimPredicateClass::UpperBounded:
        return upper.has_value() && endpoint_in_e(*upper);
    }
    return false;
}

RSResults applyTrimRoughCheckCorrection(
    const RSResults & raw,
    size_t start_pack,
    const MinMaxIndex & trim_minmax,
    TrimPredicateClass predicate_class,
    bool record_metrics)
{
    RSResults results = raw;
    UInt64 none_count = 0;
    UInt64 some_count = 0;
    UInt64 all_count = 0;
    UInt64 all_null_count = 0;
    UInt64 none_to_some_count = 0;
    UInt64 all_to_some_count = 0;
    for (size_t i = 0; i < results.size(); ++i)
    {
        const size_t pack_id = start_pack + i;
        const bool has_trimmed_low = trim_minmax.hasTrimmedLow(pack_id);
        const bool has_trimmed_high = trim_minmax.hasTrimmedHigh(pack_id);

        // Map pack-level low/high outlier flags to "must match" / "must not match"
        // under the current predicate class. Outliers live in D-E and are invisible
        // to the trim min-max, so raw None/All may need correction below.
        bool trimmed_match_exists = false;
        bool trimmed_nonmatch_exists = false;
        switch (predicate_class)
        {
        case TrimPredicateClass::EqualityOrInOrBounded:
        {
            // Q ⊆ E: any low/high trimmed value is outside Q, so it never matches.
            // It can only invalidate an All (pack still has a non-matching outlier).
            trimmed_match_exists = false;
            trimmed_nonmatch_exists = has_trimmed_low || has_trimmed_high;
            break;
        }
        case TrimPredicateClass::LowerBounded:
        {
            // col >= T / col > T with T in E: a high outlier always matches,
            // a low outlier never matches.
            trimmed_match_exists = has_trimmed_high;
            trimmed_nonmatch_exists = has_trimmed_low;
            break;
        }
        case TrimPredicateClass::UpperBounded:
        {
            // col <= T / col < T with T in E: a low outlier always matches,
            // a high outlier never matches.
            trimmed_match_exists = has_trimmed_low;
            trimmed_nonmatch_exists = has_trimmed_high;
            break;
        }
        }

        auto & r = results[i];
        if (trimmed_match_exists)
        {
            if (r == RSResult::None)
            {
                r = RSResult::Some;
                if (record_metrics)
                    ++none_to_some_count;
            }
            else if (r == RSResult::NoneNull)
            {
                r = RSResult::SomeNull;
                if (record_metrics)
                    ++none_to_some_count;
            }
        }
        if (trimmed_nonmatch_exists)
        {
            if (r == RSResult::All)
            {
                r = RSResult::Some;
                if (record_metrics)
                    ++all_to_some_count;
            }
            else if (r == RSResult::AllNull)
            {
                r = RSResult::SomeNull;
                if (record_metrics)
                    ++all_to_some_count;
            }
        }

        if (record_metrics)
        {
            if (r == RSResult::None || r == RSResult::NoneNull)
                ++none_count;
            else if (r == RSResult::Some || r == RSResult::SomeNull)
                ++some_count;
            else if (r == RSResult::All)
                ++all_count;
            else if (r == RSResult::AllNull)
                ++all_null_count;
        }
    }

    if (record_metrics)
    {
        if (none_count != 0)
            GET_METRIC(tiflash_storage_trim_minmax_rough_check_pack_count, result_none).Increment(none_count);
        if (some_count != 0)
            GET_METRIC(tiflash_storage_trim_minmax_rough_check_pack_count, result_some).Increment(some_count);
        if (all_count != 0)
            GET_METRIC(tiflash_storage_trim_minmax_rough_check_pack_count, result_all).Increment(all_count);
        if (all_null_count != 0)
            GET_METRIC(tiflash_storage_trim_minmax_rough_check_pack_count, result_all_null).Increment(all_null_count);
        if (none_to_some_count != 0)
            GET_METRIC(tiflash_storage_trim_minmax_correction_pack_count, type_none_to_some)
                .Increment(none_to_some_count);
        if (all_to_some_count != 0)
            GET_METRIC(tiflash_storage_trim_minmax_correction_pack_count, type_all_to_some)
                .Increment(all_to_some_count);
    }
    return results;
}

RSOperatorPtr normalizeTemporalRangesForTrim(const RSOperatorPtr & op)
{
    if (!op)
        return op;

    RSOperators leaves;
    flattenTopLevelAnd(op, leaves);

    std::unordered_map<ColId, BoundAccumulator> bounds;
    RSOperators kept;
    kept.reserve(leaves.size());

    for (const auto & leaf : leaves)
    {
        if (auto bound = tryAsTemporalRangeBound(leaf))
        {
            auto & acc = bounds[bound->attr.col_id];
            if (acc.originals.empty())
                acc.attr = bound->attr;
            acc.originals.push_back(leaf);
            if (!applyBound(acc, *bound))
                acc.failed = true;
            continue;
        }
        kept.push_back(leaf);
    }

    for (auto & [col_id, acc] : bounds)
    {
        (void)col_id;
        // Any unparseable bound (or no successful bound) abandons DateRange merge for the column.
        if (acc.failed || (!acc.lower && !acc.upper))
        {
            kept.insert(kept.end(), acc.originals.begin(), acc.originals.end());
            continue;
        }

        DateQueryDomain domain;
        domain.lower = acc.lower;
        domain.lower_inclusive = acc.lower_inclusive;
        domain.upper = acc.upper;
        domain.upper_inclusive = acc.upper_inclusive;
        if (acc.lower && acc.upper)
            domain.predicate_class = TrimPredicateClass::EqualityOrInOrBounded;
        else if (acc.lower)
            domain.predicate_class = TrimPredicateClass::LowerBounded;
        else
            domain.predicate_class = TrimPredicateClass::UpperBounded;
        kept.push_back(createDateRange(acc.attr, std::move(domain)));
    }

    if (kept.empty())
        return EMPTY_RS_OPERATOR;
    if (kept.size() == 1)
        return kept[0];
    return createAnd(kept);
}

} // namespace DB::DM
