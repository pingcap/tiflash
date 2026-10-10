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

#pragma once

#include <Core/Field.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/RSOperator_fwd.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <common/types.h>

#include <optional>
#include <vector>

namespace DB::DM
{

enum class TrimPredicateClass : UInt8
{
    /// equality / IN / bounded range with Q ⊆ E.
    EqualityOrInOrBounded = 0,
    /// col >= L or col > L, with L ∈ E.
    LowerBounded = 1,
    /// col <= U or col < U, with U ∈ E.
    UpperBounded = 2,
};

enum class RSIndexKind : UInt8
{
    Normal = 0,
    PreferTrim = 1,
};

/// Normalized non-NULL match set for a temporal column predicate.
/// Bounds are already timezone-converted packed values when applicable.
struct DateQueryDomain
{
    TrimPredicateClass predicate_class = TrimPredicateClass::EqualityOrInOrBounded;

    /// For EqualityOrInOrBounded: all candidate values (points) that must lie in E.
    /// For ranges, also used to carry the finite endpoints that define Q.
    std::vector<Field> values;

    /// Optional range endpoints. Inclusive/exclusive flags describe Q.
    std::optional<Field> lower;
    bool lower_inclusive = true;
    std::optional<Field> upper;
    bool upper_inclusive = true;

    /// True when every value / finite endpoint that must belong to E is inside [stored_lower, stored_upper).
    bool isTrimEligible(UInt64 stored_lower, UInt64 stored_upper) const;
};

struct RSIndexRequest
{
    ColId col_id = 0;
    RSIndexKind preferred_kind = RSIndexKind::Normal;
    std::optional<DateQueryDomain> query_domain;
};

using RSIndexRequests = std::vector<RSIndexRequest>;

/// Apply conservative None/All downgrades using per-pack trim flags.
RSResults applyTrimRoughCheckCorrection(
    const RSResults & raw,
    size_t start_pack,
    const MinMaxIndex & trim_minmax,
    TrimPredicateClass predicate_class,
    bool record_metrics = false);

/// Flatten top-level AND and merge temporal GE/GT/LE/LT into DateRange PreferTrim ops.
/// Does not rewrite children under OR / NOT. Equal / In are left unchanged.
RSOperatorPtr normalizeTemporalRangesForTrim(const RSOperatorPtr & op);

} // namespace DB::DM
