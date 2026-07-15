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

#pragma once

#include <Storages/DeltaMerge/Filter/DateQueryDomain.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/RoughCheck.h>
#include <Storages/DeltaMerge/Index/TrimMinMaxIndex.h>

namespace DB::DM
{

class Equal : public ColCmpVal
{
public:
    Equal(const Attr & attr_, const Field & value_)
        : ColCmpVal(attr_, value_)
    {}

    String name() override { return "equal"; }

    DateQueryDomain makeQueryDomain() const
    {
        DateQueryDomain domain;
        domain.predicate_class = TrimPredicateClass::EqualityOrInOrBounded;
        domain.values = {value};
        return domain;
    }

    RSIndexRequests getIndexRequests() override
    {
        if (TrimMinMax::isSupportedTemporalType(*attr.type))
        {
            return {RSIndexRequest{
                .col_id = attr.col_id,
                .preferred_kind = RSIndexKind::PreferTrim,
                .query_domain = makeQueryDomain(),
            }};
        }
        return RSOperator::getIndexRequests();
    }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        if (auto trim = getTrimRSIndex(param, attr, makeQueryDomain()))
        {
            auto raw = trim->minmax->checkCmp<RoughCheck::CheckEqual>(start_pack, pack_count, value, trim->type);
            return applyTrimRoughCheckCorrection(
                raw,
                start_pack,
                *trim->minmax,
                TrimPredicateClass::EqualityOrInOrBounded);
        }
        return minMaxCheckCmp<RoughCheck::CheckEqual>(start_pack, pack_count, param, attr, value);
    }
};

} // namespace DB::DM
