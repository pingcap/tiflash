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
        // attr.type can be empty when column id is missing from table defines.
        if (attr.type && TrimMinMax::isSupportedTemporalType(*attr.type))
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
                TrimPredicateClass::EqualityOrInOrBounded,
                param.record_trim_metrics);
        }
        return minMaxCheckCmp<RoughCheck::CheckEqual>(start_pack, pack_count, param, attr, value);
    }

    ColumnRangePtr buildSets(const google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> & index_infos) override
    {
        if (auto set = IntegerSet::createValueSet(attr.type, {value}); set)
        {
            auto iter = std::find_if(index_infos.begin(), index_infos.end(), [&](const auto & info) {
                return info.index_type() == tipb::ColumnarIndexType::TypeInverted
                    && info.inverted_query_info().column_id() == attr.col_id;
            });
            if (iter != index_infos.end())
                return SingleColumnRange::create(
                    iter->inverted_query_info().column_id(),
                    iter->inverted_query_info().index_id(),
                    set);
        }
        return UnsupportedColumnRange::create();
    }
};

} // namespace DB::DM
