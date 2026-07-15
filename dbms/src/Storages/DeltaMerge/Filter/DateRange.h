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

#include <Storages/DeltaMerge/Filter/DateQueryDomain.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/RoughCheck.h>

#include <magic_enum.hpp>

namespace DB::DM
{

/// Normalized temporal range used only for Rough Set filtering.
/// Row-level filters remain the original DAG expressions.
class DateRange : public RSOperator
{
public:
    DateRange(const Attr & attr_, DateQueryDomain domain_)
        : attr(attr_)
        , domain(std::move(domain_))
    {}

    String name() override { return "date_range"; }

    ColIds getColumnIDs() override { return {attr.col_id}; }

    RSIndexRequests getIndexRequests() override
    {
        return {RSIndexRequest{
            .col_id = attr.col_id,
            .preferred_kind = RSIndexKind::PreferTrim,
            .query_domain = domain,
        }};
    }

    String toDebugString() override
    {
        return fmt::format(
            R"({{"op":"{}","col":"{}","class":"{}","lower":"{}","lower_inclusive":{},"upper":"{}","upper_inclusive":{}}})",
            name(),
            attr.col_name,
            magic_enum::enum_name(domain.predicate_class),
            domain.lower ? applyVisitor(FieldVisitorToDebugString(), *domain.lower) : "",
            domain.lower_inclusive,
            domain.upper ? applyVisitor(FieldVisitorToDebugString(), *domain.upper) : "",
            domain.upper_inclusive);
    }

    Poco::JSON::Object::Ptr toJSONObject() override
    {
        Poco::JSON::Object::Ptr obj = new Poco::JSON::Object();
        obj->set("op", name());
        obj->set("col", attr.col_name);
        obj->set("class", String(magic_enum::enum_name(domain.predicate_class)));
        if (domain.lower)
            obj->set("lower", applyVisitor(FieldVisitorToDebugString(), *domain.lower));
        if (domain.upper)
            obj->set("upper", applyVisitor(FieldVisitorToDebugString(), *domain.upper));
        return obj;
    }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        if (auto trim = getTrimRSIndex(param, attr, domain))
        {
            auto raw = checkRange(start_pack, pack_count, trim->type, trim->minmax);
            return applyTrimRoughCheckCorrection(raw, start_pack, *trim->minmax, domain.predicate_class);
        }

        if (auto rs_index = getRSIndex(param, attr))
            return checkRange(start_pack, pack_count, rs_index->type, rs_index->minmax);

        return RSResults(pack_count, RSResult::Some);
    }

    const DateQueryDomain & getDomain() const { return domain; }
    const Attr & getAttr() const { return attr; }

private:
    RSResults checkRange(
        size_t start_pack,
        size_t pack_count,
        const DataTypePtr & type,
        const MinMaxIndexPtr & minmax) const
    {
        RSResults results(pack_count, RSResult::All);
        if (domain.lower)
        {
            RSResults lower_res = domain.lower_inclusive
                ? minmax->checkCmp<RoughCheck::CheckGreaterEqual>(start_pack, pack_count, *domain.lower, type)
                : minmax->checkCmp<RoughCheck::CheckGreater>(start_pack, pack_count, *domain.lower, type);
            std::transform(
                results.begin(),
                results.end(),
                lower_res.begin(),
                results.begin(),
                [](const auto a, const auto b) { return a && b; });
        }
        if (domain.upper)
        {
            RSResults upper_res;
            if (domain.upper_inclusive)
            {
                // col <= U  <=>  !(col > U), but keep None for empty packs (has_value=false).
                // Negating empty-pack None into All breaks trim outlier corrections.
                upper_res = minmax->checkCmp<RoughCheck::CheckGreater>(start_pack, pack_count, *domain.upper, type);
            }
            else
            {
                // col < U  <=>  !(col >= U)
                upper_res
                    = minmax->checkCmp<RoughCheck::CheckGreaterEqual>(start_pack, pack_count, *domain.upper, type);
            }
            for (size_t i = 0; i < pack_count; ++i)
            {
                if (minmax->hasValue(start_pack + i))
                    upper_res[i] = !upper_res[i];
                // else: leave None for empty packs
            }
            std::transform(
                results.begin(),
                results.end(),
                upper_res.begin(),
                results.begin(),
                [](const auto a, const auto b) { return a && b; });
        }
        return results;
    }

    Attr attr;
    DateQueryDomain domain;
};

} // namespace DB::DM
