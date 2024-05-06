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

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB::DM
{

class In : public RSOperator
{
    Attr attr;
    Fields values;

public:
    In(const Attr & attr_, const Fields & values_)
        : attr(attr_)
        , values(values_)
    {}

    String name() override { return "in"; }

    ColIds getColumnIDs() override { return {attr.col_id}; }

    String toDebugString() override
    {
        FmtBuffer buf;
        buf.fmtAppend(R"({{"op":"{}","col":"{}","value":"[)", name(), attr.col_name);
        buf.joinStr(
            values.cbegin(),
            values.cend(),
            [](const auto & v, FmtBuffer & fb) {
                fb.fmtAppend("\"{}\"", applyVisitor(FieldVisitorToDebugString(), v));
            },
            ",");
        buf.append("]}");
        return buf.toString();
    };

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        // If values is empty (for example where a in ()), all packs will not match.
        // So return none directly.
        if (values.empty())
            return RSResults(pack_count, RSResult::None);
        auto rs_index = getRSIndex(param, attr);
        return rs_index ? rs_index->minmax->checkIn(start_pack, pack_count, values, rs_index->type)
                        : RSResults(pack_count, RSResult::Some);
    }
};

} // namespace DB::DM
