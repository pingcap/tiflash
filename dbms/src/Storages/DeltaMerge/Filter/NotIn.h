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

class NotIn : public RSOperator
{
    Attr attr;
    Fields values;

public:
    NotIn(const Attr & attr_, const Fields & values_)
        : attr(attr_)
        , values(values_)
    {
        if (unlikely(values.empty()))
            throw Exception("Unexpected empty values");
    }

    String name() override { return "not_in"; }

    Attrs getAttrs() override { return {attr}; }

    String toDebugString() override
    {
        String s = R"({"op":")" + name() + R"(","col":")" + attr.col_name + R"(","value":"[)";
        for (auto & v : values)
            s += "\"" + applyVisitor(FieldVisitorToDebugString(), v) + "\",";
        s.pop_back();
        return s + "]}";
    };

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        RSResults res(pack_count, RSResult::Some);
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_DIRECTLY(param, attr, rsindex, res);
        res = rsindex.minmax->checkIn(start_pack, pack_count, values, rsindex.type);
        // Note: Actually NotIn is not always equal to !In, like check(not in (NULL)) == check(in (NULL)) == None
        // But it is not a problem, because we will check NULL in the filter operator.
        std::transform(res.begin(), res.end(), res.begin(), [](RSResult result) { return !result; });
        return res;
    }
};

} // namespace DB::DM
