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
        RSResults res(pack_count, RSResult::None);
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_DIRECTLY(param, attr, rsindex, res);
        // TODO optimize for IN
        res = rsindex.minmax->checkEqual(start_pack, pack_count, values[0], rsindex.type);
        std::transform(res.begin(), res.end(), res.begin(), [](RSResult r) { return !r; });
        for (size_t i = 1; i < values.size(); ++i)
        {
            auto tmp = rsindex.minmax->checkEqual(start_pack, pack_count, values[i], rsindex.type);
            std::transform(tmp.begin(), tmp.end(), res.begin(), res.begin(), [](RSResult r1, RSResult r2) { return r1 && !r2; });
        }
        return res;
    }
};

} // namespace DB::DM
