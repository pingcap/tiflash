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

namespace DB
{
namespace DM
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


    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        // TODO optimize for IN
        RSResult res = !rsindex.minmax->checkEqual(pack_id, values[0], rsindex.type);
        for (size_t i = 1; i < values.size(); ++i)
            res = res && !rsindex.minmax->checkEqual(pack_id, values[i], rsindex.type);
        return res;
    }
};

} // namespace DM

} // namespace DB
