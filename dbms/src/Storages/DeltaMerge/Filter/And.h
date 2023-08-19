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

namespace DB
{
namespace DM
{
class And : public LogicalOp
{
public:
    explicit And(const RSOperators & children_)
        : LogicalOp(children_)
    {
        if (children.empty())
            throw Exception("Unexpected empty children");
    }

    String name() override { return "and"; }

    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        auto res = children[0]->roughCheck(pack_id, param);
        if (res == None)
            return res;
        for (size_t i = 1; i < children.size(); ++i)
        {
            res = res && children[i]->roughCheck(pack_id, param);
            if (res == None)
                return res;
        }
        return res;
    }

    // TODO: override applyOptimize()
};

} // namespace DM

} // namespace DB