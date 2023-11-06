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

class Or : public LogicalOp
{
public:
    explicit Or(const RSOperators & children_)
        : LogicalOp(children_)
    {
        if (children.empty())
            throw Exception("Unexpected empty children");
    }

    String name() override { return "or"; }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        RSResults res(pack_count, RSResult::None); // None || X = X
        for (const auto & child : children)
        {
            const auto tmp = child->roughCheck(start_pack, pack_count, param);
            std::transform(res.begin(), res.end(), tmp.cbegin(), res.begin(), [](const auto a, const auto b) {
                return a || b;
            });
        }
        return res;
    }
};

} // namespace DB::DM
