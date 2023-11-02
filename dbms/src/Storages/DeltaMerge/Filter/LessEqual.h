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
#include <Storages/DeltaMerge/Index/RoughCheck.h>

namespace DB::DM
{

class LessEqual : public ColCmpVal
{
public:
    LessEqual(const Attr & attr_, const Field & value_, int null_direction)
        : ColCmpVal(attr_, value_, null_direction)
    {}

    String name() override { return "less_equal"; }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        RSResults results(pack_count, RSResult::Some);
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_DIRECTLY(param, attr, rsindex, results);
        results = rsindex.minmax->checkCmp<RoughCheck::CheckGreater>(start_pack, pack_count, value, rsindex.type);
        std::transform(results.begin(), results.end(), results.begin(), [](const auto result) { return !result; });
        return results;
    }

    RSOperatorPtr switchDirection() override { return createGreater(attr, value, null_direction); }
};

} // namespace DB::DM
