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

class Greater : public ColCmpVal
{
public:
    Greater(const Attr & attr_, const Field & value_, int null_direction_)
        : ColCmpVal(attr_, value_, null_direction_)
    {}

    String name() override { return "greater"; }

    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        return rsindex.minmax->checkGreater(pack_id, value, rsindex.type, null_direction);
    }

    RSResults batchRoughCheck(size_t pack_count, const RSCheckParam & param) override
    {
        RSResults results(pack_count, RSResult::Some);
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_DIRECTLY(param, attr, rsindex, results);
        for (size_t i = 0; i < pack_count; ++i)
        {
            results[i] = rsindex.minmax->checkGreater(i, value, rsindex.type, null_direction);
        }
        return results;
    }

    RSOperatorPtr switchDirection() override { return createLess(attr, value, null_direction); }
};

} // namespace DB::DM