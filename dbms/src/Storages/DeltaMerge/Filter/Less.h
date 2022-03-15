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
class Less : public ColCmpVal
{
public:
    Less(const Attr & attr_, const Field & value_, int null_direction)
        : ColCmpVal(attr_, value_, null_direction)
    {}

    String name() override { return "less"; }

    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        return !rsindex.minmax->checkGreaterEqual(pack_id, value, rsindex.type, null_direction);
    }

    RSOperatorPtr switchDirection() override { return createGreater(attr, value, null_direction); }
};

} // namespace DM

} // namespace DB