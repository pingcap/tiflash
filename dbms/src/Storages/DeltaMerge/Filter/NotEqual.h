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
class NotEqual : public ColCmpVal
{
public:
    NotEqual(const Attr & attr_, const Field & value_)
        : ColCmpVal(attr_, value_, 0)
    {}

    String name() override { return "not_equal"; }

    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        return !rsindex.minmax->checkEqual(pack_id, value, rsindex.type);
    }
};

} // namespace DM

} // namespace DB