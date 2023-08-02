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
class Equal : public ColCmpVal
{
public:
    Equal(const Attr & attr_, const Field & value_)
        : ColCmpVal(attr_, value_, 0)
    {}

    String name() override { return "equal"; }

    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        auto res = RSResult::Some;
        if (rsindex.minmax != nullptr)
        {
            auto minmax_res = rsindex.minmax->checkEqual(pack_id, value, rsindex.type);
           // LOG_INFO(Logger::get("hyy"), "equal roughCheck minmax with minmax_res = {} for pack_id {} ", static_cast<int>(minmax_res), pack_id);
            res = res && minmax_res;
        }

        if (rsindex.bloom_filter_index != nullptr)
        {
            auto bloom_filter_index_res = rsindex.bloom_filter_index->checkEqual(pack_id, value, rsindex.type);
            //LOG_INFO(Logger::get("hyy"), "equal roughCheck bloom_filter with bloom_filter_res = {} for pack_id {}", static_cast<int>(bloom_filter_index_res), pack_id);
            res = res && bloom_filter_index_res;
        }
        //LOG_INFO(Logger::get("hyy"), "equal roughCheck res = {} for pack_id {}", static_cast<int>(res), pack_id);
        return res;
    }
};


} // namespace DM

} // namespace DB