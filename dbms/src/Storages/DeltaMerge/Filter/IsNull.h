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

class IsNull : public RSOperator
{
    Attr attr;

public:
    explicit IsNull(const Attr & attr_)
        : attr(attr_)
    {}

    String name() override { return "isnull"; }

    ColIds getColumnIDs() override { return {attr.col_id}; }

    String toDebugString() override { return fmt::format(R"({{"op":"{}","col":"{}"}})", name(), attr.col_name); }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        auto rs_index = getRSIndex(param, attr);
        return rs_index ? rs_index->minmax->checkIsNull(start_pack, pack_count) : RSResults(pack_count, RSResult::Some);
    }
};

} // namespace DB::DM
