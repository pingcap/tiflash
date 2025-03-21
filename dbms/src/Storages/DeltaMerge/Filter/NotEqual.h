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

class NotEqual : public ColCmpVal
{
public:
    NotEqual(const Attr & attr_, const Field & value_)
        : ColCmpVal(attr_, value_)
    {}

    String name() override { return "not_equal"; }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        auto results = minMaxCheckCmp<RoughCheck::CheckEqual>(start_pack, pack_count, param, attr, value);
        std::transform(results.begin(), results.end(), results.begin(), [](RSResult result) { return !result; });
        return results;
    }

    ColumnRangePtr buildSets(const LocalIndexInfosSnapshot & index_info) override
    {
        auto less = IntegerSet::createLessRangeSet(attr.type->getTypeId(), value, /*not_included=*/true);
        auto greater = IntegerSet::createGreaterRangeSet(attr.type->getTypeId(), value, /*not_included=*/true);
        if (less && greater)
        {
            auto iter = std::find_if(index_info->begin(), index_info->end(), [&](const auto & info) {
                return info.column_id == attr.col_id && info.kind == TiDB::ColumnarIndexKind::Inverted;
            });
            if (iter != index_info->end())
                return SingleColumnRange::create(iter->column_id, iter->index_id, less->unionWith(greater));
        }
        return UnsupportedColumnRange::create();
    }
};

} // namespace DB::DM
