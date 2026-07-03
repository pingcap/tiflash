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

class NullEqual : public ColCmpVal
{
public:
    NullEqual(const Attr & attr_, const Field & value_)
        : ColCmpVal(attr_, value_)
    {}

    String name() override { return "nullEqual"; }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        auto rs_index = getRSIndex(param, attr);
        if (!rs_index)
            return RSResults(pack_count, RSResult::Some);

        if (value.isNull())
        {
            return rs_index->minmax->checkIsNull(start_pack, pack_count);
        }
        else
        {
            return rs_index->minmax->checkCmp<RoughCheck::CheckEqual>(start_pack, pack_count, value, rs_index->type);
        }
    }

    ColumnRangePtr buildSets(const google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> & index_infos) override
    {
        if (value.isNull())
            return UnsupportedColumnRange::create();

        if (auto set = IntegerSet::createValueSet(attr.type, {value}); set)
        {
            auto iter = std::find_if(index_infos.begin(), index_infos.end(), [&](const auto & info) {
                return info.index_type() == tipb::ColumnarIndexType::TypeInverted
                    && info.inverted_query_info().column_id() == attr.col_id;
            });
            if (iter != index_infos.end())
                return SingleColumnRange::create(
                    iter->inverted_query_info().column_id(),
                    iter->inverted_query_info().index_id(),
                    set);
        }
        return UnsupportedColumnRange::create();
    }
};

} // namespace DB::DM