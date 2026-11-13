// Copyright 2026 PingCAP, Inc.
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

// The child compares session literals against temporary session min/max bounds.
class SessionTimezone : public RSOperator
{
    RSOperatorPtr child;
    Attr attr;
    TimezoneInfo timezone;

public:
    SessionTimezone(RSOperatorPtr child_, const Attr & attr_, const TimezoneInfo & timezone_)
        : child(std::move(child_))
        , attr(attr_)
        , timezone(timezone_)
    {}

    String name() override { return child->name(); }
    ColIds getColumnIDs() override { return child->getColumnIDs(); }
    Poco::JSON::Object::Ptr toJSONObject() override
    {
        auto obj = child->toJSONObject();
        obj->set(
            "session_timezone",
            timezone.is_name_based ? timezone.timezone_name : std::to_string(timezone.timezone_offset));
        return obj;
    }
    String toDebugString() override
    {
        return fmt::format(
            "{} [session_timezone={}]",
            child->toDebugString(),
            timezone.is_name_based ? timezone.timezone_name : std::to_string(timezone.timezone_offset));
    }
    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        auto index = getRSIndex(param, attr);
        if (!index)
            return RSResults(pack_count, RSResult::SomeNull);
        std::vector<UInt8> converted;
        index->minmax = index->minmax->withTimezone(start_pack, pack_count, timezone, converted);
        RSCheckParam session_param;
        session_param.indexes.emplace(attr.col_id, *index);
        auto results = child->roughCheck(0, pack_count, session_param);
        for (size_t i = 0; i < pack_count; ++i)
            if (!converted[i])
                results[i] = RSResult::SomeNull;
        return results;
    }
    ColumnRangePtr buildSets(const google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> &) override
    {
        // Inverted indexes still contain UTC values.
        return UnsupportedColumnRange::create();
    }
};

} // namespace DB::DM
