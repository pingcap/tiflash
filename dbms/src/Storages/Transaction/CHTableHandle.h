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

#include <Core/Types.h>

#include <Storages/Transaction/TiKVHandle.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace CHTableHandle
{

static const UInt64 SIGN_MARK = UInt64(1) << 63;

using UInt64TableHandle = TiKVHandle::Handle<UInt64>;

using UInt64TableHandleRange = HandleRange<UInt64>;

using HandleIDType = TiKVHandle::HandleIDType;

inline bool withSignMark(const HandleID & handle_id) { return (handle_id & SIGN_MARK) != 0; }

// It's a very important role: the user's pk order is not equal with the handle order in tikv.
// for example: user insert row with only pk (1<<64)-1, and 1, but we get the tikv handle -1, 1.
// so, for any range presented by Int64, if the type of pk is UInt64, we should transform it into to several ranges to get the real order.
inline std::tuple<int, std::array<UInt64TableHandleRange, 2>> splitForUInt64TableHandle(const HandleRange<Int64> & ori_range)
{
    static const UInt64 unsigned_2_power_max = UInt64(1) << 63; // 100000...

    const UInt64TableHandle & begin = {ori_range.first.type, static_cast<UInt64>(ori_range.first.handle_id)};
    const UInt64TableHandle & end = {ori_range.second.type, static_cast<UInt64>(ori_range.second.handle_id)};

    if (begin.type == end.type)
    {
        if (begin.type != HandleIDType::NORMAL)
        {
            // return original one
            return {1, {UInt64TableHandleRange{begin, end}, UInt64TableHandleRange{}}};
        }

        // both are normal

        if ((begin.handle_id & SIGN_MARK) == (end.handle_id & SIGN_MARK))
        {
            // both negative or both positive. relative order are same.
            return {1, {UInt64TableHandleRange{{begin.handle_id}, {end.handle_id}}, UInt64TableHandleRange{}}};
        }
        else
        {
            // one is negative, the other is positive
            return {
                2,
                {
                    UInt64TableHandleRange{{0}, {end.handle_id}},
                    UInt64TableHandleRange{{begin.handle_id}, UInt64TableHandle::max},
                },
            };
        }
    }
    else
    {
        switch (begin.type)
        {
            case HandleIDType::NORMAL:
                // only can be max
                if (end.type != HandleIDType::MAX)
                    throw Exception("end.type != HandleIDType::MAX", ErrorCodes::LOGICAL_ERROR);

                if (withSignMark(begin.handle_id))
                {
                    if (unsigned_2_power_max == begin.handle_id)
                        return {
                            1, {UInt64TableHandleRange{UInt64TableHandle::normal_min, UInt64TableHandle::max}, UInt64TableHandleRange{}}};

                    return {
                        2,
                        {
                            UInt64TableHandleRange{{0}, {unsigned_2_power_max}},
                            UInt64TableHandleRange{{begin.handle_id}, UInt64TableHandle::max},
                        },
                    };
                }
                else
                {
                    return {
                        1,
                        {
                            UInt64TableHandleRange{{begin.handle_id}, {unsigned_2_power_max}},
                            UInt64TableHandleRange{},
                        },
                    };
                }
                break;
            case HandleIDType::MAX:
                // can not into here
                throw Exception("begin.type == HandleIDType::MAX && end.type != HandleIDType::MAX", ErrorCodes::LOGICAL_ERROR);
                break;
        }
    }
    throw Exception("splitForUInt64TableHandle should not happen", ErrorCodes::LOGICAL_ERROR);
}

template <typename Type>
inline void merge_ranges(std::vector<std::pair<Type, Type>> & ranges)
{
    if (ranges.empty())
        return;

    // ranged may overlap, should merge them
    std::sort(ranges.begin(), ranges.end());
    size_t size = 0;
    for (size_t i = 1; i < ranges.size(); ++i)
    {
        if (ranges[i].first <= ranges[size].second)
            ranges[size].second = std::max(ranges[i].second, ranges[size].second);
        else
            ranges[++size] = ranges[i];
    }
    size = size + 1;
    ranges.resize(size);
}

inline bool needSplit(const HandleRange<Int64> & ori_range)
{
    return (ori_range.first.handle_id & SIGN_MARK) != (ori_range.second.handle_id & SIGN_MARK);
}

} // namespace CHTableHandle

} // namespace DB
