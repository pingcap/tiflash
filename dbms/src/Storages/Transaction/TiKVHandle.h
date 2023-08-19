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

#include <sstream>

#include <Core/Types.h>
#include <Storages/Transaction/Types.h>
#include <common/likely.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace TiKVHandle
{

// make sure MIN < NORMAL < MAX
enum HandleIDType : UInt8
{
    /*
    because handle range is like [xxx, yyy), MIN can be regarded as <NORMAL, min>
    MIN = 0,
    */
    NORMAL = 0,
    MAX = 1,
};

inline static const char * handleIDTypeToString(HandleIDType type)
{
    switch (type)
    {
        case NORMAL:
            return "NORMAL";
        case MAX:
            return "MAX";
    }
    throw Exception("handleIDTypeToString fail", ErrorCodes::LOGICAL_ERROR);
}

template <bool isInt64>
struct DummyIdentity
{
};

template <typename HandleType>
struct Handle
{
    HandleIDType type = HandleIDType::NORMAL;
    HandleType handle_id = HandleType{0};

    static const Handle normal_min;
    static const Handle max;

    bool operator<(const Handle & handle) const
    {
        if (unlikely(type != handle.type))
            return type < handle.type;
        return handle_id < handle.handle_id;
    }

    bool operator<=(const Handle & handle) const { return !(*this > handle); }

    HandleType operator-(const Handle & handle) const { return subtract(handle, DummyIdentity<std::is_same_v<HandleType, Int64>>()); }

    bool operator==(const Handle & handle) const { return type == handle.type && handle_id == handle.handle_id; }

    bool operator>(const Handle & handle) const
    {
        if (unlikely(type != handle.type))
            return type > handle.type;
        return handle_id > handle.handle_id;
    }

    bool operator>=(const Handle & handle) const { return !(*this < handle); }

    void toString(std::stringstream & ss) const
    {
        if (type == HandleIDType::NORMAL)
            ss << handle_id;
        else
            ss << "<" << handleIDTypeToString(type) << ">";
    }

    String toString() const
    {
        std::stringstream ss;
        toString(ss);
        return ss.str();
    }

    // we can not transfer it into HandleType directly
    operator const HandleType &() = delete;

    Handle() = default;
    Handle(const HandleIDType type_, const HandleType handle_id_) : type(type_), handle_id(handle_id_) {}
    // not explicit, can be transferred from HandleType
    Handle(const HandleType handle_id_) : type(HandleIDType::NORMAL), handle_id(handle_id_) {}

private:
    HandleType subtract(const Handle & handle, DummyIdentity<true>) const
    {
        if (type == handle.type && type == HandleIDType::NORMAL)
            return handle_id - handle.handle_id;
        throw Exception("can not compute if type of either one is not HandleIDType::NORMAL", ErrorCodes::LOGICAL_ERROR);
    }

    // can not use operator - when HandleType is not Int64
    HandleType subtract(const Handle & handle, DummyIdentity<false>) const = delete;
};

template <typename HandleType>
const Handle<HandleType> Handle<HandleType>::normal_min = Handle<HandleType>(HandleIDType::NORMAL, std::numeric_limits<HandleType>::min());

template <typename HandleType>
const Handle<HandleType> Handle<HandleType>::max = Handle<HandleType>(HandleIDType::MAX, 0);

template <typename HandleType>
inline bool operator<(const HandleType & handle_id, const Handle<HandleType> & handle)
{
    return handle.type == HandleIDType::MAX || handle_id < handle.handle_id;
}

template <typename HandleType>
inline bool operator==(const HandleType & handle_id, const Handle<HandleType> & handle)
{
    return handle.type == HandleIDType::NORMAL && handle_id == handle.handle_id;
}

template <typename HandleType>
inline bool operator>=(const HandleType & handle_id, const Handle<HandleType> & handle)
{
    return !(handle_id < handle);
}

template <typename HandleType>
inline bool operator>(const HandleType & handle_id, const Handle<HandleType> & handle)
{
    return handle.type == HandleIDType::NORMAL && handle_id > handle.handle_id;
}

template <typename HandleType>
inline bool operator<=(const HandleType & handle_id, const Handle<HandleType> & handle)
{
    return !(handle_id > handle);
}

} // namespace TiKVHandle

template <typename HandleType>
using HandleRange = std::pair<TiKVHandle::Handle<HandleType>, TiKVHandle::Handle<HandleType>>;

} // namespace DB
