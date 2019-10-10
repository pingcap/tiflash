#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>
#include <Storages/Transaction/Types.h>
#include <common/likely.h>

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

struct Handle
{
    using HandleType = Int64;
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

    HandleType operator-(const Handle & handle) const { return subtract(handle); }

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
    HandleType subtract(const Handle & handle) const
    {
        if (type == handle.type && type == HandleIDType::NORMAL)
            return handle_id - handle.handle_id;
        throw Exception("can not compute if type of either one is not HandleIDType::NORMAL", ErrorCodes::LOGICAL_ERROR);
    }
};

inline bool operator<(const Int64 & handle_id, const Handle & handle)
{
    return handle.type == HandleIDType::MAX || handle_id < handle.handle_id;
}

inline bool operator==(const Int64 & handle_id, const Handle & handle)
{
    return handle.type == HandleIDType::NORMAL && handle_id == handle.handle_id;
}

inline bool operator>=(const Int64 & handle_id, const Handle & handle) { return !(handle_id < handle); }

inline bool operator>(const Int64 & handle_id, const Handle & handle)
{
    return handle.type == HandleIDType::NORMAL && handle_id > handle.handle_id;
}

inline bool operator<=(const Int64 & handle_id, const Handle & handle) { return !(handle_id > handle); }

} // namespace TiKVHandle

using HandleRange = std::pair<TiKVHandle::Handle, TiKVHandle::Handle>;

} // namespace DB
