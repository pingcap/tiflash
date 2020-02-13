#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>

namespace DB
{

namespace DM
{

struct Attr
{
    String      col_name;
    ColId       col_id;
    DataTypePtr type;
};
using Attrs = std::vector<Attr>;

enum class RSResult : UInt8
{
    Unknown = 0, // Not checked yet
    Some    = 1, // Suspected (but may be empty or full)
    None    = 2, // Empty
    All     = 3, // Full
};

static constexpr RSResult Unknown = RSResult::Unknown;
static constexpr RSResult Some    = RSResult::Some;
static constexpr RSResult None    = RSResult::None;
static constexpr RSResult All     = RSResult::All;

inline RSResult operator!(RSResult v)
{
    if (unlikely(v == Unknown))
        throw Exception("Unexpected Unknown");
    if (v == All)
        return None;
    else if (v == None)
        return All;
    return v;
}

inline RSResult operator||(RSResult v0, RSResult v1)
{
    if (unlikely(v0 == Unknown || v1 == Unknown))
        throw Exception("Unexpected Unknown");
    if (v0 == All || v1 == All)
        return All;
    if (v0 == Some || v1 == Some)
        return Some;
    return None;
}

inline RSResult operator&&(RSResult v0, RSResult v1)
{
    if (unlikely(v0 == Unknown || v1 == Unknown))
        throw Exception("Unexpected Unknown");
    if (v0 == None || v1 == None)
        return None;
    if (v0 == All && v1 == All)
        return All;
    return Some;
}

} // namespace DM

} // namespace DB