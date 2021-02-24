#pragma once

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

class IDataType;

template <typename... Ts, typename F>
static bool castTypeToEither(const IDataType * input_type, F && f)
{
    /// XXX can't use && here because gcc-7 complains about parentheses around && within ||
    const IDataType * type = input_type;
    bool is_nullable = type->isNullable();
    if (is_nullable)
        type = typeid_cast<const DataTypeNullable *>(type)->getNestedType().get();
    return ((typeid_cast<const Ts *>(type) ? f(*typeid_cast<const Ts *>(type), is_nullable) : false) || ...);
}

}
