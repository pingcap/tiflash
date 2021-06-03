#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/expression.pb.h>
#pragma GCC diagnostic pop

#include <Core/Types.h>
#include <common/StringRef.h>

namespace DB
{

class TiDBEnum
{
public:
    TiDBEnum(UInt64 value_, const StringRef & name_) : value(value_), name(name_) {}
    UInt64 value;
    const StringRef & name;
};
} // namespace DB
