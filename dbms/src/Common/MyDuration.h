#pragma once

#include <Core/Field.h>
#include <common/DateLUTImpl.h>


namespace DB
{
static const Int64 NANO_SECOND = 1;
static const Int64 MICRO_SECOND = 1000 * NANO_SECOND;
static const Int64 MILLI_SECOND = 1000 * MICRO_SECOND;
static const Int64 SECOND = 1000 * MILLI_SECOND;
static const Int64 MINUTE = 60 * SECOND;
static const Int64 HOUR = 60 * MINUTE;

class MyDuration
{
public:
    Int64 nanos;
    UInt8 fsp;

    MyDuration() = default;
    MyDuration(Int64 nanos_, UInt8 fsp_)
        : nanos(nanos_)
        , fsp(fsp_)
    {}
    MyDuration(Int32 hour, Int32 minute, Int32 second, Int32 microsecond, UInt8 fsp)
        : MyDuration(hour * HOUR + minute * MINUTE + second * SECOND + microsecond * MICRO_SECOND, fsp)
    {}

    std::tuple<Int32, Int32, Int32, Int32, Int32> splitDuration() const;
    bool isNeg() const { return nanos < 0; }
    UInt32 hours() const;
    UInt32 minutes() const;
    UInt32 seconds() const;
    UInt32 microsecond() const;
};
} // namespace DB
