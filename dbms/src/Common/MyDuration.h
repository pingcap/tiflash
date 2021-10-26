#pragma once

#include <common/DateLUTImpl.h>
#include <common/ErrorHandlers.h>

namespace DB
{
struct DurationParts
{
    Int32 neg, hour, minute, second, microsecond;
};

namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

class MyDuration
{
private:
    static const Int64 NANO_SECOND = 1;
    static const Int64 NANOS_PER_MICRO = 1000 * NANO_SECOND;
    static const Int64 NANOS_PER_MILLI = 1000 * NANOS_PER_MICRO;
    static const Int64 NANOS_PER_SECOND = 1000 * NANOS_PER_MILLI;
    static const Int64 NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;
    static const Int64 NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE;

    static const Int64 MAX_HOUR_PART = 838;
    static const Int64 MAX_MINUTE_PART = 59;
    static const Int64 MAX_SECOND_PART = 59;
    static const Int64 MAX_MICRO_PART = 999999;
    static const Int64 MAX_NANOS = MAX_HOUR_PART * NANOS_PER_HOUR + MAX_MINUTE_PART * NANOS_PER_MINUTE + MAX_SECOND_PART * NANOS_PER_SECOND + MAX_MICRO_PART * NANOS_PER_MICRO;

public:
    Int64 nanos;
    UInt8 fsp;

    MyDuration() = default;
    MyDuration(Int64 nanos_, UInt8 fsp_)
        : nanos(nanos_)
        , fsp(fsp_)
    {
        if (fsp > 6)
            throw Exception("fsp must >= 0 and <= 6", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }
    MyDuration(Int32 neg, Int32 hour, Int32 minute, Int32 second, Int32 microsecond, UInt8 fsp)
        : MyDuration(neg * (hour * NANOS_PER_HOUR + minute * NANOS_PER_MINUTE + second * NANOS_PER_SECOND + microsecond * NANOS_PER_MICRO), fsp)
    {
        if (fsp > 6)
            throw Exception("fsp must >= 0 and <= 6", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (minute > MAX_MINUTE_PART || minute < 0)
            throw Exception("minute must >= 0 and <= 59", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (second > MAX_SECOND_PART || second < 0)
            throw Exception("second must >= 0 and <= 59", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (microsecond > MAX_MICRO_PART || microsecond < 0)
            throw Exception("microsecond must >= 0 and <= 999999", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (hour > MAX_HOUR_PART || hour < 0)
            throw Exception("hour must >= 0 and <= 838", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    DurationParts splitDuration() const;
    bool isNeg() const { return nanos < 0; }
    Int32 hours() const;
    Int32 minutes() const;
    Int32 seconds() const;
    Int32 microSecond() const;

    String toString() const;
};
} // namespace DB
