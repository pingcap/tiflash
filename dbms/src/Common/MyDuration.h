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
    UInt8 sign;
    Int16 hour;
    UInt8 minute;
    UInt8 second;
    UInt32 micro_second; // ms second <= 999999

    MyDuration() = default;
    explicit MyDuration(Int64 packed);
    MyDuration(UInt8 sign_, Int16 hour_, UInt8 minute_, UInt8 second_, UInt32 micro_second_);
};
} // namespace DB
