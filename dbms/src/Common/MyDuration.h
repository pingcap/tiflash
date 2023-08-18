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

#include <common/DateLUTImpl.h>
#include <common/ErrorHandlers.h>
#include <fmt/format.h>

namespace DB
{
struct DurationParts
{
    Int64 neg, hour, minute, second, microsecond;
};

namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

class MyDuration
{
private:
    static constexpr Int64 NANO_SECOND = 1;
    static constexpr Int64 NANOS_PER_MICRO = 1000 * NANO_SECOND;
    static constexpr Int64 NANOS_PER_MILLI = 1000 * NANOS_PER_MICRO;
    static constexpr Int64 NANOS_PER_SECOND = 1000 * NANOS_PER_MILLI;
    static constexpr Int64 NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;
    static constexpr Int64 NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE;

    static const int8_t DefaultFsp = 6;

    Int64 nanos;
    UInt8 fsp;

public:
    static constexpr Int64 MAX_HOUR_PART = 838;
    static constexpr Int64 MAX_MINUTE_PART = 59;
    static constexpr Int64 MAX_SECOND_PART = 59;
    static constexpr Int64 MAX_MICRO_PART = 999999;
    static constexpr Int64 MAX_NANOS = MAX_HOUR_PART * NANOS_PER_HOUR + MAX_MINUTE_PART * NANOS_PER_MINUTE
        + MAX_SECOND_PART * NANOS_PER_SECOND + MAX_MICRO_PART * NANOS_PER_MICRO;
    static_assert(MAX_NANOS > 0);

    MyDuration() = default;
    explicit MyDuration(Int64 nanos_)
        : nanos(nanos_)
        , fsp(DefaultFsp)
    {
        if (nanos_ > MAX_NANOS || nanos_ < -MAX_NANOS)
        {
            throw Exception(
                fmt::format("nanos must >= {} and <= {}", -MAX_NANOS, MAX_NANOS),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
    }
    MyDuration(Int64 nanos_, UInt8 fsp_)
        : nanos(nanos_)
        , fsp(fsp_)
    {
        if (nanos_ > MAX_NANOS || nanos_ < -MAX_NANOS)
        {
            throw Exception(
                fmt::format("nanos must >= {} and <= {}", -MAX_NANOS, MAX_NANOS),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
        if (fsp > 6 || fsp < 0)
            throw Exception("fsp must >= 0 and <= 6", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }
    MyDuration(Int32 neg, Int32 hour, Int32 minute, Int32 second, Int32 microsecond, UInt8 fsp)
        : MyDuration(
            neg
                * (hour * NANOS_PER_HOUR + minute * NANOS_PER_MINUTE + second * NANOS_PER_SECOND
                   + microsecond * NANOS_PER_MICRO),
            fsp)
    {
        if (fsp > 6 || fsp < 0)
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
    Int64 nanoSecond() const { return nanos; };

    String toString() const;
};
} // namespace DB
