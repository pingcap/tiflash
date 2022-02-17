// Copyright 2022 PingCAP, Ltd.
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

#include <Common/MyDuration.h>
#include <fmt/format.h>

namespace DB
{
DurationParts MyDuration::splitDuration() const
{
    Int64 sign = 1;
    Int64 t = nanos;
    if (t < 0)
    {
        t = -t;
        sign = -1;
    }
    Int64 hours = t / NANOS_PER_HOUR;
    t -= hours * NANOS_PER_HOUR;
    Int64 minutes = t / NANOS_PER_MINUTE;
    t -= minutes * NANOS_PER_MINUTE;
    Int64 seconds = t / NANOS_PER_SECOND;
    t -= seconds * NANOS_PER_SECOND;
    Int64 micro_seconds = t / NANOS_PER_MICRO;
    return DurationParts{sign, hours, minutes, seconds, micro_seconds};
}

Int32 MyDuration::hours() const
{
    return std::abs(nanos) / NANOS_PER_HOUR;
}

Int32 MyDuration::minutes() const
{
    return std::abs(nanos) / NANOS_PER_MINUTE % 60;
}

Int32 MyDuration::seconds() const
{
    return std::abs(nanos) / NANOS_PER_SECOND % 60;
}

Int32 MyDuration::microSecond() const
{
    return std::abs(nanos) / NANOS_PER_MICRO % 1000000;
}

String MyDuration::toString() const
{
    auto [sign, hour, minute, second, microsecond] = splitDuration();
    if (fsp == 0)
    {
        return fmt::format("{}{:02}:{:02}:{:02}", sign > 0 ? "" : "-", hour, minute, second);
    }
    auto frac_str = fmt::format("{:06}", microsecond);
    // "{:.2}" will keep the left most 2 char
    // fmt::format("{:.2}", "0123") -> "01"
    switch (fsp)
    {
    case 0:
        return fmt::format("{}{:02}:{:02}:{:02}.{:.0}", sign > 0 ? "" : "-", hour, minute, second, frac_str);
    case 1:
        return fmt::format("{}{:02}:{:02}:{:02}.{:.1}", sign > 0 ? "" : "-", hour, minute, second, frac_str);
    case 2:
        return fmt::format("{}{:02}:{:02}:{:02}.{:.2}", sign > 0 ? "" : "-", hour, minute, second, frac_str);
    case 3:
        return fmt::format("{}{:02}:{:02}:{:02}.{:.3}", sign > 0 ? "" : "-", hour, minute, second, frac_str);
    case 4:
        return fmt::format("{}{:02}:{:02}:{:02}.{:.4}", sign > 0 ? "" : "-", hour, minute, second, frac_str);
    case 5:
        return fmt::format("{}{:02}:{:02}:{:02}.{:.5}", sign > 0 ? "" : "-", hour, minute, second, frac_str);
    case 6:
        return fmt::format("{}{:02}:{:02}:{:02}.{:.6}", sign > 0 ? "" : "-", hour, minute, second, frac_str);
    default:
        throw DB::Exception(fmt::format("invalid precision for MyDuration [fsp={}]", fsp));
    }
}
} // namespace DB
