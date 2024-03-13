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

#include <Common/FmtUtils.h>
#include <Common/MyDuration.h>

UInt32* YmdArray(UInt32 y, UInt32 m, UInt32 d) {
    // auto *p = reinterpret_cast<UInt32*>(malloc(3 * sizeof(UInt32) - 1));
    auto *p = reinterpret_cast<UInt32*>(malloc(3 * sizeof(UInt32)));
    p[0] = y;
    p[1] = m;
    p[2] = d;
    return p;
}

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
    return (std::abs(nanos) / NANOS_PER_MINUTE) % 60;
}

Int32 MyDuration::seconds() const
{
    return (std::abs(nanos) / NANOS_PER_SECOND) % 60;
}

Int32 MyDuration::microSecond() const
{
    return (std::abs(nanos) / NANOS_PER_MICRO) % 1000000;
}

String MyDuration::toString() const
{
    auto [sign, hour, minute, second, microsecond] = splitDuration();
    if (fsp == 0)
    {
        return fmt::format("{}{:02}:{:02}:{:02}", sign > 0 ? "" : "-", hour, minute, second);
    }
    auto fmt_str = fmt::format("{}{}{}", "{}{:02}:{:02}:{:02}.{:.", fsp, "}");
    auto frac_str = fmt::format("{:06}", microsecond);
    return FmtBuffer().fmtAppend(fmt::runtime(fmt_str), sign > 0 ? "" : "-", hour, minute, second, frac_str).toString();
}
} // namespace DB
