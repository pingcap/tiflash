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

#include <Common/Logger.h>
#include <IO/BaseFile/IORateLimitConfig.h>

#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <cpptoml.h>
#if !__clang__
#pragma GCC diagnostic pop
#endif

namespace
{
template <typename T>
void readConfig(const std::shared_ptr<cpptoml::table> & table, const String & name, T & value)
{
#ifndef NDEBUG
    if (!table->contains_qualified(name))
        return;
#endif
    if (auto p = table->get_qualified_as<typename std::remove_reference<decltype(value)>::type>(name); p)
    {
        value = *p;
    }
}
} // namespace

namespace DB
{

void IORateLimitConfig::parse(const String & storage_io_rate_limit, const LoggerPtr & log)
{
    std::istringstream ss(storage_io_rate_limit);
    cpptoml::parser p(ss);
    auto config = p.parse();

    readConfig(config, "max_bytes_per_sec", max_bytes_per_sec);
    readConfig(config, "max_read_bytes_per_sec", max_read_bytes_per_sec);
    readConfig(config, "max_write_bytes_per_sec", max_write_bytes_per_sec);
    readConfig(config, "foreground_write_weight", fg_write_weight);
    readConfig(config, "background_write_weight", bg_write_weight);
    readConfig(config, "foreground_read_weight", fg_read_weight);
    readConfig(config, "background_read_weight", bg_read_weight);
    readConfig(config, "emergency_pct", emergency_pct);
    readConfig(config, "high_pct", high_pct);
    readConfig(config, "medium_pct", medium_pct);
    readConfig(config, "tune_base", tune_base);
    readConfig(config, "min_bytes_per_sec", min_bytes_per_sec);
    readConfig(config, "auto_tune_sec", auto_tune_sec);

    use_max_bytes_per_sec = (max_read_bytes_per_sec == 0 && max_write_bytes_per_sec == 0);

    LOG_DEBUG(log, "storage.io_rate_limit {}", toString());
}

std::string IORateLimitConfig::toString() const
{
    return fmt::format(
        "max_bytes_per_sec {} max_read_bytes_per_sec {} max_write_bytes_per_sec {} use_max_bytes_per_sec {} "
        "fg_write_weight {} bg_write_weight {} fg_read_weight {} bg_read_weight {} fg_write_max_bytes_per_sec {} "
        "bg_write_max_bytes_per_sec {} fg_read_max_bytes_per_sec {} bg_read_max_bytes_per_sec {} emergency_pct {} "
        "high_pct {} "
        "medium_pct {} tune_base {} min_bytes_per_sec {} auto_tune_sec {}",
        max_bytes_per_sec,
        max_read_bytes_per_sec,
        max_write_bytes_per_sec,
        use_max_bytes_per_sec,
        fg_write_weight,
        bg_write_weight,
        fg_read_weight,
        bg_read_weight,
        getFgWriteMaxBytesPerSec(),
        getBgWriteMaxBytesPerSec(),
        getFgReadMaxBytesPerSec(),
        getBgReadMaxBytesPerSec(),
        emergency_pct,
        high_pct,
        medium_pct,
        tune_base,
        min_bytes_per_sec,
        auto_tune_sec);
}

UInt64 IORateLimitConfig::readWeight() const
{
    return fg_read_weight + bg_read_weight;
}

UInt64 IORateLimitConfig::writeWeight() const
{
    return fg_write_weight + bg_write_weight;
}

UInt64 IORateLimitConfig::totalWeight() const
{
    return readWeight() + writeWeight();
}

UInt64 IORateLimitConfig::getFgWriteMaxBytesPerSec() const
{
    if (writeWeight() <= 0 || totalWeight() <= 0)
    {
        return 0;
    }
    return use_max_bytes_per_sec ? static_cast<UInt64>(1.0 * max_bytes_per_sec / totalWeight() * fg_write_weight)
                                 : static_cast<UInt64>(1.0 * max_write_bytes_per_sec / writeWeight() * fg_write_weight);
}

UInt64 IORateLimitConfig::getBgWriteMaxBytesPerSec() const
{
    if (writeWeight() <= 0 || totalWeight() <= 0)
    {
        return 0;
    }
    return use_max_bytes_per_sec ? static_cast<UInt64>(1.0 * max_bytes_per_sec / totalWeight() * bg_write_weight)
                                 : static_cast<UInt64>(1.0 * max_write_bytes_per_sec / writeWeight() * bg_write_weight);
}

UInt64 IORateLimitConfig::getFgReadMaxBytesPerSec() const
{
    if (readWeight() <= 0 || totalWeight() <= 0)
    {
        return 0;
    }
    return use_max_bytes_per_sec ? static_cast<UInt64>(1.0 * max_bytes_per_sec / totalWeight() * fg_read_weight)
                                 : static_cast<UInt64>(1.0 * max_read_bytes_per_sec / readWeight() * fg_read_weight);
}

UInt64 IORateLimitConfig::getBgReadMaxBytesPerSec() const
{
    if (readWeight() <= 0 || totalWeight() <= 0)
    {
        return 0;
    }
    return use_max_bytes_per_sec ? static_cast<UInt64>(1.0 * max_bytes_per_sec / totalWeight() * bg_read_weight)
                                 : static_cast<UInt64>(1.0 * max_read_bytes_per_sec / readWeight() * bg_read_weight);
}

UInt64 IORateLimitConfig::getWriteMaxBytesPerSec() const
{
    return getBgWriteMaxBytesPerSec() + getFgWriteMaxBytesPerSec();
}

UInt64 IORateLimitConfig::getReadMaxBytesPerSec() const
{
    return getBgReadMaxBytesPerSec() + getFgReadMaxBytesPerSec();
}

bool IORateLimitConfig::operator==(const IORateLimitConfig & config) const
{
    return config.max_bytes_per_sec == max_bytes_per_sec && config.max_read_bytes_per_sec == max_read_bytes_per_sec
        && config.max_write_bytes_per_sec == max_write_bytes_per_sec && config.bg_write_weight == bg_write_weight
        && config.fg_write_weight == fg_write_weight && config.bg_read_weight == bg_read_weight
        && config.fg_read_weight == fg_read_weight && config.emergency_pct == emergency_pct
        && config.high_pct == high_pct && config.medium_pct == medium_pct && config.tune_base == tune_base
        && config.min_bytes_per_sec == min_bytes_per_sec && config.auto_tune_sec == auto_tune_sec;
}

} // namespace DB