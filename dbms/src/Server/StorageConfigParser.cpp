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

/// Suppress gcc warning: ‘*((void*)&<anonymous> +4)’ may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <cpptoml.h>
#if !__clang__
#pragma GCC diagnostic pop
#endif

#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Path.h>
#include <Poco/String.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/StorageConfigParser.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <set>
#include <sstream>
#include <tuple>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
} // namespace ErrorCodes

static std::string getCanonicalPath(std::string path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception("path configuration parameter is empty");
    if (path.back() != '/')
        path += '/';
    return path;
}

static String getNormalizedPath(const String & s)
{
    return getCanonicalPath(Poco::Path{s}.toString());
}

void TiFlashStorageConfig::parseStoragePath(const String & storage, Poco::Logger * log)
{
    std::istringstream ss(storage);
    cpptoml::parser p(ss);
    auto table = p.parse();

    auto get_checked_qualified_array = [log](const std::shared_ptr<cpptoml::table> table, const char * key) -> cpptoml::option<Strings> {
        auto throw_invalid_value = [log, key]() {
            String error_msg = fmt::format("The configuration \"storage.{}\" should be an array of strings. Please check your configuration file.", key);
            LOG_FMT_ERROR(log, "{}", error_msg);
            throw Exception(error_msg, ErrorCodes::INVALID_CONFIG_PARAMETER);
        };
        // not exist key
        if (!table->contains_qualified(key))
            return cpptoml::option<Strings>();

        // key exist, but not array
        auto qualified_ptr = table->get_qualified(key);
        if (!qualified_ptr->is_array())
        {
            throw_invalid_value();
        }
        // key exist, but can not convert to string array, maybe it is an int array
        auto string_array = table->get_qualified_array_of<String>(key);
        if (!string_array)
        {
            throw_invalid_value();
        }
        return string_array;
    };

    // main
    if (auto main_paths = get_checked_qualified_array(table, "main.dir"); main_paths)
        main_data_paths = *main_paths;
    if (auto main_capacity = table->get_qualified_array_of<int64_t>("main.capacity"); main_capacity)
    {
        for (const auto & c : *main_capacity)
            main_capacity_quota.emplace_back(static_cast<size_t>(c));
    }
    if (main_data_paths.empty())
    {
        String error_msg = "The configuration \"storage.main.dir\" is empty. Please check your configuration file.";
        LOG_FMT_ERROR(log, "{}", error_msg);
        throw Exception(error_msg, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
    if (!main_capacity_quota.empty() && main_capacity_quota.size() != main_data_paths.size())
    {
        String error_msg = fmt::format(
            "The array size of \"storage.main.dir\"[size={}] "
            "is not equal to \"storage.main.capacity\"[size={}]. "
            "Please check your configuration file.",
            main_data_paths.size(),
            main_capacity_quota.size());
        LOG_FMT_ERROR(log, "{}", error_msg);
        throw Exception(error_msg, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
    for (size_t i = 0; i < main_data_paths.size(); ++i)
    {
        // normalized
        main_data_paths[i] = getNormalizedPath(main_data_paths[i]);
        if (main_capacity_quota.size() <= i)
            main_capacity_quota.emplace_back(0);
        LOG_FMT_INFO(log, "Main data candidate path: {}, capacity_quota: {}", main_data_paths[i], main_capacity_quota[i]);
    }

    // latest
    if (auto latest_paths = get_checked_qualified_array(table, "latest.dir"); latest_paths)
        latest_data_paths = *latest_paths;
    if (auto latest_capacity = table->get_qualified_array_of<int64_t>("latest.capacity"); latest_capacity)
    {
        for (const auto & c : *latest_capacity)
            latest_capacity_quota.emplace_back(static_cast<size_t>(c));
    }
    // If it is empty, use the same dir as "main.dir"
    if (latest_data_paths.empty())
    {
        LOG_FMT_INFO(log, "The configuration \"storage.latest.dir\" is empty, use the same dir and capacity of \"storage.main.dir\"");
        latest_data_paths = main_data_paths;
        latest_capacity_quota = main_capacity_quota;
    }
    if (!latest_capacity_quota.empty() && latest_capacity_quota.size() != latest_data_paths.size())
    {
        String error_msg = fmt::format(
            "The array size of \"storage.latest.dir\"[size={}] "
            "is not equal to \"storage.latest.capacity\"[size={}]. "
            "Please check your configuration file.",
            latest_data_paths.size(),
            latest_capacity_quota.size());
        LOG_FMT_ERROR(log, "{}", error_msg);
        throw Exception(error_msg, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
    for (size_t i = 0; i < latest_data_paths.size(); ++i)
    {
        // normalized
        latest_data_paths[i] = getNormalizedPath(latest_data_paths[i]);
        if (latest_capacity_quota.size() <= i)
            latest_capacity_quota.emplace_back(0);
        LOG_FMT_INFO(log, "Latest data candidate path: {}, capacity_quota: {}", latest_data_paths[i], latest_capacity_quota[i]);
    }

    // Raft
    if (auto kvstore_paths = get_checked_qualified_array(table, "raft.dir"); kvstore_paths)
        kvstore_data_path = *kvstore_paths;
    if (kvstore_data_path.empty())
    {
        // generated from latest path
        for (const auto & s : latest_data_paths)
        {
            String path = Poco::Path{s + "/kvstore"}.toString();
            kvstore_data_path.emplace_back(std::move(path));
        }
    }
    for (auto & path : kvstore_data_path)
    {
        // normalized
        path = getNormalizedPath(path);
        LOG_FMT_INFO(log, "Raft data candidate path: {}", path);
    }
}

void TiFlashStorageConfig::parseMisc(const String & storage_section, Poco::Logger * log)
{
    std::istringstream ss(storage_section);
    cpptoml::parser p(ss);
    auto table = p.parse();

    if (table->contains("bg_task_io_rate_limit"))
    {
        LOG_FMT_WARNING(log, "The configuration \"bg_task_io_rate_limit\" is deprecated. Check [storage.io_rate_limit] section for new style.");
    }

    if (auto version = table->get_qualified_as<UInt64>("format_version"); version)
    {
        format_version = *version;
    }

    auto get_bool_config_or_default = [&](const String & name, bool default_value) {
        if (auto value = table->get_qualified_as<Int32>(name); value)
        {
            return (*value != 0);
        }
        else if (auto value_b = table->get_qualified_as<bool>(name); value_b)
        {
            return *value_b;
        }
        else
        {
            return default_value;
        }
    };

    lazily_init_store = get_bool_config_or_default("lazily_init_store", lazily_init_store);

    LOG_FMT_INFO(log, "format_version {} lazily_init_store {}", format_version, lazily_init_store);
}

Strings TiFlashStorageConfig::getAllNormalPaths() const
{
    Strings all_normal_path;
    std::set<String> path_set;
    for (const auto & s : main_data_paths)
        path_set.insert(s);
    for (const auto & s : latest_data_paths)
        path_set.insert(s);
    // keep the first path
    all_normal_path.emplace_back(latest_data_paths[0]);
    path_set.erase(latest_data_paths[0]);
    for (const auto & s : path_set)
        all_normal_path.emplace_back(s);
    return all_normal_path;
}

bool TiFlashStorageConfig::parseFromDeprecatedConfiguration(Poco::Util::LayeredConfiguration & config, Poco::Logger * log)
{
    if (!config.has("path"))
        return false;

    LOG_FMT_WARNING(log, "The configuration \"path\" is deprecated. Check [storage] section for new style.");

    String paths = config.getString("path");
    Poco::trimInPlace(paths);
    if (paths.empty())
        throw Exception(
            fmt::format("The configuration \"path\" is empty! [path={}]", config.getString("path")),
            ErrorCodes::INVALID_CONFIG_PARAMETER);
    Strings all_normal_path;
    Poco::StringTokenizer string_tokens(paths, ",");
    for (const auto & string_token : string_tokens)
    {
        all_normal_path.emplace_back(getNormalizedPath(string_token));
    }

    // If you set `path_realtime_mode` to `true` and multiple directories are deployed in the path, the latest data is stored in the first directory and older data is stored in the rest directories.
    bool path_realtime_mode = config.getBool("path_realtime_mode", false);
    for (size_t i = 0; i < all_normal_path.size(); ++i)
    {
        const String p = Poco::Path{all_normal_path[i]}.toString();
        // Only use the first path for storing latest data
        if (i == 0)
            latest_data_paths.emplace_back(p);
        if (path_realtime_mode)
        {
            if (i != 0)
                main_data_paths.emplace_back(p);
        }
        else
        {
            main_data_paths.emplace_back(p);
        }
    }

    {
        // kvstore_path
        String str_kvstore_path;
        if (config.has("raft.kvstore_path"))
        {
            LOG_FMT_WARNING(log, "The configuration \"raft.kvstore_path\" is deprecated. Check [storage.raft] section for new style.");
            str_kvstore_path = config.getString("raft.kvstore_path");
        }
        if (str_kvstore_path.empty())
        {
            str_kvstore_path = all_normal_path[0] + "/kvstore";
        }
        str_kvstore_path = getNormalizedPath(str_kvstore_path);
        kvstore_data_path.emplace_back(str_kvstore_path);
    }

    // Ensure these vars are clear
    main_capacity_quota.clear();
    latest_capacity_quota.clear();

    // logging
    for (const auto & s : main_data_paths)
        LOG_FMT_INFO(log, "Main data candidate path: {}", s);
    for (const auto & s : latest_data_paths)
        LOG_FMT_INFO(log, "Latest data candidate path: {}", s);
    for (const auto & s : kvstore_data_path)
        LOG_FMT_INFO(log, "Raft data candidate path: {}", s);
    return true;
}

std::tuple<size_t, TiFlashStorageConfig> TiFlashStorageConfig::parseSettings(Poco::Util::LayeredConfiguration & config, Poco::Logger * log)
{
    size_t global_capacity_quota = 0; // "0" by default, means no quota, use the whole disk capacity.
    TiFlashStorageConfig storage_config;

    // Always try to parse storage miscellaneous configuration when [storage] section exist.
    if (config.has("storage"))
    {
        storage_config.parseMisc(config.getString("storage"), log);
    }

    if (config.has("storage.main"))
    {
        if (config.has("path"))
            LOG_FMT_WARNING(log, "The configuration \"path\" is ignored when \"storage\" is defined.");
        if (config.has("capacity"))
            LOG_FMT_WARNING(log, "The configuration \"capacity\" is ignored when \"storage\" is defined.");

        storage_config.parseStoragePath(config.getString("storage"), log);

        if (config.has("raft.kvstore_path"))
        {
            Strings & kvstore_paths = storage_config.kvstore_data_path;
            String deprecated_kvstore_path = config.getString("raft.kvstore_path");
            if (!deprecated_kvstore_path.empty())
            {
                LOG_FMT_WARNING(log, "The configuration \"raft.kvstore_path\" is deprecated. Check \"storage.raft.dir\" for new style.");
                kvstore_paths.clear();
                kvstore_paths.emplace_back(getNormalizedPath(deprecated_kvstore_path));
                for (auto & kvstore_path : kvstore_paths)
                {
                    LOG_FMT_WARNING(
                        log,
                        "Raft data candidate path: {}. "
                        "The path is overwritten by deprecated configuration for backward compatibility.",
                        kvstore_path);
                }
            }
        }
    }
    else
    {
        // capacity
        if (config.has("capacity"))
        {
            LOG_FMT_WARNING(log, "The configuration \"capacity\" is deprecated. Check [storage] section for new style.");
            // TODO: support human readable format for capacity, mark_cache_size, minmax_index_cache_size
            // eg. 100GiB, 10MiB
            String capacities = config.getString("capacity");
            Poco::trimInPlace(capacities);
            Poco::StringTokenizer string_tokens(capacities, ",");
            size_t num_token = 0;
            for (const auto & string_token : string_tokens)
            {
                if (num_token == 0)
                {
                    global_capacity_quota = DB::parse<size_t>(string_token.data(), string_token.size());
                }
                num_token++;
            }
            if (num_token != 1)
                LOG_FMT_WARNING(log, "Only the first number in configuration \"capacity\" take effect");
            LOG_FMT_INFO(log, "The capacity limit is: {}", formatReadableSizeWithBinarySuffix(global_capacity_quota));
        }

        if (!storage_config.parseFromDeprecatedConfiguration(config, log))
        {
            // Can not parse from the deprecated configuration "path".
            String msg = "The configuration \"storage.main\" section is not defined. Please check your configuration file.";
            LOG_FMT_ERROR(log, "{}", msg);
            throw Exception(msg, ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

    return std::make_tuple(global_capacity_quota, storage_config);
}

void StorageIORateLimitConfig::parse(const String & storage_io_rate_limit, Poco::Logger * log)
{
    std::istringstream ss(storage_io_rate_limit);
    cpptoml::parser p(ss);
    auto config = p.parse();

    auto read_config = [&](const std::string & name, auto & value) {
        if (auto p = config->get_qualified_as<typename std::remove_reference<decltype(value)>::type>(name); p)
        {
            value = *p;
        }
    };

    read_config("max_bytes_per_sec", max_bytes_per_sec);
    read_config("max_read_bytes_per_sec", max_read_bytes_per_sec);
    read_config("max_write_bytes_per_sec", max_write_bytes_per_sec);
    read_config("foreground_write_weight", fg_write_weight);
    read_config("background_write_weight", bg_write_weight);
    read_config("foreground_read_weight", fg_read_weight);
    read_config("background_read_weight", bg_read_weight);
    read_config("emergency_pct", emergency_pct);
    read_config("high_pct", high_pct);
    read_config("medium_pct", medium_pct);
    read_config("tune_base", tune_base);
    read_config("min_bytes_per_sec", min_bytes_per_sec);
    read_config("auto_tune_sec", auto_tune_sec);

    use_max_bytes_per_sec = (max_read_bytes_per_sec == 0 && max_write_bytes_per_sec == 0);

    LOG_FMT_DEBUG(log, "storage.io_rate_limit {}", toString());
}

std::string StorageIORateLimitConfig::toString() const
{
    return fmt::format(
        "max_bytes_per_sec {} max_read_bytes_per_sec {} max_write_bytes_per_sec {} use_max_bytes_per_sec {} "
        "fg_write_weight {} bg_write_weight {} fg_read_weight {} bg_read_weight {} fg_write_max_bytes_per_sec {} "
        "bg_write_max_bytes_per_sec {} fg_read_max_bytes_per_sec {} bg_read_max_bytes_per_sec {} emergency_pct {} high_pct {} "
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

UInt64 StorageIORateLimitConfig::readWeight() const
{
    return fg_read_weight + bg_read_weight;
}

UInt64 StorageIORateLimitConfig::writeWeight() const
{
    return fg_write_weight + bg_write_weight;
}

UInt64 StorageIORateLimitConfig::totalWeight() const
{
    return readWeight() + writeWeight();
}

UInt64 StorageIORateLimitConfig::getFgWriteMaxBytesPerSec() const
{
    if (totalWeight() <= 0 || writeWeight() <= 0)
    {
        return 0;
    }
    return use_max_bytes_per_sec ? max_bytes_per_sec / totalWeight() * fg_write_weight
                                 : max_write_bytes_per_sec / writeWeight() * fg_write_weight;
}

UInt64 StorageIORateLimitConfig::getBgWriteMaxBytesPerSec() const
{
    if (totalWeight() <= 0 || writeWeight() <= 0)
    {
        return 0;
    }
    return use_max_bytes_per_sec ? max_bytes_per_sec / totalWeight() * bg_write_weight
                                 : max_write_bytes_per_sec / writeWeight() * bg_write_weight;
}

UInt64 StorageIORateLimitConfig::getFgReadMaxBytesPerSec() const
{
    if (totalWeight() <= 0 || readWeight() <= 0)
    {
        return 0;
    }
    return use_max_bytes_per_sec ? max_bytes_per_sec / totalWeight() * fg_read_weight
                                 : max_read_bytes_per_sec / readWeight() * fg_read_weight;
}

UInt64 StorageIORateLimitConfig::getBgReadMaxBytesPerSec() const
{
    if (totalWeight() <= 0 || readWeight() <= 0)
    {
        return 0;
    }
    return use_max_bytes_per_sec ? max_bytes_per_sec / totalWeight() * bg_read_weight
                                 : max_read_bytes_per_sec / readWeight() * bg_read_weight;
}

UInt64 StorageIORateLimitConfig::getWriteMaxBytesPerSec() const
{
    return getBgWriteMaxBytesPerSec() + getFgWriteMaxBytesPerSec();
}

UInt64 StorageIORateLimitConfig::getReadMaxBytesPerSec() const
{
    return getBgReadMaxBytesPerSec() + getFgReadMaxBytesPerSec();
}

bool StorageIORateLimitConfig::operator==(const StorageIORateLimitConfig & config) const
{
    return config.max_bytes_per_sec == max_bytes_per_sec && config.max_read_bytes_per_sec == max_read_bytes_per_sec
        && config.max_write_bytes_per_sec == max_write_bytes_per_sec && config.bg_write_weight == bg_write_weight
        && config.fg_write_weight == fg_write_weight && config.bg_read_weight == bg_read_weight && config.fg_read_weight == fg_read_weight
        && config.emergency_pct == emergency_pct && config.high_pct == high_pct && config.medium_pct == medium_pct
        && config.tune_base == tune_base && config.min_bytes_per_sec == min_bytes_per_sec && config.auto_tune_sec == auto_tune_sec;
}
} // namespace DB
