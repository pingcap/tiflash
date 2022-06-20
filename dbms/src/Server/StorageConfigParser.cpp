/// Suppress gcc warning: ‘*((void*)&<anonymous> +4)’ may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <Common/Config/cpptoml.h>
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

static String getNormalizedPath(const String & s) { return getCanonicalPath(Poco::Path{s}.toString()); }

void TiFlashStorageConfig::parse(const String & storage, Poco::Logger * log)
{
    std::istringstream ss(storage);
    cpptoml::parser p(ss);
    auto table = p.parse();

    auto get_checked_qualified_array = [log](const std::shared_ptr<cpptoml::table> table, const char * key) -> cpptoml::option<Strings> {
        auto throw_invalid_value = [log, key]() {
            String error_msg
                = String("The configuration \"storage.") + key + "\" should be an array of strings. Please check your configuration file.";
            LOG_ERROR(log, error_msg);
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
        LOG_ERROR(log, error_msg);
        throw Exception(error_msg, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
    if (!main_capacity_quota.empty() && main_capacity_quota.size() != main_data_paths.size())
    {
        String error_msg = "The array size of \"storage.main.dir\"[size=" + toString(main_data_paths.size())
            + "] is not equal to \"storage.main.capacity\"[size=" + toString(main_capacity_quota.size())
            + "]. Please check your configuration file.";
        LOG_ERROR(log, error_msg);
        throw Exception(error_msg, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
    for (size_t i = 0; i < main_data_paths.size(); ++i)
    {
        // normalized
        main_data_paths[i] = getNormalizedPath(main_data_paths[i]);
        if (main_capacity_quota.size() <= i)
            main_capacity_quota.emplace_back(0);
        LOG_INFO(log, "Main data candidate path: " << main_data_paths[i] << ", capacity_quota: " << main_capacity_quota[i]);
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
        LOG_INFO(log, "The configuration \"storage.latest.dir\" is empty, use the same dir and capacity of \"storage.main.dir\"");
        latest_data_paths = main_data_paths;
        latest_capacity_quota = main_capacity_quota;
    }
    if (!latest_capacity_quota.empty() && latest_capacity_quota.size() != latest_data_paths.size())
    {
        String error_msg = "The array size of \"storage.latest.dir\"[size=" + toString(latest_data_paths.size())
            + "] is not equal to \"storage.latest.capacity\"[size=" + toString(latest_capacity_quota.size())
            + "]. Please check your configuration file.";
        LOG_ERROR(log, error_msg);
        throw Exception(error_msg, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
    for (size_t i = 0; i < latest_data_paths.size(); ++i)
    {
        // normalized
        latest_data_paths[i] = getNormalizedPath(latest_data_paths[i]);
        if (latest_capacity_quota.size() <= i)
            latest_capacity_quota.emplace_back(0);
        LOG_INFO(log, "Latest data candidate path: " << latest_data_paths[i] << ", capacity_quota: " << latest_capacity_quota[i]);
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
        LOG_INFO(log, "Raft data candidate path: " << path);
    }

    // rate limiter
    if (auto rate_limit = table->get_qualified_as<UInt64>("bg_task_io_rate_limit"); rate_limit)
        bg_task_io_rate_limit = *rate_limit;

    if (auto version = table->get_qualified_as<UInt64>("format_version"); version)
        format_version = *version;

    if (auto lazily_init = table->get_qualified_as<Int32>("lazily_init_store"); lazily_init)
        lazily_init_store = (*lazily_init != 0);
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

    LOG_WARNING(log, "The configuration \"path\" is deprecated. Check [storage] section for new style.");

    String paths = config.getString("path");
    Poco::trimInPlace(paths);
    if (paths.empty())
        throw Exception(
            "The configuration \"path\" is empty! [path=" + config.getString("path") + "]", ErrorCodes::INVALID_CONFIG_PARAMETER);
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
            LOG_WARNING(log, "The configuration \"raft.kvstore_path\" is deprecated. Check [storage.raft] section for new style.");
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
        LOG_INFO(log, "Main data candidate path: " << s);
    for (const auto & s : latest_data_paths)
        LOG_INFO(log, "Latest data candidate path: " << s);
    for (const auto & s : kvstore_data_path)
        LOG_INFO(log, "Raft data candidate path: " << s);
    return true;
}

std::tuple<size_t, TiFlashStorageConfig> TiFlashStorageConfig::parseSettings(Poco::Util::LayeredConfiguration & config, Poco::Logger * log)
{
    size_t global_capacity_quota = 0; // "0" by default, means no quota, use the whole disk capacity.
    TiFlashStorageConfig storage_config;

    // Always try to parse storage miscellaneous configuration when [storage] section exist.
    if (config.has("storage"))
    {
        if (config.has("path"))
            LOG_WARNING(log, "The configuration \"path\" is ignored when \"storage\" is defined.");
        if (config.has("capacity"))
            LOG_WARNING(log, "The configuration \"capacity\" is ignored when \"storage\" is defined.");

        storage_config.parse(config.getString("storage"), log);

        if (config.has("raft.kvstore_path"))
        {
            Strings & kvstore_paths = storage_config.kvstore_data_path;
            String deprecated_kvstore_path = config.getString("raft.kvstore_path");
            if (!deprecated_kvstore_path.empty())
            {
                LOG_WARNING(log, "The configuration \"raft.kvstore_path\" is deprecated. Check \"storage.raft.dir\" for new style.");
                kvstore_paths.clear();
                kvstore_paths.emplace_back(getNormalizedPath(deprecated_kvstore_path));
                for (auto & kvstore_path : kvstore_paths)
                {
                    LOG_WARNING(log,
                        "Raft data candidate path: "
                            << kvstore_path << ". The path is overwritten by deprecated configuration for backward compatibility.");
                }
            }
        }
    }
    else
    {
        // capacity
        if (config.has("capacity"))
        {
            LOG_WARNING(log, "The configuration \"capacity\" is deprecated. Check [storage] section for new style.");
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
                LOG_WARNING(log, "Only the first number in configuration \"capacity\" take effect");
            LOG_INFO(log, "The capacity limit is: " + formatReadableSizeWithBinarySuffix(global_capacity_quota));
        }

        if (!storage_config.parseFromDeprecatedConfiguration(config, log))
        {
            // Can not parse from the deprecated configuration "path".
            String msg = "The configuration \"storage.main\" section is not defined. Please check your configuration file.";
            LOG_ERROR(log, msg);
            throw Exception(msg, ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

    return std::make_tuple(global_capacity_quota, storage_config);
}

} // namespace DB
