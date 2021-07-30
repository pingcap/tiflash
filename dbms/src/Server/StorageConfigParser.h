#pragma once

#include <Core/Types.h>

#include <tuple>
#include <vector>

namespace Poco
{
class Logger;
namespace Util
{
class LayeredConfiguration;
}
} // namespace Poco

namespace DB
{

struct StorageIORateLimitConfig
{
public:
    enum class IORateLimitMode
    {
        WRITE_ONLY = 1,
    };
    UInt64 max_bytes_per_sec;
    IORateLimitMode mode;
    UInt32 fg_write_weight;
    UInt32 bg_write_weight;
    
    StorageIORateLimitConfig() : max_bytes_per_sec(0), mode(IORateLimitMode::WRITE_ONLY), 
        fg_write_weight(1), bg_write_weight(5) {}
    
    void parse(const String& storage_io_rate_limit, Poco::Logger* log);

    std::string toString() const;

    UInt64 getFgWriteMaxBytesPerSec() const;
    UInt64 getBgWriteMaxBytesPerSec() const;

    bool operator ==(const StorageIORateLimitConfig& config) const;
};

struct TiFlashStorageConfig
{
public:
    Strings main_data_paths;
    std::vector<size_t> main_capacity_quota;
    Strings latest_data_paths;
    std::vector<size_t> latest_capacity_quota;
    Strings kvstore_data_path;

    UInt64 bg_task_io_rate_limit = 0;
    UInt64 format_version = 0;
    bool lazily_init_store = false;
public:
    TiFlashStorageConfig() {}

    Strings getAllNormalPaths() const;

    static std::tuple<size_t, TiFlashStorageConfig> parseSettings(Poco::Util::LayeredConfiguration & config, Poco::Logger * log);

private:
    void parse(const String & storage_section, Poco::Logger * log);

    bool parseFromDeprecatedConfiguration(Poco::Util::LayeredConfiguration & config, Poco::Logger * log);
};


} // namespace DB
