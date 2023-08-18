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

#include <Common/Logger.h>
#include <Core/Types.h>

#include <tuple>
#include <vector>

namespace Poco::Util
{
class LayeredConfiguration;
} // namespace Poco::Util

namespace DB
{
struct StorageIORateLimitConfig
{
public:
    // For disk that read bandwidth and write bandwith are calculated together, such as AWS's EBS.
    UInt64 max_bytes_per_sec;
    // For disk that read bandwidth and write bandwith are calculated separatly, such as GCP's persistent disks.
    UInt64 max_read_bytes_per_sec;
    UInt64 max_write_bytes_per_sec;

    bool use_max_bytes_per_sec;

    // Currently, IORateLimiter supports 4 I/O type: foreground write, foreground read, background write and background read.
    // Initially, We calculate bandwidth for each I/O type according to the proportion of weights.
    // If *_weight is 0, the corresponding I/O type is not limited.
    UInt32 fg_write_weight;
    UInt32 bg_write_weight;
    UInt32 fg_read_weight;
    UInt32 bg_read_weight;

    Int32 emergency_pct;
    Int32 high_pct;
    Int32 medium_pct;

    Int32 tune_base;
    Int64 min_bytes_per_sec;

    Int32 auto_tune_sec;

    StorageIORateLimitConfig()
        : max_bytes_per_sec(0)
        , max_read_bytes_per_sec(0)
        , max_write_bytes_per_sec(0)
        , use_max_bytes_per_sec(true)
        , fg_write_weight(25)
        , bg_write_weight(25)
        , fg_read_weight(25)
        , bg_read_weight(25)
        , emergency_pct(96)
        , high_pct(85)
        , medium_pct(60)
        , tune_base(2)
        , min_bytes_per_sec(2 * 1024 * 1024)
        , auto_tune_sec(5)
    {}

    void parse(const String & storage_io_rate_limit, const LoggerPtr & log);

    std::string toString() const;

    UInt64 getFgWriteMaxBytesPerSec() const;
    UInt64 getBgWriteMaxBytesPerSec() const;
    UInt64 getFgReadMaxBytesPerSec() const;
    UInt64 getBgReadMaxBytesPerSec() const;
    UInt64 getWriteMaxBytesPerSec() const;
    UInt64 getReadMaxBytesPerSec() const;
    UInt64 readWeight() const;
    UInt64 writeWeight() const;
    UInt64 totalWeight() const;

    bool operator==(const StorageIORateLimitConfig & config) const;
};

struct StorageS3Config
{
    bool is_enabled = false;

    // verbose logging for http requests. Use for debugging
    bool verbose = false;

    bool enable_http_pool = true; // will be removed after testing
    bool enable_poco_client = true; // will be removed after testing

    String endpoint;
    String bucket;
    String access_key_id;
    String secret_access_key;
    UInt64 max_connections = 4096;
    UInt64 connection_timeout_ms = 1000;
    UInt64 request_timeout_ms = 30000;
    UInt64 max_redirections = 10;
    String root;

    inline static String S3_ACCESS_KEY_ID = "S3_ACCESS_KEY_ID";
    inline static String S3_SECRET_ACCESS_KEY = "S3_SECRET_ACCESS_KEY";

    void parse(const String & content);
    void enable(bool check_requirements, const LoggerPtr & log);
    bool isS3Enabled() const;

    String toString() const;
};

struct StorageRemoteCacheConfig
{
    String dir;
    UInt64 capacity = 0;
    UInt64 dtfile_level = 100;
    double delta_rate = 0.1;
    double reserved_rate = 0.1;

    bool isCacheEnabled() const;
    void initCacheDir() const;
    String getDTFileCacheDir() const;
    String getPageCacheDir() const;
    UInt64 getDTFileCapacity() const;
    UInt64 getPageCapacity() const;
    UInt64 getReservedCapacity() const;
    void parse(const String & content, const LoggerPtr & log);

    std::pair<Strings, std::vector<size_t>> getCacheDirInfos(bool is_compute_mode) const;
};

struct TiFlashStorageConfig
{
public:
    Strings main_data_paths;
    std::vector<size_t> main_capacity_quota;
    Strings latest_data_paths;
    std::vector<size_t> latest_capacity_quota;
    Strings kvstore_data_path;

    UInt64 format_version = 0;
    bool lazily_init_store = true;
    UInt64 api_version = 1;

    StorageS3Config s3_config;
    StorageRemoteCacheConfig remote_cache_config;

public:
    TiFlashStorageConfig() = default;

    Strings getAllNormalPaths() const;

    static std::tuple<size_t, TiFlashStorageConfig> parseSettings(Poco::Util::LayeredConfiguration & config, const LoggerPtr & log);

private:
    void parseStoragePath(const String & storage_section, const LoggerPtr & log);

    bool parseFromDeprecatedConfiguration(Poco::Util::LayeredConfiguration & config, const LoggerPtr & log);

    void parseMisc(const String & storage_section, const LoggerPtr & log);
};


} // namespace DB
