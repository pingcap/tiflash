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

#include <Core/Types.h>

#include <tuple>
#include <vector>

namespace Poco::Util
{
class LayeredConfiguration;
} // namespace Poco::Util

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

struct StorageS3Config
{
    bool is_enabled = false;

    // verbose logging for http requests. Use for debugging
    bool verbose = false;

    bool enable_http_pool = false; // will be removed after testing
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
    void disable() { is_enabled = false; }

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

    static std::tuple<size_t, TiFlashStorageConfig> parseSettings(
        Poco::Util::LayeredConfiguration & config,
        const LoggerPtr & log);

private:
    void parseStoragePath(const String & storage_section, const LoggerPtr & log);

    bool parseFromDeprecatedConfiguration(Poco::Util::LayeredConfiguration & config, const LoggerPtr & log);

    void parseMisc(const String & storage_section, const LoggerPtr & log);
};


} // namespace DB
