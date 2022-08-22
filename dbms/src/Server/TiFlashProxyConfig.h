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

#pragma once
#include <Common/TiFlashBuildInfo.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <common/types.h>

#include <unordered_map>

namespace DB
{
struct TiFlashProxyConfig
{
    static const std::string config_prefix;
    std::vector<const char *> args;
    std::unordered_map<std::string, std::string> val_map;
    bool is_proxy_runnable = false;

    // TiFlash Proxy will set the default value of "flash.proxy.addr", so we don't need to set here.
    const String engine_store_version = "engine-version";
    const String engine_store_git_hash = "engine-git-hash";
    const String engine_store_address = "engine-addr";
    const String engine_store_advertise_address = "advertise-engine-addr";
    const String pd_endpoints = "pd-endpoints";
    const String engine_label = "engine-label";
    const String engine_label_value = "tiflash";

    explicit TiFlashProxyConfig(Poco::Util::LayeredConfiguration & config)
    {
        if (!config.has(config_prefix))
            return;

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);
        {
            std::unordered_map<std::string, std::string> args_map;
            for (const auto & key : keys)
            {
                const auto k = config_prefix + "." + key;
                args_map[key] = config.getString(k);
            }
            args_map[pd_endpoints] = config.getString("raft.pd_addr");
            args_map[engine_store_version] = TiFlashBuildInfo::getReleaseVersion();
            args_map[engine_store_git_hash] = TiFlashBuildInfo::getGitHash();
            if (!args_map.count(engine_store_address))
                args_map[engine_store_address] = config.getString("flash.service_addr");
            else
                args_map[engine_store_advertise_address] = args_map[engine_store_address];
            args_map[engine_label] = engine_label_value;

            for (auto && [k, v] : args_map)
            {
                val_map.emplace("--" + k, std::move(v));
            }
        }

        args.push_back("TiFlash Proxy");
        for (const auto & v : val_map)
        {
            args.push_back(v.first.data());
            args.push_back(v.second.data());
        }
        is_proxy_runnable = true;
    }
};

const std::string TiFlashProxyConfig::config_prefix = "flash.proxy";
} // namespace DB