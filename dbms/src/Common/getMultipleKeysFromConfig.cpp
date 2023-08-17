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

#include <Common/StringUtils/StringUtils.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
std::vector<std::string> getMultipleKeysFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & root,
    const std::string & name)
{
    std::vector<std::string> values;
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(root, config_keys);
    for (const auto & key : config_keys)
    {
        if (key != name && !(startsWith(key, name + "[") && endsWith(key, "]")))
            continue;
        values.emplace_back(key);
    }
    return values;
}


std::vector<std::string> getMultipleValuesFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & root,
    const std::string & name)
{
    std::vector<std::string> values;
    for (const auto & key : DB::getMultipleKeysFromConfig(config, root, name))
        values.emplace_back(config.getString(key));
    return values;
}

} // namespace DB
