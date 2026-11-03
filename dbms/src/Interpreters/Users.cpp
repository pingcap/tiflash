// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/dbms/src/Interpreters/Users.cpp
//
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

#include <Interpreters/Quota.h>
#include <Interpreters/Users.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
const User & User::getDefaultUser()
{
    static User u(User::DEFAULT_USER_NAME);
    return u;
}

User::User(const String & name_)
    : name(name_)
    , profile(User::DEFAULT_USER_NAME)
    , quota(QuotaForInterval::DEFAULT_QUOTA_NAME)
{}

User::User(const String & name_, const String & config_elem, Poco::Util::AbstractConfiguration & config)
    : name(name_)
{
    // password / password_sha256_hex / networks from legacy ClickHouse users config are ignored.
    // TiFlash does not enforce those fields; see SecurityManager::authorizeAndGetUser.

    profile = config.getString(config_elem + ".profile");
    quota = config.getString(config_elem + ".quota");

    /// Fill list of allowed databases.
    const auto config_sub_elem = config_elem + ".allow_databases";
    if (config.has(config_sub_elem))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_sub_elem, config_keys);

        databases.reserve(config_keys.size());
        for (const auto & key : config_keys)
        {
            const auto database_name = config.getString(config_sub_elem + "." + key);
            databases.insert(database_name);
        }
    }
}


} // namespace DB
