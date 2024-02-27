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

#include <Common/Exception.h>
#include <IO/Buffer/HexWriteBuffer.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/SecurityManager.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>
#include <openssl/sha.h>

namespace DB
{
namespace ErrorCodes
{
extern const int DNS_ERROR;
extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
extern const int UNKNOWN_USER;
extern const int REQUIRED_PASSWORD;
extern const int WRONG_PASSWORD;
extern const int IP_ADDRESS_NOT_ALLOWED;
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

using UserPtr = SecurityManager::UserPtr;

void SecurityManager::loadFromConfig(Poco::Util::AbstractConfiguration & config)
{
    Container new_users;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys("users", config_keys);

    for (const std::string & key : config_keys)
    {
        auto user = std::make_shared<const User>(key, "users." + key, config);
        new_users.emplace(key, std::move(user));
    }
    // Insert a "default" user if not defined
    if (new_users.count(User::DEFAULT_USER_NAME) == 0)
        new_users.emplace(User::DEFAULT_USER_NAME, std::make_shared<const User>(User::getDefaultUser()));

    users = std::move(new_users);
}

UserPtr SecurityManager::authorizeAndGetUser(
    const String & user_name,
    const String & password,
    const Poco::Net::IPAddress & address) const
{
    auto user = getUser(user_name);

    // A shortcut for TiFlash, bypass all checks by default
    if (likely(user->password.empty()))
        return user;

    // These code path below for authentication is useless for TiFlash
    if (!user->addresses.contains(address))
        throw Exception(
            "User " + user_name + " is not allowed to connect from address " + address.toString(),
            ErrorCodes::IP_ADDRESS_NOT_ALLOWED);

    auto on_wrong_password = [&]() {
        if (password.empty())
            throw Exception("Password required for user " + user_name, ErrorCodes::REQUIRED_PASSWORD);
        else
            throw Exception("Wrong password for user " + user_name, ErrorCodes::WRONG_PASSWORD);
    };

    if (!user->password_sha256_hex.empty())
    {
        unsigned char hash[32];

        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(password.data()), password.size());
        SHA256_Final(hash, &ctx);

        String hash_hex;
        {
            WriteBufferFromString buf(hash_hex);
            HexWriteBuffer hex_buf(buf);
            hex_buf.write(reinterpret_cast<const char *>(hash), sizeof(hash));
        }

        Poco::toLowerInPlace(hash_hex);

        if (hash_hex != user->password_sha256_hex)
            on_wrong_password();
    }
    else if (password != user->password)
    {
        on_wrong_password();
    }

    return user;
}

UserPtr SecurityManager::getUser(const String & user_name) const
{
    if (auto it = users.find(user_name); likely(users.end() != it))
        return it->second;
    throw Exception("Unknown user " + user_name, ErrorCodes::UNKNOWN_USER);
}

bool SecurityManager::hasAccessToDatabase(const std::string & user_name, const std::string & database_name) const
{
    auto user = getUser(user_name);
    return user->databases.empty() || user->databases.count(database_name);
}

} // namespace DB
