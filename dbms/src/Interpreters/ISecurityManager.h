// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/dbms/src/Interpreters/ISecurityManager.h
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

#pragma once

#include <Interpreters/Users.h>

namespace Poco
{
namespace Net
{
class IPAddress;
}
} // namespace Poco

namespace DB
{
/** Duties of security manager in TiFlash:
  * 1) Resolve named users from users config (profile, quota, allow_databases)
  * 2) Grant access to databases via allow_databases
  *
  * Password / client-IP authentication from ClickHouse was removed as unused for TiFlash.
  * authorizeAndGetUser keeps the old signature for call-site compatibility but ignores
  * password and address; see SecurityManager.
  */
class ISecurityManager
{
public:
    using UserPtr = std::shared_ptr<const User>;

    virtual void loadFromConfig(Poco::Util::AbstractConfiguration & config) = 0;

    /// Resolve user; password and address are ignored (legacy ClickHouse auth removed).
    virtual UserPtr authorizeAndGetUser(
        const String & user_name,
        const String & password,
        const Poco::Net::IPAddress & address) const
        = 0;

    /// Just find user
    virtual UserPtr getUser(const String & user_name) const = 0;

    /// Check if the user has access to the database.
    virtual bool hasAccessToDatabase(const String & user_name, const String & database_name) const = 0;

    virtual ~ISecurityManager() {}
};

} // namespace DB
