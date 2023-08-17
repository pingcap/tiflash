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

namespace DB
{
/** Duties of security manager:
  * 1) Authenticate users
  * 2) Provide user settings (profile, quota, ACLs)
  * 3) Grant access to databases
  */
class ISecurityManager
{
public:
    using UserPtr = std::shared_ptr<const User>;

    virtual void loadFromConfig(Poco::Util::AbstractConfiguration & config) = 0;

    /// Find user and make authorize checks
    virtual UserPtr authorizeAndGetUser(
        const String & user_name,
        const String & password,
        const Poco::Net::IPAddress & address) const = 0;

    /// Just find user
    virtual UserPtr getUser(const String & user_name) const = 0;

    /// Check if the user has access to the database.
    virtual bool hasAccessToDatabase(const String & user_name, const String & database_name) const = 0;

    virtual ~ISecurityManager() {}
};

} // namespace DB
