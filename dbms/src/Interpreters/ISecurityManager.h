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
namespace Util
{
class AbstractConfiguration;
}
} // namespace Poco

namespace DB
{
/** Duties of security manager in TiFlash:
  * Resolve the built-in "default" user (profile, quota). Legacy users / users.*
  * entries in users config are not loaded.
  *
  * Password / client-IP authentication and allow_databases gating from ClickHouse
  * were removed as unused for TiFlash. Callers that still pass a password for
  * protocol compatibility (Context::setUser) do not go through SecurityManager.
  */
class ISecurityManager
{
public:
    using UserPtr = std::shared_ptr<const User>;

    /// Install / refresh known users. Config may still carry legacy users.*; ignored.
    virtual void loadFromConfig(Poco::Util::AbstractConfiguration & config) = 0;

    /// Look up a user by name (profile / quota); no auth checks.
    virtual UserPtr getUser(const String & user_name) const = 0;

    virtual ~ISecurityManager() {}
};

} // namespace DB
