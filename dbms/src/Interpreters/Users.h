// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/dbms/src/Interpreters/Users.h
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

#include <Core/Types.h>


namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
} // namespace Poco


namespace DB
{
/** User settings used by TiFlash.
  *
  * ClickHouse-style password, client-network ACLs, and allow_databases gating were removed:
  * TiFlash does not authenticate with users.xml passwords and does not restrict database
  * access per user. Transport security uses TiFlashSecurityConfig (TLS/mTLS).
  * User still carries profile / quota for settings and resource limits.
  */
struct User
{
    String name;

    String profile;
    String quota;

    User(const String & name_);
    User(const String & name_, const String & config_elem, Poco::Util::AbstractConfiguration & config);

    constexpr static const char * DEFAULT_USER_NAME = "default";
    static const User & getDefaultUser();
};


} // namespace DB
