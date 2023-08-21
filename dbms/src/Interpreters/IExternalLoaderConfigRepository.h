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

#include <Poco/AutoPtr.h>
#include <Poco/Timestamp.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <set>
#include <string>

namespace DB
{
/** Repository with configurations of user-defined objects (dictionaries, models).
  * Used by ExternalLoader.
  */
class IExternalLoaderConfigRepository
{
public:
    using Files = std::set<std::string>;
    virtual Files list(const Poco::Util::AbstractConfiguration & config, const std::string & path_key) const = 0;

    virtual bool exists(const std::string & config_file) const = 0;

    virtual Poco::Timestamp getLastModificationTime(const std::string & config_file) const = 0;

    virtual Poco::AutoPtr<Poco::Util::AbstractConfiguration> load(const std::string & config_file) const = 0;

    virtual ~IExternalLoaderConfigRepository() {}
};

} // namespace DB
