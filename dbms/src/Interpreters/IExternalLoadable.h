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

#include <chrono>
#include <memory>
#include <string>


namespace Poco::Util
{
class AbstractConfiguration;
}


namespace DB
{
/// Min and max lifetimes for a loadable object or it's entry
struct ExternalLoadableLifetime final
{
    UInt64 min_sec;
    UInt64 max_sec;

    ExternalLoadableLifetime(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};


/// Basic interface for external loadable objects. Is used in ExternalLoader.
class IExternalLoadable : public std::enable_shared_from_this<IExternalLoadable>
{
public:
    virtual ~IExternalLoadable() = default;

    virtual const ExternalLoadableLifetime & getLifetime() const = 0;

    virtual std::string getName() const = 0;
    /// True if object can be updated when lifetime exceeded.
    virtual bool supportUpdates() const = 0;
    /// If lifetime exceeded and isModified() ExternalLoader replace current object with the result of clone().
    virtual bool isModified() const = 0;
    /// Returns new object with the same configuration. Is used to update modified object when lifetime exceeded.
    virtual std::unique_ptr<IExternalLoadable> clone() const = 0;

    virtual std::chrono::time_point<std::chrono::system_clock> getCreationTime() const = 0;

    virtual std::exception_ptr getCreationException() const = 0;
};

} // namespace DB
