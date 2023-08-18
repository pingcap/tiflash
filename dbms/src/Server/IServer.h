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

#include <Common/TiFlashSecurity.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace DB
{

class IServer
{
public:
    /// Returns the application's configuration.
    virtual Poco::Util::LayeredConfiguration & config() const = 0;

    /// Returns the application's logger.
    virtual Poco::Logger & logger() const = 0;

    /// Returns global application's context.
    virtual Context & context() const = 0;

    /// Returns true if shutdown signaled.
    virtual bool isCancelled() const = 0;

    virtual ~IServer() {}
};

} // namespace DB
