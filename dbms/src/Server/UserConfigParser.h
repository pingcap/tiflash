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

#include <Common/Logger.h>
#include <Core/Types.h>

#include <memory>

namespace Poco
{
class Logger;
namespace Util
{
class LayeredConfiguration;
}
} // namespace Poco

namespace DB
{
class Context;
class ConfigReloader;
using ConfigReloaderPtr = std::unique_ptr<ConfigReloader>;
namespace UserConfig
{
ConfigReloaderPtr parseSettings(
    Poco::Util::LayeredConfiguration & config,
    const std::string & config_path,
    const std::unique_ptr<Context> & global_context,
    const LoggerPtr & log);

}
} // namespace DB
