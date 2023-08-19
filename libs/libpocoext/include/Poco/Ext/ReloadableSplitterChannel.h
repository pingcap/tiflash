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


#include <Poco/SplitterChannel.h>

#include <functional>

namespace Poco
{
class Logger;
namespace Util
{
class AbstractConfiguration;
}
class ReloadableSplitterChannel : public SplitterChannel
{
public:
    using SplitterChannelValidator = std::function<void(Channel &, Util::AbstractConfiguration &)>;
    void changeProperties(Util::AbstractConfiguration & config);
    // just for test now
    void setPropertiesValidator(SplitterChannelValidator validator) { properties_validator = validator; }
    void validateProperties(Util::AbstractConfiguration & expect_config)
    {
        FastMutex::ScopedLock lock(_mutex);
        for (auto it : _channels)
        {
            properties_validator(*it, expect_config);
        }
    }

protected:
    void setPropertiesRecursively(Channel & channel, Util::AbstractConfiguration & config);
    SplitterChannelValidator properties_validator = nullptr; // just for test now
};
} // namespace Poco