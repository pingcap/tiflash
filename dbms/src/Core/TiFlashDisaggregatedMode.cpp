// Copyright 2022 PingCAP, Ltd.
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
#include <Core/TiFlashDisaggregatedMode.h>

namespace DB
{
DisaggregatedMode getDisaggregatedMode(const Poco::Util::LayeredConfiguration & config)
{
    static const std::string config_key = "flash.disaggregated_mode";
    DisaggregatedMode mode = DisaggregatedMode::None;
    if (config.has(config_key))
    {
        std::string mode_str = config.getString(config_key);
        RUNTIME_ASSERT(mode_str == DISAGGREGATED_MODE_STORAGE || mode_str == DISAGGREGATED_MODE_COMPUTE,
                       "Expect disaggregated_mode is {} or {}, got: {}",
                       DISAGGREGATED_MODE_STORAGE,
                       DISAGGREGATED_MODE_COMPUTE,
                       mode_str);
        if (mode_str == DISAGGREGATED_MODE_COMPUTE)
        {
            mode = DisaggregatedMode::Compute;
        }
        else
        {
            mode = DisaggregatedMode::Storage;
        }
    }
    return mode;
}
} // namespace DB
