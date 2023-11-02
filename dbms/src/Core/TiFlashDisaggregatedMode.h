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

#include <Poco/Util/LayeredConfiguration.h>

#include <string>

#define DEF_PROXY_LABEL "tiflash"
#define DISAGGREGATED_MODE_COMPUTE_PROXY_LABEL DISAGGREGATED_MODE_COMPUTE
// Note that TiFlash Write Node is also named as TiFlash Storage Node in many places.
// To make sure it is consistent to our documents, we better stick with "tiflash_write" in the configurations.
#define DISAGGREGATED_MODE_WRITE "tiflash_write"
#define DISAGGREGATED_MODE_STORAGE "tiflash_storage" // backward compatibility just for parsing config
#define DISAGGREGATED_MODE_COMPUTE "tiflash_compute"
// engine_role determine whether TiFlash use S3 to write.
#define DISAGGREGATED_MODE_WRITE_ENGINE_ROLE "write"

namespace DB
{
enum class DisaggregatedMode
{
    None,
    Compute,
    Storage,
};

DisaggregatedMode getDisaggregatedMode(const Poco::Util::LayeredConfiguration & config);
bool useAutoScaler(const Poco::Util::LayeredConfiguration & config);
std::string getProxyLabelByDisaggregatedMode(DisaggregatedMode mode);
} // namespace DB
