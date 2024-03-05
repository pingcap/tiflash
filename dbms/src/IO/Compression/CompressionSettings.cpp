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

#include "CompressionSettings.h"

#include <Common/config.h>
#include <Interpreters/Settings.h>
#include <lz4hc.h>

namespace DB
{
CompressionSettings::CompressionSettings(const Settings & settings)
{
    method = settings.network_compression_method;
    switch (method)
    {
    case CompressionMethod::ZSTD:
        level = settings.network_zstd_compression_level;
        break;
    default:
        level = getDefaultLevel(method);
    }
}

int CompressionSettings::getDefaultLevel(CompressionMethod method)
{
    switch (method)
    {
    case CompressionMethod::LZ4:
        return 1;
    case CompressionMethod::LZ4HC:
        return LZ4HC_CLEVEL_DEFAULT;
    case CompressionMethod::ZSTD:
        return 1;
#if USE_QPL
    case CompressionMethod::QPL:
        return 1;
#endif
    default:
        return -1;
    }
}

} // namespace DB
