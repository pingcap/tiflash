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

#pragma once

#include <IO/CompressedStream.h>


namespace DB
{
struct Settings;

struct CompressionSettings
{
    CompressionMethod method;
    int level;

    CompressionSettings()
        : CompressionSettings(CompressionMethod::LZ4)
    {
    }

    CompressionSettings(CompressionMethod method_)
        : method(method_)
        , level(getDefaultLevel(method))
    {
    }

    CompressionSettings(CompressionMethod method_, int level_)
        : method(method_)
        , level(level_)
    {
    }

    CompressionSettings(const Settings & settings);

    static int getDefaultLevel(CompressionMethod method);
};

} // namespace DB
