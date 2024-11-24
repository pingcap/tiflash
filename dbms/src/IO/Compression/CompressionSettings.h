// Copyright 2024 PingCAP, Inc.
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

#include <DataTypes/IDataType.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/Compression/CompressionMethod.h>
#include <common/types.h>

#include <unordered_map>

namespace DB
{
struct Settings;

constexpr CompressionMethodByte method_byte_map[] = {
    CompressionMethodByte::NONE, // Invalid
    CompressionMethodByte::LZ4, // LZ4
    CompressionMethodByte::LZ4, // LZ4HC
    CompressionMethodByte::ZSTD, // ZSTD
    CompressionMethodByte::QPL, // QPL
    CompressionMethodByte::NONE, // NONE
    CompressionMethodByte::Lightweight, // Lightweight
};

const std::unordered_map<CompressionMethodByte, CompressionMethod> method_map = {
    {CompressionMethodByte::LZ4, CompressionMethod::LZ4},
    {CompressionMethodByte::ZSTD, CompressionMethod::ZSTD},
    {CompressionMethodByte::QPL, CompressionMethod::QPL},
    {CompressionMethodByte::NONE, CompressionMethod::NONE},
    {CompressionMethodByte::DeltaFOR, CompressionMethod::NONE},
    {CompressionMethodByte::RunLength, CompressionMethod::NONE},
    {CompressionMethodByte::FOR, CompressionMethod::NONE},
    {CompressionMethodByte::Lightweight, CompressionMethod::Lightweight},
};

struct CompressionSetting
{
    CompressionMethod method;
    int level;
    // The type of data to be compressed.
    // It may be used to determine the codec to use.
    CompressionDataType data_type = CompressionDataType::Unknown;
    // Always use method_byte to determine the codec to use except for LZ4HC codec
    CompressionMethodByte method_byte;

    CompressionSetting()
        : CompressionSetting(CompressionMethod::LZ4)
    {}

    explicit CompressionSetting(CompressionMethod method_)
        : method(method_)
        , level(getDefaultLevel(method))
        , method_byte(method_byte_map[static_cast<size_t>(method_)])
    {}

    explicit CompressionSetting(CompressionMethodByte method_byte_)
        : method(method_map.at(method_byte_))
        , level(getDefaultLevel(method))
        , method_byte(method_byte_)
    {}

    CompressionSetting(CompressionMethod method_, int level_)
        : method(method_)
        , level(level_)
        , method_byte(method_byte_map[static_cast<size_t>(method_)])
    {}

    explicit CompressionSetting(const Settings & settings);

    template <typename T>
    static CompressionSetting create(T method, int level, const IDataType & type);

    static int getDefaultLevel(CompressionMethod method);
};

struct CompressionSettings
{
    CompressionSettings()
        : settings(1, CompressionSetting(CompressionMethod::LZ4))
    {}

    explicit CompressionSettings(CompressionMethod method_)
        : settings(1, CompressionSetting(method_))
    {}

    CompressionSettings(CompressionMethod method_, int level_)
        : settings(1, CompressionSetting(method_, level_))
    {}

    explicit CompressionSettings(const Settings & settings_)
        : settings(1, CompressionSetting(settings_))
    {}

    explicit CompressionSettings(const std::vector<CompressionSetting> & settings_)
        : settings(settings_)
    {}

    explicit CompressionSettings(CompressionSetting setting)
        : settings(1, std::move(setting))
    {}

    std::vector<CompressionSetting> settings;
};

} // namespace DB
