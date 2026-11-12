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

#include <DataTypes/IDataType.h>
#include <Flash/Mpp/MppVersion.h>

#include <string_view>

namespace DB
{
namespace CodecUtils
{
// Safety limit for decoded CH blocks, including rows accumulated across chunks.
constexpr size_t MAX_DECODED_BLOCK_ROWS = 1ULL << 24;

struct DataTypeWithTypeName
{
    DataTypeWithTypeName(const DataTypePtr & t, const String & n)
        : type(t)
        , name(n)
    {}

    DataTypePtr type;
    String name;
};

void checkColumnSize(const String & identifier, size_t expected, size_t actual);
void checkRowCount(std::string_view identifier, size_t rows, size_t existing_rows = 0);
void checkDataTypeName(const String & identifier, size_t column_index, const String & expected, const String & actual);
const IDataType & convertDataType(const IDataType & type);
const IDataType & convertDataTypeByPacketVersion(const IDataType & type, MPPDataPacketVersion packet_version);
} // namespace CodecUtils
} // namespace DB
