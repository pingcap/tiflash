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

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Flash/Coprocessor/CodecUtils.h>
#include <Flash/Coprocessor/DAGUtils.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace DB::ErrorCodes

namespace DB::CodecUtils
{
void checkColumnSize(const String & identifier, size_t expected, size_t actual)
{
    if (unlikely(expected != actual))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{} schema size mismatch, expected {}, actual {}.",
            identifier,
            expected,
            actual);
}

void checkDataTypeName(const String & identifier, size_t column_index, const String & expected, const String & actual)
{
    if (likely(expected == actual))
        return;
    if (expected == DataTypeString::NameV2 && actual == DataTypeString::LegacyName)
        return;
    if (expected == DataTypeString::NullableNameV2 && actual == DataTypeString::NullableLegacyName)
        return;

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "{} schema mismatch at column {}, expected {}, actual {}",
        identifier,
        column_index,
        expected,
        actual);
}

const IDataType & convertDataTypeByPacketVersion(const IDataType & type, MPPDataPacketVersion packet_version)
{
    if (packet_version >= MPPDataPacketVersion::MPPDataPacketV2)
        return type;

    // If packet_version < MPPDataPacketVersion::MPPDataPacketV2, use legacy DataTypeString.
    static const auto legacy_string_type = DataTypeFactory::instance().getOrSet(DataTypeString::LegacyName);
    static const auto legacy_nullable_string_type
        = DataTypeFactory::instance().getOrSet(DataTypeString::NullableLegacyName);

    auto name = type.getName();
    if (name == DataTypeString::NameV2)
        return *legacy_string_type;
    else if (name == DataTypeString::NullableNameV2)
        return *legacy_nullable_string_type;
    else
        return type;
}

} // namespace DB::CodecUtils
