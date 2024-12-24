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

#include <Common/Exception.h>
#include <Storages/FormatVersion.h>

namespace DB
{

namespace
{
const StorageFormatVersion & toStorageFormat(UInt64 setting)
{
    switch (setting)
    {
    case 1:
        return STORAGE_FORMAT_V1;
    case 2:
        return STORAGE_FORMAT_V2;
    case 3:
        return STORAGE_FORMAT_V3;
    case 4:
        return STORAGE_FORMAT_V4;
    case 5:
        return STORAGE_FORMAT_V5;
    case 6:
        return STORAGE_FORMAT_V6;
    case 7:
        return STORAGE_FORMAT_V7;
    case 8:
        return STORAGE_FORMAT_V8;
    case 100:
        return STORAGE_FORMAT_V100;
    case 101:
        return STORAGE_FORMAT_V101;
    case 102:
        return STORAGE_FORMAT_V102;
    case 103:
        return STORAGE_FORMAT_V103;
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal setting value: {}", setting);
    }
}
} // namespace

std::span<const size_t> getStorageFormatsForDisagg()
{
    static const auto formats = std::array{
        STORAGE_FORMAT_V100.identifier,
        STORAGE_FORMAT_V101.identifier,
        STORAGE_FORMAT_V102.identifier,
        STORAGE_FORMAT_V103.identifier};
    return formats;
}

bool isStorageFormatForDisagg(UInt64 version)
{
    return std::any_of(getStorageFormatsForDisagg().begin(), getStorageFormatsForDisagg().end(), [version](UInt64 v) {
        return v == version;
    });
}

void setStorageFormat(UInt64 setting)
{
    STORAGE_FORMAT_CURRENT = toStorageFormat(setting);
}

void setStorageFormat(const StorageFormatVersion & version)
{
    STORAGE_FORMAT_CURRENT = version;
}

} // namespace DB
