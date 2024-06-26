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

#include <Common/Exception.h>
#include <Core/Types.h>
#include <IO/WriteHelpers.h>

namespace DB
{
// Those versions use different data types. It is definitely a bad design pattern.
// Unfortunately we cannot change it for compatibility issue.

namespace SegmentFormat
{
using Version = UInt32;

inline static constexpr Version V1 = 1;
inline static constexpr Version V2 = 2; // Support clustered index
inline static constexpr Version V3 = 3; // Meta using protobuf

} // namespace SegmentFormat

namespace DMFileFormat
{
using Version = UInt32;

inline static constexpr Version V0 = 0;
inline static constexpr Version V1 = 1; // Add column stats
inline static constexpr Version V2 = 2; // Add checksum and configuration
inline static constexpr Version V3 = 3; // Use Meta V2
} // namespace DMFileFormat

namespace StableFormat
{
using Version = Int64;

inline static constexpr Version V1 = 1;
inline static constexpr Version V2 = 2; // Meta using protobuf
} // namespace StableFormat

namespace DeltaFormat
{
using Version = UInt64;

inline static constexpr Version V1 = 1;
inline static constexpr Version V2 = 2; // Support clustered index
inline static constexpr Version V3 = 3; // Support DeltaPackFile
inline static constexpr Version V4 = 4; // Meta using protobuf
} // namespace DeltaFormat

namespace PageFormat
{
using Version = UInt32;

inline static constexpr Version V1 = 1;
// Support multiple thread-write && read with offset inside page. See FLASH_341 && FLASH-942 for details.
inline static constexpr Version V2 = 2;
// Support multiple thread-write/read with offset inside page && support wal store meta && support space reused.
// If we do enabled PageFormat::V3, it is not means all data in Disk will be V3 format.
// - If we already have V2 data in disk. It will turn PageStorage into MIX_MODE
// - If we don't have any v2 data in disk. It will turn PageStorage into ONLY_V3
inline static constexpr Version V3 = 3;
// Store all data in one ps instance.
inline static constexpr Version V4 = 4;
} // namespace PageFormat

struct StorageFormatVersion
{
    SegmentFormat::Version segment;
    DMFileFormat::Version dm_file;
    StableFormat::Version stable;
    DeltaFormat::Version delta;
    PageFormat::Version page;
    size_t identifier;
};

inline static const StorageFormatVersion STORAGE_FORMAT_V1 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V1,
    .stable = StableFormat::V1,
    .delta = DeltaFormat::V2,
    .page = PageFormat::V2,
    .identifier = 1,
};

inline static const StorageFormatVersion STORAGE_FORMAT_V2 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V1,
    .stable = StableFormat::V1,
    .delta = DeltaFormat::V3, // diff
    .page = PageFormat::V2,
    .identifier = 2,
};

inline static const StorageFormatVersion STORAGE_FORMAT_V3 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V2, // diff
    .stable = StableFormat::V1,
    .delta = DeltaFormat::V3,
    .page = PageFormat::V2,
    .identifier = 3,
};


inline static const StorageFormatVersion STORAGE_FORMAT_V4 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V2,
    .stable = StableFormat::V1,
    .delta = DeltaFormat::V3,
    .page = PageFormat::V3, // diff
    .identifier = 4,
};

inline static const StorageFormatVersion STORAGE_FORMAT_V5 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V3, // diff
    .stable = StableFormat::V1,
    .delta = DeltaFormat::V3,
    .page = PageFormat::V3,
    .identifier = 5,
};

inline static const StorageFormatVersion STORAGE_FORMAT_V6 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V3,
    .stable = StableFormat::V2, // diff
    .delta = DeltaFormat::V3,
    .page = PageFormat::V3,
    .identifier = 6,
};

inline static const StorageFormatVersion STORAGE_FORMAT_V7 = StorageFormatVersion{
    .segment = SegmentFormat::V3, // diff
    .dm_file = DMFileFormat::V3,
    .stable = StableFormat::V2,
    .delta = DeltaFormat::V4, // diff
    .page = PageFormat::V3,
    .identifier = 7,
};

// STORAGE_FORMAT_V100 is used for S3 only
inline static const StorageFormatVersion STORAGE_FORMAT_V100 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V3,
    .stable = StableFormat::V1,
    .delta = DeltaFormat::V3,
    .page = PageFormat::V4, // diff
    .identifier = 100,
};

// STORAGE_FORMAT_V101 is used for S3 only
inline static const StorageFormatVersion STORAGE_FORMAT_V101 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V3,
    .stable = StableFormat::V2, // diff
    .delta = DeltaFormat::V3,
    .page = PageFormat::V4,
    .identifier = 101,
};

// STORAGE_FORMAT_V102 is used for S3 only
inline static const StorageFormatVersion STORAGE_FORMAT_V102 = StorageFormatVersion{
    .segment = SegmentFormat::V3, // diff
    .dm_file = DMFileFormat::V3,
    .stable = StableFormat::V2,
    .delta = DeltaFormat::V4, // diff
    .page = PageFormat::V4,
    .identifier = 102,
};

inline StorageFormatVersion STORAGE_FORMAT_CURRENT = STORAGE_FORMAT_V6;

inline const StorageFormatVersion & toStorageFormat(UInt64 setting)
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
    case 100:
        return STORAGE_FORMAT_V100;
    case 101:
        return STORAGE_FORMAT_V101;
    case 102:
        return STORAGE_FORMAT_V102;
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal setting value: {}", setting);
    }
}

inline void setStorageFormat(UInt64 setting)
{
    STORAGE_FORMAT_CURRENT = toStorageFormat(setting);
}

inline void setStorageFormat(const StorageFormatVersion & version)
{
    STORAGE_FORMAT_CURRENT = version;
}

} // namespace DB
