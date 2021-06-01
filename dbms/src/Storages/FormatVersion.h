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

} // namespace SegmentFormat

namespace DMFileFormat
{
using Version = UInt32;

inline static constexpr Version V0 = 0;
inline static constexpr Version V1 = 1; // Add column stats
} // namespace DMFileFormat

namespace StableFormat
{
using Version = Int64;

inline static constexpr Version V1 = 1;
} // namespace StableFormat

namespace DeltaFormat
{
using Version = UInt64;

inline static constexpr Version V1 = 1;
inline static constexpr Version V2 = 2; // Support clustered index
inline static constexpr Version V3 = 3; // Support DeltaPackFile
} // namespace DeltaFormat

namespace PageFormat
{
using Version = UInt32;

inline static constexpr Version V1 = 1;
// Support multiple thread-write && read with offset inside page. See FLASH_341 && FLASH-942 for details.
inline static constexpr Version V2 = 2;
} // namespace PageFormat

struct StorageFormatVersion
{
    SegmentFormat::Version segment;
    DMFileFormat::Version dm_file;
    StableFormat::Version stable;
    DeltaFormat::Version delta;
    PageFormat::Version page;
};

inline static const StorageFormatVersion STORAGE_FORMAT_V1 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V1,
    .stable = StableFormat::V1,
    .delta = DeltaFormat::V2,
    .page = PageFormat::V2,
};

inline static const StorageFormatVersion STORAGE_FORMAT_V2 = StorageFormatVersion{
    .segment = SegmentFormat::V2,
    .dm_file = DMFileFormat::V1,
    .stable = StableFormat::V1,
    .delta = DeltaFormat::V3, // diff
    .page = PageFormat::V2,
};

inline StorageFormatVersion STORAGE_FORMAT_CURRENT = STORAGE_FORMAT_V2;

inline const StorageFormatVersion & toStorageFormat(UInt64 setting)
{
    switch (setting)
    {
        case 1:
            return STORAGE_FORMAT_V1;
        case 2:
            return STORAGE_FORMAT_V2;
        default:
            throw Exception("Illegal setting value: " + DB::toString(setting));
    }
}

inline void setStorageFormat(UInt64 setting) { STORAGE_FORMAT_CURRENT = toStorageFormat(setting); }

inline void setStorageFormat(const StorageFormatVersion & version) { STORAGE_FORMAT_CURRENT = version; }

} // namespace DB
