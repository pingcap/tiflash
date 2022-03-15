#pragma once

#include <Common/Checksum.h>
#include <common/types.h>

#include <cstdint>

namespace DB::PS::V3::Format
{
enum RecordType : UInt8
{
    // Zero is reserved for preallocated files
    ZeroType = 0,
    FullType = 1,

    // For fragments
    FirstType = 2,
    MiddleType = 3,
    LastType = 4,

    // For recycled log files
    RecyclableFullType = 5,
    RecyclableFirstType = 6,
    RecyclableMiddleType = 7,
    RecyclableLastType = 8,
};
static constexpr UInt8 MaxRecordType = RecyclableLastType;

static constexpr UInt32 BLOCK_SIZE = 32 * 1024;
static_assert(BLOCK_SIZE < std::numeric_limits<UInt16>::max());

using ChecksumClass = Digest::CRC64;

using ChecksumType = ChecksumClass::HashType;

static constexpr UInt32 CHECKSUM_FIELD_SIZE = ChecksumClass::hash_size;

// If the size of payload is larger than `BLOCK_SIZE`, it will be splitted into
// fragments. So `PAYLOAD_FIELD_SIZE` must be fit in UInt16.
static constexpr UInt32 PAYLOAD_FIELD_SIZE = sizeof(UInt16);

// The checksum count begin at the `type` field in Header/RecyclableHeader
static constexpr size_t CHECKSUM_START_OFFSET = Format::CHECKSUM_FIELD_SIZE + Format::PAYLOAD_FIELD_SIZE;

using LogNumberType = UInt32;

// Header is
// - checksum (`CHECKSUM_FIELD_SIZE` bytes
// - length (`PAYLOAD_FIELD_SIZE` bytes)
// - type (1 byte)
static constexpr int HEADER_SIZE = CHECKSUM_FIELD_SIZE + PAYLOAD_FIELD_SIZE + sizeof(MaxRecordType);

// Recyclable header is
// - checksum (`CHECKSUM_FIELD_SIZE` bytes)
// - length (`PAYLOAD_FIELD_SIZE` bytes)
// - type (1 byte),
// - log number (4 bytes).
static constexpr int RECYCLABLE_HEADER_SIZE = CHECKSUM_FIELD_SIZE + PAYLOAD_FIELD_SIZE + sizeof(MaxRecordType) + sizeof(LogNumberType);

} // namespace DB::PS::V3::Format
