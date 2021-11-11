#pragma once

namespace DB::PS::V3::Format
{
enum RecordType
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
static constexpr int MaxRecordType = RecyclableLastType;

static constexpr unsigned int BLOCK_SIZE = 32 * 1024;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte)
static constexpr int HEADER_SIZE = 4 + 2 + 1;

// Recyclable header is checksum (4 bytes), length (2 bytes), type (1 byte),
// log number (4 bytes).
static constexpr int RECYCLABLE_HEADER_SIZE = 4 + 2 + 1 + 4;
} // namespace DB::PS::V3::Format
