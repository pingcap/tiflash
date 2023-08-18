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

using LogNumberType = UInt64;

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
static constexpr int RECYCLABLE_HEADER_SIZE
    = CHECKSUM_FIELD_SIZE + PAYLOAD_FIELD_SIZE + sizeof(MaxRecordType) + sizeof(LogNumberType);

} // namespace DB::PS::V3::Format
