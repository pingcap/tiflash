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

#include <common/types.h>

/** Common defines for compression */

#define DBMS_MAX_COMPRESSED_SIZE 0x40000000ULL /// 1GB

/** one byte for method, 4 bytes for compressed size, 4 bytes for uncompressed size */
#define COMPRESSED_BLOCK_HEADER_SIZE 9

namespace DB
{

/** The compressed block format is as follows:
  *
  * The first 16 bytes are the checksum from all other bytes of the block. Now only CityHash128 is used.
  * In the future, you can provide other checksums, although it will not be possible to make them different in size.
  *
  * The next byte specifies the compression algorithm. Then everything depends on the algorithm.
  *
  * 0x82 - LZ4 or LZ4HC (they have the same format).
  *        Next 4 bytes - the size of the compressed data, taking into account the header; 4 bytes is the size of the uncompressed data.
  *
  * NOTE: Why is 0x82?
  * Originally only QuickLZ was used. Then LZ4 was added.
  * The high bit is set to distinguish from QuickLZ, and the second bit is set for compatibility,
  *  for the functions qlz_size_compressed, qlz_size_decompressed to work.
  * Although now such compatibility is no longer relevant.
  *
  * 0x90 - ZSTD
  *
  * All sizes are little endian.
  */

// clang-format off
// `CompressionMethodByte` is used to indicate which compression/decompression algorithm to use.
// The value of `CompressionMethodByte` will be stored with compressed data.
enum class CompressionMethodByte : UInt8
{
    NONE            = 0x02,
    LZ4             = 0x82,
    QPL             = 0x88,
    ZSTD            = 0x90,
    Multiple        = 0x91,
    DeltaFOR        = 0x92,
    RunLength       = 0x93,
    FOR             = 0x94,
    Lightweight     = 0x95,
    // COL_END is not a compreesion method, but a flag of column end used in compact file.
    COL_END         = 0x66,
};
// clang-format on

enum class CompressionDataType : UInt8
{
    // These enum values are used to represent the number of bytes of the type
    Int8 = 1, // Int8/UInt8
    Int16 = 2, // Int16/UInt16
    Int32 = 4, // Int32/UInt32
    Int64 = 8, // Int64/UInt64
    // These enum values are not related to the number of bytes of the type
    Float32 = 9,
    Float64 = 10,
    String = 11,
};

} // namespace DB
