// Copyright 2025 PingCAP, Inc.
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

#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBuffer.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <common/types.h>

namespace DB::DM::InvertedIndex
{

enum class Version
{
    Invalid = 0,
    V1 = 1,
};

// InvertedIndex file format:
// | VERSION | Block 0 (compressed) | Block 1 (compressed) | ... | Block N (compressed) | Meta | Meta size | Magic flag |

// Block format:
// | number of values | value | row_ids size | value | row_ids size | ... | value | row_ids size | row_ids | row_ids | ... | row_ids |

// Meta format:
// | size of T | number of blocks | offset | size | min | max | offset | size | min | max | ... | offset | size | min | max |

using RowID = UInt32;
using RowIDs = std::vector<RowID>;

// A block is a minimal unit of IO, it will be as small as possible, but >= 64KB.
static constexpr size_t BlockSize = 64 * 1024; // 64 KB

// <value, row_ids>
template <typename T>
struct BlockEntry
{
    T value;
    RowIDs row_ids;
};

template <typename T>
struct Block
{
    Block() = default;

    explicit Block(UInt32 size)
        : entries(size)
    {}

    std::vector<BlockEntry<T>> entries;
    void serialize(WriteBuffer & write_buf) const;
    static void deserialize(Block<T> & block, ReadBuffer & read_buf);

    static void search(BitmapFilterPtr & bitmap_filter, ReadBuffer & read_buf, T key);
    static void searchRange(BitmapFilterPtr & bitmap_filter, ReadBuffer & read_buf, T begin, T end);
};

template <typename T>
struct MetaEntry
{
    UInt32 offset; // offset in the file
    UInt32 size; // block size
    T min;
    T max;

    void serialize(WriteBuffer & write_buf) const;
    static void deserialize(MetaEntry<T> & entry, ReadBuffer & read_buf);
};

template <typename T>
struct Meta
{
    Meta() = default;

    explicit Meta(UInt32 size)
        : entries(size)
    {}

    std::vector<MetaEntry<T>> entries;
    void serialize(WriteBuffer & write_buf) const;
    static void deserialize(Meta<T> & meta, ReadBuffer & read_buf);
};

static auto constexpr MagicFlag = "INVE";
static UInt32 constexpr MagicFlagLength = 4; // strlen(MagicFlag)

// Get the size of the block in bytes.
template <typename T>
constexpr size_t getBlockSize(UInt32 entry_size, UInt32 row_ids_size)
{
    return sizeof(UInt32) + entry_size * (sizeof(T) + sizeof(UInt32)) + row_ids_size * sizeof(RowID);
}

} // namespace DB::DM::InvertedIndex
