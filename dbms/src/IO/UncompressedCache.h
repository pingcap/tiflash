// Copyright 2022 PingCAP, Ltd.
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

#include <Common/HashTable/Hash.h>
#include <Common/LRUCache.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <IO/BufferWithOwnMemory.h>

namespace DB
{
struct UncompressedCacheCell
{
    Memory data;
    size_t compressed_size;
};

struct UncompressedSizeWeightFunction
{
    size_t operator()(const UncompressedCacheCell & x) const
    {
        return x.data.size();
    }
};


/** Cache of decompressed blocks for implementation of CachedCompressedReadBuffer. thread-safe.
  */
class UncompressedCache : public LRUCache<UInt128, UncompressedCacheCell, TrivialHash, UncompressedSizeWeightFunction>
{
private:
    using Base = LRUCache<UInt128, UncompressedCacheCell, TrivialHash, UncompressedSizeWeightFunction>;

public:
    UncompressedCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes)
    {}

    /// Calculate key from path to file and offset.
    static UInt128 hash(const String & path_to_file, size_t offset)
    {
        UInt128 key;

        SipHash hash;
        hash.update(path_to_file.data(), path_to_file.size() + 1);
        hash.update(offset);
        hash.get128(key);

        return key;
    }

    MappedPtr get(const Key & key)
    {
        MappedPtr res = Base::get(key);

        return res;
    }
};

using UncompressedCachePtr = std::shared_ptr<UncompressedCache>;

} // namespace DB
