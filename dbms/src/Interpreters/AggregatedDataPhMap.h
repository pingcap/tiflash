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

#pragma once

#include <Common/HashTable/PhHashTable.h>
#include <Interpreters/Aggregator.h>

// todo hash?
template <typename Key, typename Mapped, typename Hash>
using PhHashMap = PhHashTable<Key, Mapped, Hash>;

template <typename Key, typename Mapped, typename Hash>
using PhHashMapWithSavedHash = PhHashTableWithSavedHash<Key, Mapped, Hash>;

// Note: template parameter of Hash/Grower/Allocator will be ignored for phmap.
template <
    typename Key,
    typename MappedType,
    typename Hash,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = PhHashMap>
class TwoLevelPhHashMapTable
    : public TwoLevelHashTable<Key, MappedType, Hash, Grower, Allocator, ImplTable<Key, MappedType, Hash>>
{
public:
    using Impl = ImplTable<Key, MappedType, Hash>;
    using LookupResult = typename Impl::LookupResult;
    using Mapped = MappedType;
    using Self = TwoLevelPhHashMapTable;

    using TwoLevelHashTable<Key, Mapped, Hash, Grower, Allocator, ImplTable<Key, Mapped, Hash>>::TwoLevelHashTable;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            this->impls[i].forEachMapped(func);
    }

    typename Self::Mapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted)
            new (&it->getMapped()) typename Self::Mapped();

        return it->getMapped();
    }
};

template <typename Key, typename Mapped, typename Hash>
using TwoLevelPhHashMap = TwoLevelPhHashMapTable<Key, Mapped, Hash>;

template <typename Key, typename Mapped, typename Hash>
using TwoLevelPhHashMapWithSavedHash
    = TwoLevelPhHashMapTable<Key, Mapped, Hash, TwoLevelHashTableGrower<>, HashTableAllocator, PhHashMapWithSavedHash>;

namespace DB
{
// todo: also test magic hash.
using AggregatedDataWithUInt32KeyPhMap = PhHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyPhMap = PhHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
using AggregatedDataWithKeys128PhMap = PhHashMap<UInt128, AggregateDataPtr, HashCRC32<UInt128>>;
using AggregatedDataWithStringKeyPhMap = PhHashMapWithSavedHash<StringRef, AggregateDataPtr, DefaultHash<StringRef>>;

using AggregatedDataWithUInt32KeyTwoLevelPhMap = TwoLevelPhHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyTwoLevelPhMap = TwoLevelPhHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
using AggregatedDataWithKeys128TwoLevelPhMap = TwoLevelPhHashMap<UInt128, AggregateDataPtr, HashCRC32<UInt128>>;
using AggregatedDataWithStringKeyTwoLevelPhMap = TwoLevelPhHashMapWithSavedHash<StringRef, AggregateDataPtr, DefaultHash<StringRef>>;
} // namespace DB
