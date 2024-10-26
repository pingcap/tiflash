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

template <typename Key>
using DefPhHash = PhHash<Key, PhHashSeed1>;
// TODO
template <typename Key, typename Mapped, typename Hash = DefPhHash<Key>>
using PhHashMap = PhHashTable<Key, Mapped, Hash>;

template <typename Key, typename Mapped, typename Hash = DefPhHash<Key>>
using PhHashMapWithSavedHash = PhHashTableWithSavedHash<Key, Mapped, Hash>;

// template parameter of Hash/Grower/Allocator will be ignored for phmap.
template <
    typename Key,
    typename MappedType,
    typename Hash = DefPhHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = PhHashMap>
class TwoLevelPhHashMapTable
    : public TwoLevelHashTable<Key, MappedType, Hash, Grower, Allocator, ImplTable<Key, MappedType, Hash>>
{
public:
    using Impl = ImplTable<Key, MappedType, Hash>;
    using LookupResult = typename Impl::LookupResult;
    // TODO dup?
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

template <typename Key, typename Mapped, typename Hash = DefPhHash<Key>>
using TwoLevelPhHashMap = TwoLevelPhHashMapTable<Key, Mapped, Hash>;

template <typename Key, typename Mapped, typename Hash = DefPhHash<Key>>
using TwoLevelPhHashMapWithSavedHash
    = TwoLevelPhHashMapTable<Key, Mapped, Hash, TwoLevelHashTableGrower<>, HashTableAllocator, PhHashMapWithSavedHash>;

// TODO gjt Allocator
template <typename TMapped, typename Allocator>
struct StringHashMapPhSubMaps
{
    static constexpr bool isPhMap = true;
    static constexpr bool isNestedMap = false;
    // TODO StringHashMap for now use StringHashTableHash
    using T0 = StringHashTableEmpty<StringHashMapCell<StringRef, TMapped>>;
    using T1 = PhHashTable<StringKey8, TMapped, StringHashTableHash>;
    using T2 = PhHashTable<StringKey16, TMapped, StringHashTableHash>;
    using T3 = PhHashTable<StringKey24, TMapped, StringHashTableHash>;
    // TODO saved hash?
    using Ts = PhHashTable<StringRef, TMapped, StringHashTableHash>;
};

template <typename Mapped>
using PhStringHashMap = StringHashMap<Mapped, HashTableAllocator, StringHashMapPhSubMaps>;

template <typename Mapped>
using TwoLevelPhStringHashMap
    = TwoLevelStringHashMap<Mapped, HashTableAllocator, StringHashMap, StringHashMapPhSubMaps>;

// using AggregatedDataWithUInt64KeyPhMap = PhHashMap<UInt64, AggregateDataPtr>;
// using AggregatedDataWithStringKey = PhHashMapWithSavedHash<StringRef, AggregateDataPtr>;
// using AggregatedDataWithUInt64KeyTwoLevelPhMap = TwoLevelPhHashMap<UInt64, AggregateDataPtr>;
// using AggregatedDataWithStringKeyTwoLevel = TwoLevelPhHashMapWithSavedHash<StringRef, AggregateDataPtr>;
//
// using AggregationMethod_key64 = AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyPhMap, false>;
// using AggregationMethod_key32_two_level = AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyTwoLevelPhMap, false>;
// using AggregationMethod_key64_two_level = AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyTwoLevelPhMap, false>;

namespace DB
{
using AggregatedDataWithUInt8KeyPhMap = FixedImplicitZeroHashMapWithCalculatedSize<UInt8, AggregateDataPtr>;
using AggregatedDataWithUInt16KeyPhMap = FixedImplicitZeroHashMap<UInt16, AggregateDataPtr>;

using AggregatedDataWithUInt32KeyPhMap = PhHashMap<UInt32, AggregateDataPtr>;
using AggregatedDataWithUInt64KeyPhMap = PhHashMap<UInt64, AggregateDataPtr>;

using AggregatedDataWithShortStringKeyPhMap = PhStringHashMap<AggregateDataPtr>;
// TODO DefPhHash for StringRef
using AggregatedDataWithStringKeyPhMap = PhHashMapWithSavedHash<StringRef, AggregateDataPtr, DefaultHash<StringRef>>;

// TODO hasher ok with Int256? for now use HashCRC32???
using AggregatedDataWithInt256KeyPhMap = PhHashTable<Int256, AggregateDataPtr, HashCRC32<Int256>>;

using AggregatedDataWithKeys128PhMap = PhHashMap<UInt128, AggregateDataPtr>;
using AggregatedDataWithKeys256PhMap = PhHashTable<UInt256, AggregateDataPtr, HashCRC32<UInt256>>;
// using AggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, HashCRC32<UInt128>>;
// using AggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, HashCRC32<UInt256>>;

using AggregatedDataWithUInt32KeyTwoLevelPhMap = TwoLevelPhHashMap<UInt32, AggregateDataPtr>;
using AggregatedDataWithUInt64KeyTwoLevelPhMap = TwoLevelPhHashMap<UInt64, AggregateDataPtr>;

using AggregatedDataWithInt256KeyTwoLevelPhMap = TwoLevelPhHashMap<Int256, AggregateDataPtr, HashCRC32<Int256>>;

using AggregatedDataWithShortStringKeyTwoLevelPhMap = TwoLevelPhStringHashMap<AggregateDataPtr>;
using AggregatedDataWithStringKeyTwoLevelPhMap = TwoLevelPhHashMapWithSavedHash<StringRef, AggregateDataPtr, DefaultHash<StringRef>>;

using AggregatedDataWithKeys128TwoLevelPhMap = TwoLevelPhHashMap<UInt128, AggregateDataPtr>;
using AggregatedDataWithKeys256TwoLevelPhMap = TwoLevelPhHashMap<UInt256, AggregateDataPtr, HashCRC32<UInt256>>;

// TODO hash is not good
using AggregatedDataWithUInt64KeyHash64PhMap = PhHashMap<UInt64, AggregateDataPtr, DefaultHash<UInt64>>;
using AggregatedDataWithStringKeyHash64PhMap = PhHashMapWithSavedHash<StringRef, AggregateDataPtr, StringRefHash64>;
using AggregatedDataWithKeys128Hash64PhMap = PhHashMap<UInt128, AggregateDataPtr, DefaultHash<UInt128>>;
using AggregatedDataWithKeys256Hash64PhMap = PhHashMap<UInt256, AggregateDataPtr, DefaultHash<UInt256>>;
} // namespace DB
