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

#include <Interpreters/Aggregator.h>
#include <Common/HashTable/PhHashTable.h>

// TODO
template <typename Key, typename Mapped>
using PhHashMap = PhHashTable<Key, Mapped, PhHash<Key, PhHashSeed1>>;

template <typename Key, typename Mapped>
using PhHashMapWithSavedHash = PhHashTableWithSavedHash<Key, Mapped, PhHash<Key, PhHashSeed1>>;

// template parameter of Hash/Grower/Allocator will be ignored for phmap.
template <
    typename Key,
    typename MappedType,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = PhHashMap>
class TwoLevelPhHashMapTable
    : public TwoLevelHashTable<Key, MappedType, Hash, Grower, Allocator, ImplTable<Key, MappedType>>
{
public:
    using Impl = ImplTable<Key, MappedType>;
    using LookupResult = typename Impl::LookupResult;
    // TODO dup?
    using Mapped = MappedType;
    using Self = TwoLevelPhHashMapTable;

    using TwoLevelHashTable<Key, Mapped, Hash, Grower, Allocator, ImplTable<Key, Mapped>>::TwoLevelHashTable;

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

template <typename Key, typename Mapped>
using TwoLevelPhHashMap = TwoLevelPhHashMapTable<Key, Mapped>;

template <typename Key, typename Mapped>
using TwoLevelPhHashMapWithSavedHash = TwoLevelPhHashMapTable<
    Key, Mapped, DefaultHash<Key>, TwoLevelHashTableGrower<>, HashTableAllocator, PhHashMapWithSavedHash>;

// TODO gjt Allocator
template <typename TMapped, typename Allocator>
struct StringHashMapPhSubMaps
{
    static constexpr bool isPhMap = true;
    using T0 = StringHashTableEmpty<StringHashMapCell<StringRef, TMapped>>;
    // TODO seed
    using T1 = PhHashMap<StringKey8, TMapped>;
    using T2 = PhHashMap<StringKey16, TMapped>;
    using T3 = PhHashMap<StringKey24, TMapped>;
    using Ts = PhHashMap<StringRef, TMapped>;
};

template <typename Mapped>
using PhStringHashMap = StringHashMap<Mapped, HashTableAllocator, StringHashMapPhSubMaps>;

template <typename Mapped>
using TwoLevelPhStringHashMap = TwoLevelStringHashMap<Mapped, HashTableAllocator, StringHashMap, StringHashMapPhSubMaps>;

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
using AggregatedDataWithStringKeyPhMap = PhHashMapWithSavedHash<StringRef, AggregateDataPtr>;

// TODO hasher ok with Int256?
using AggregatedDataWithInt256KeyPhMap = PhHashMap<Int256, AggregateDataPtr>;

using AggregatedDataWithKeys128PhMap = PhHashMap<UInt128, AggregateDataPtr>;
using AggregatedDataWithKeys256PhMap = PhHashMap<UInt256, AggregateDataPtr>;

using AggregatedDataWithUInt32KeyTwoLevelPhMap = TwoLevelPhHashMap<UInt32, AggregateDataPtr>;
using AggregatedDataWithUInt64KeyTwoLevelPhMap = TwoLevelPhHashMap<UInt64, AggregateDataPtr>;

using AggregatedDataWithInt256KeyTwoLevelPhMap = TwoLevelPhHashMap<Int256, AggregateDataPtr>;

using AggregatedDataWithShortStringKeyTwoLevelPhMap = TwoLevelPhStringHashMap<AggregateDataPtr>;
using AggregatedDataWithStringKeyTwoLevelPhMap = TwoLevelPhHashMapWithSavedHash<StringRef, AggregateDataPtr>;

using AggregatedDataWithKeys128TwoLevelPhMap = TwoLevelPhHashMap<UInt128, AggregateDataPtr>;
using AggregatedDataWithKeys256TwoLevelPhMap = TwoLevelPhHashMap<UInt256, AggregateDataPtr>;

using AggregatedDataWithUInt64KeyHash64PhMap = PhHashMap<UInt64, AggregateDataPtr>;
using AggregatedDataWithStringKeyHash64PhMap = PhHashMapWithSavedHash<StringRef, AggregateDataPtr>;
using AggregatedDataWithKeys128Hash64PhMap = PhHashMap<UInt128, AggregateDataPtr>;
using AggregatedDataWithKeys256Hash64PhMap = HashMap<UInt256, AggregateDataPtr>;
} // namespace DB
