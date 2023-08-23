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

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/TwoLevelHashTable.h>


template <
    typename Key,
    typename Cell,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = HashMapTable>
class TwoLevelHashMapTable
    : public TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>>
{
public:
    using Impl = ImplTable<Key, Cell, Hash, Grower, Allocator>;
    using LookupResult = typename Impl::LookupResult;

    using TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator, ImplTable<Key, Cell, Hash, Grower, Allocator>>::
        TwoLevelHashTable;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            this->impls[i].forEachMapped(func);
    }

    typename Cell::Mapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted)
            new (&it->getMapped()) typename Cell::Mapped();

        return it->getMapped();
    }
};


template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = HashMapTable>
using TwoLevelHashMap = TwoLevelHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable>;


template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = TwoLevelHashTableGrower<>,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = HashMapTable>
using TwoLevelHashMapWithSavedHash
    = TwoLevelHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower, Allocator, ImplTable>;
