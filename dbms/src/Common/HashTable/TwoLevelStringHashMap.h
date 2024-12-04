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

#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelStringHashTable.h>

template <
    typename TMapped,
    typename HashSelector,
    typename Allocator = HashTableAllocator,
    template <typename...> typename ImplTable = StringHashMap>
class TwoLevelStringHashMap
    : public TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, HashSelector, Allocator>, ImplTable<TMapped, HashSelector, Allocator>>
{
public:
    using Key = StringRef;
    using Self = TwoLevelStringHashMap;
    using Base = TwoLevelStringHashTable<StringHashMapSubMaps<TMapped, HashSelector, Allocator>, StringHashMap<TMapped, HashSelector, Allocator>>;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i)
            this->impls[i].forEachMapped(func);
    }

    TMapped & ALWAYS_INLINE operator[](const Key & x)
    {
        bool inserted;
        LookupResult it;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getMapped()) TMapped();
        return it->getMapped();
    }
};
