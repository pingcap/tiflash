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

#include <Common/HashTable/ClearableHashSet.h>
#include <Common/HashTable/HashMap.h>


template <typename Key, typename Mapped, typename Hash>
struct ClearableHashMapCell : public ClearableHashTableCell<Key, HashMapCell<Key, Mapped, Hash, ClearableHashSetState>>
{
    using Base = ClearableHashTableCell<Key, HashMapCell<Key, Mapped, Hash, ClearableHashSetState>>;
    using Base::Base;

    ClearableHashMapCell(const typename Base::value_type & value_, const typename Base::State & state)
        : Base::BaseCell(value_, state)
        , Base::version(state.version)
    {}
};

template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator>
class ClearableHashMap : public HashTable<Key, ClearableHashMapCell<Key, Mapped, Hash>, Hash, Grower, Allocator>
{
public:
    Mapped & operator[](const Key & x)
    {
        typename ClearableHashMap::LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted)
            new (&it->getMapped()) Mapped();

        return it->getMapped();
    }

    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};
