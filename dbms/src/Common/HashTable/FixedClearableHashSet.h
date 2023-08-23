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
#include <Common/HashTable/FixedHashTable.h>


template <typename Key>
struct FixedClearableHashTableCell
{
    using State = ClearableHashSetState;

    using value_type = Key;
    using mapped_type = VoidMapped;
    UInt32 version;

    FixedClearableHashTableCell() = default;
    FixedClearableHashTableCell(const Key &, const State & state)
        : version(state.version)
    {}

    VoidKey getKey() const { return {}; }
    VoidMapped getMapped() const { return {}; }

    bool isZero(const State & state) const { return version != state.version; }
    void setZero() { version = 0; }

    struct CellExt
    {
        Key key;
        VoidKey getKey() const { return {}; }
        VoidMapped getMapped() const { return {}; }
        const value_type & getValue() const { return key; }
        void update(Key && key_, FixedClearableHashTableCell *) { key = key_; }
    };
};


template <typename Key, typename Allocator = HashTableAllocator>
class FixedClearableHashSet
    : public FixedHashTable<
          Key,
          FixedClearableHashTableCell<Key>,
          FixedHashTableStoredSize<FixedClearableHashTableCell<Key>>,
          Allocator>
{
public:
    using Base = FixedHashTable<
        Key,
        FixedClearableHashTableCell<Key>,
        FixedHashTableStoredSize<FixedClearableHashTableCell<Key>>,
        Allocator>;
    using LookupResult = typename Base::LookupResult;

    void clear()
    {
        ++this->version;
        this->m_size = 0;
    }
};
