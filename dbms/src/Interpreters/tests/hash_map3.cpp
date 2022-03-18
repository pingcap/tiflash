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

#include <iostream>

#define DBMS_HASH_MAP_DEBUG_RESIZES
#define DBMS_HASH_MAP_COUNT_COLLISIONS


#include <string.h>

#include <cstdlib>

#include <utility>

#include <Core/Types.h>
#include <Common/Exception.h>

#include <IO/ReadHelpers.h>

#include <common/StringRef.h>

#include <Common/HashTable/HashMap.h>


template
<
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator
>
class HashMapWithDump : public HashMap<Key, Mapped, Hash, Grower, Allocator>
{
public:
    void dump() const
    {
        for (size_t i = 0; i < this->grower.bufSize(); ++i)
        {
            if (this->buf[i].isZero(*this))
                std::cerr << "[    ]";
            else
                std::cerr << '[' << this->buf[i].getValue().first.data << ", " << this->buf[i].getValue().second << ']';
        }
        std::cerr << std::endl;
    }
};


struct SimpleHash
{
    size_t operator() (UInt64 x) const { return x; }
    size_t operator() (StringRef x) const { return DB::parse<UInt64>(x.data); }
};

struct Grower : public HashTableGrower<2>
{
    void increaseSize()
    {
        ++size_degree;
    }
};

int main(int, char **)
{
    using Map = HashMapWithDump<
        StringRef,
        UInt64,
        SimpleHash,
        Grower,
        HashTableAllocatorWithStackMemory<4 * 24>>;

    Map map;

    map.dump();
    std::cerr << "size: " << map.size() << std::endl;
    map[StringRef("1", 1)] = 1;
    map.dump();
    std::cerr << "size: " << map.size() << std::endl;
    map[StringRef("9", 1)] = 1;
    map.dump();
    std::cerr << "size: " << map.size() << std::endl;
    std::cerr << "Collisions: " << map.getCollisions() << std::endl;
    map[StringRef("3", 1)] = 2;
    map.dump();
    std::cerr << "size: " << map.size() << std::endl;
    std::cerr << "Collisions: " << map.getCollisions() << std::endl;

    for (auto x : map)
        std::cerr << x.first.toString() << " -> " << x.second << std::endl;

    return 0;
}
