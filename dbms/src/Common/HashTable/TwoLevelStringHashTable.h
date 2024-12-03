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

#include <Common/HashTable/StringHashTable.h>

template <typename SubMaps, typename ImplTable = StringHashTable<SubMaps>, size_t BITS_FOR_BUCKET = 8>
class TwoLevelStringHashTable : private boost::noncopyable
{
protected:
    using HashValue = size_t;
    using Self = TwoLevelStringHashTable;

public:
    using Key = StringRef;
    using Impl = ImplTable;

    static constexpr size_t NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
    static constexpr size_t MAX_BUCKET = NUM_BUCKETS - 1;

    static constexpr bool is_string_hash_map = true;
    static constexpr bool is_two_level = true;

    // TODO: currently hashing contains redundant computations when doing distributed or external aggregations
    size_t hash(const Key & x) const
    {
        return const_cast<Self &>(*this).dispatch(*this, x, [&](const auto &, const auto &, size_t hash) {
            return hash;
        });
    }

    void setResizeCallback(const ResizeCallback & resize_callback)
    {
        for (auto & impl : impls)
            impl.setResizeCallback(resize_callback);
    }

    size_t operator()(const Key & x) const { return hash(x); }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t getBucketFromHash(size_t hash_value) { return (hash_value >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET; }

    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    Impl impls[NUM_BUCKETS];

    TwoLevelStringHashTable() = default;

    template <typename Source>
    explicit TwoLevelStringHashTable(Source & src)
    {
        if (src.m0.hasZero())
            impls[0].m0.setHasZero(*src.m0.zeroValue());

        for (auto & v : src.m1)
        {
            // size_t hash_value = v.getHash(src.m1);
            const size_t hash_value = ImplTable::T1::Hash::operator()(v.getKey());
            v.setHash(hash_value);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m1.insertUniqueNonZero(&v, hash_value);
        }
        for (auto & v : src.m2)
        {
            // size_t hash_value = v.getHash(src.m2);
            const size_t hash_value = ImplTable::T2::Hash::operator()(v.getKey());
            v.setHash(hash_value);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m2.insertUniqueNonZero(&v, hash_value);
        }
        for (auto & v : src.m3)
        {
            // size_t hash_value = v.getHash(src.m3);
            const size_t hash_value = ImplTable::T3::Hash::operator()(v.getKey());
            v.setHash(hash_value);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m3.insertUniqueNonZero(&v, hash_value);
        }
        for (auto & v : src.ms)
        {
            // size_t hash_value = v.getHash(src.ms);
            const size_t hash_value = ImplTable::Ts::Hash::operator()(v.getKey());
            v.setHash(hash_value);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].ms.insertUniqueNonZero(&v, hash_value);
        }
    }

    // This function is mostly the same as StringHashTable::dispatch, but with
    // added bucket computation. See the comments there.
    template <typename Self, typename Func, typename KeyHolder>
    static auto
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
        NO_INLINE NO_SANITIZE_ADDRESS NO_SANITIZE_THREAD
#else
        ALWAYS_INLINE
#endif
        dispatch(Self & self, KeyHolder && key_holder, Func && func)
    {
        const StringRef & x = keyHolderGetKey(key_holder);
        const size_t sz = x.size;
        if (sz == 0)
        {
            keyHolderDiscardKey(key_holder);
            return func(self.impls[0].m0, VoidKey{}, 0);
        }

        if (x.data[x.size - 1] == 0)
        {
            // Strings with trailing zeros are not representable as fixed-size
            // string keys. Put them to the generic table.
            auto res = SubMaps::Hash::operator()(x);
            auto buck = getBucketFromHash(res);
            return func(self.impls[buck].ms, std::forward<KeyHolder>(key_holder), res);
        }

        const char * p = x.data;
        // pending bits that needs to be shifted out
        const char s = (-sz & 7) * 8;
        union
        {
            StringKey8 k8;
            StringKey16 k16;
            StringKey24 k24;
            UInt64 n[3];
        };
        switch ((sz - 1) >> 3)
        {
        case 0:
        {
            // first half page
            if ((reinterpret_cast<uintptr_t>(p) & 2048) == 0)
            {
                memcpy(&n[0], p, 8);
                if constexpr (DB::isLittleEndian())
                    n[0] &= (-1ULL >> s);
                else
                    n[0] &= (-1ULL << s);
            }
            else
            {
                const char * lp = x.data + x.size - 8;
                memcpy(&n[0], lp, 8);
                if constexpr (DB::isLittleEndian())
                    n[0] >>= s;
                else
                    n[0] <<= s;
            }
            auto res = SubMaps::Hash::operator()(k8);
            auto buck = getBucketFromHash(res);
            keyHolderDiscardKey(key_holder);
            return func(self.impls[buck].m1, k8, res);
        }
        case 1:
        {
            memcpy(&n[0], p, 8);
            const char * lp = x.data + x.size - 8;
            memcpy(&n[1], lp, 8);
            if constexpr (DB::isLittleEndian())
                n[1] >>= s;
            else
                n[1] <<= s;
            auto res = SubMaps::Hash::operator()(k16);
            auto buck = getBucketFromHash(res);
            keyHolderDiscardKey(key_holder);
            return func(self.impls[buck].m2, k16, res);
        }
        case 2:
        {
            memcpy(&n[0], p, 16);
            const char * lp = x.data + x.size - 8;
            memcpy(&n[2], lp, 8);
            if constexpr (DB::isLittleEndian())
                n[2] >>= s;
            else
                n[2] <<= s;
            auto res = SubMaps::Hash::operator()(k24);
            auto buck = getBucketFromHash(res);
            keyHolderDiscardKey(key_holder);
            return func(self.impls[buck].m3, k24, res);
        }
        default:
        {
            auto res = SubMaps::Hash::operator()(x);
            auto buck = getBucketFromHash(res);
            return func(self.impls[buck].ms, std::forward<KeyHolder>(key_holder), res);
        }
        }
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        dispatch(*this, key_holder, typename Impl::EmplaceCallable{it, inserted});
    }

    LookupResult ALWAYS_INLINE find(const Key & x) { return dispatch(*this, x, typename Impl::FindCallable{}); }

    ConstLookupResult ALWAYS_INLINE find(const Key & x) const
    {
        return dispatch(*this, x, typename Impl::FindCallable{});
    }

    void write(DB::WriteBuffer & wb) const
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            impls[i].write(wb);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
        {
            if (i != 0)
                DB::writeChar(',', wb);
            impls[i].writeText(wb);
        }
    }

    void read(DB::ReadBuffer & rb)
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            impls[i].read(rb);
    }

    void readText(DB::ReadBuffer & rb)
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
        {
            if (i != 0)
                DB::assertChar(',', rb);
            impls[i].readText(rb);
        }
    }

    size_t size() const
    {
        size_t res = 0;
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            res += impls[i].size();

        return res;
    }

    bool empty() const
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            if (!impls[i].empty())
                return false;

        return true;
    }

    size_t getBufferSizeInCells() const
    {
        size_t res = 0;
        for (const auto & impl : impls)
            res += impl.getBufferSizeInCells();
        return res;
    }
    size_t getBufferSizeInBytes() const
    {
        size_t res = 0;
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            res += impls[i].getBufferSizeInBytes();

        return res;
    }
};

template <typename Data>
struct StringHashTableSubMapSelector<0, true, Data>
{
    struct Hash
    {
        static ALWAYS_INLINE size_t operator()(const StringRef &) { return 0; }
    };

    static typename Data::Impl::T0 & getSubMap(size_t hashval, Data & data)
    {
        const auto bucket = Data::getBucketFromHash(hashval);
        return data.impls[bucket].m0;
    }
};

template <typename Data>
struct StringHashTableSubMapSelector<1, true, Data>
{
    // using Hash = StringHashTableHash;
    using Hash = HashWithMixSeed<StringKey8>;

    static typename Data::Impl::T1 & getSubMap(size_t hashval, Data & data)
    {
        const auto bucket = Data::getBucketFromHash(hashval);
        return data.impls[bucket].m1;
    }
};

template <typename Data>
struct StringHashTableSubMapSelector<2, true, Data>
{
    // using Hash = StringHashTableHash;
    using Hash = HashWithMixSeed<StringKey16>;

    static typename Data::Impl::T2 & getSubMap(size_t hashval, Data & data)
    {
        const auto bucket = Data::getBucketFromHash(hashval);
        return data.impls[bucket].m2;
    }
};

template <typename Data>
struct StringHashTableSubMapSelector<3, true, Data>
{
    // using Hash = StringHashTableHash;
    using Hash = HashWithMixSeed<StringKey24>;

    static typename Data::Impl::T3 & getSubMap(size_t hashval, Data & data)
    {
        const auto bucket = Data::getBucketFromHash(hashval);
        return data.impls[bucket].m3;
    }
};

template <typename Data>
struct StringHashTableSubMapSelector<4, true, Data>
{
    // using Hash = StringHashTableHash;
    using Hash = StringRefHash;

    static typename Data::Impl::Ts & getSubMap(size_t hashval, Data & data)
    {
        const auto bucket = Data::getBucketFromHash(hashval);
        return data.impls[bucket].ms;
    }
};
