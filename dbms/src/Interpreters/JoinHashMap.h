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
#include <Common/HashTable/HashSet.h>
#include <Core/Block.h>

namespace DB
{
using Sizes = std::vector<size_t>;
/// Reference to the row in block.
struct RowRef
{
    const Block * block;
    UInt32 row_num;

    RowRef() = default;
    RowRef(const Block * block_, size_t row_num_)
        : block(block_)
        , row_num(row_num_)
    {}
};

enum class CachedColumnState
{
    NOT_CACHED,
    CONSTRUCT_CACHE,
    CACHED,
};
struct CachedColumnInfo
{
    std::mutex mu;
    Columns columns;
    CachedColumnState state = CachedColumnState::NOT_CACHED;
    void * next;
    explicit CachedColumnInfo(void * next_)
        : next(next_)
    {}
};

/// Single linked list of references to rows. Used for ALL JOINs (non-unique JOINs)
struct RowRefList : RowRef
{
    UInt32 list_length = 0;
    union
    {
        RowRefList * next = nullptr;
        CachedColumnInfo * cached_column_info;
    };

    RowRefList() = default;
    RowRefList(const Block * block_, size_t row_num_)
        : RowRef(block_, row_num_)
    {}
    /// for head node
    RowRefList(const Block * block_, size_t row_num_, bool sentinel_head)
        : RowRef(block_, row_num_)
        , list_length(sentinel_head ? 0 : 1)
    {}
};

/// Single linked list of references to rows with used flag for each row
struct RowRefListWithUsedFlag : RowRef
{
    UInt32 list_length = 0;
    using Base_t = RowRefListWithUsedFlag;
    mutable std::atomic<bool> used{};
    union
    {
        RowRefListWithUsedFlag * next = nullptr;
        CachedColumnInfo * cached_column_info;
    };

    void setUsed() const
    {
        used.store(true, std::memory_order_relaxed);
    } /// Could be set simultaneously from different threads.
    bool getUsed() const { return used.load(std::memory_order_relaxed); }

    RowRefListWithUsedFlag() = default;
    RowRefListWithUsedFlag(const Block * block_, size_t row_num_)
        : RowRef(block_, row_num_)
    {}
    /// for head node
    RowRefListWithUsedFlag(const Block * block_, size_t row_num_, bool sentinel_head)
        : RowRef(block_, row_num_)
        , list_length(sentinel_head ? 0 : 1)
    {}
};

/** Depending on template parameter, adds or doesn't add a flag, that element was used (row was joined).
      * For implementation of RIGHT and FULL JOINs.
      * NOTE: It is possible to store the flag in one bit of pointer to block or row_num. It seems not reasonable, because memory saving is minimal.
      */
template <bool enable, typename Base>
struct WithUsedFlag;

template <typename Base>
struct WithUsedFlag<true, Base> : Base
{
    mutable std::atomic<bool> used{};
    using Base::Base;
    using Base_t = Base;
    void setUsed() const
    {
        used.store(true, std::memory_order_relaxed);
    } /// Could be set simultaneously from different threads.
    bool getUsed() const { return used.load(std::memory_order_relaxed); }
};

template <typename Base>
struct WithUsedFlag<false, Base> : Base
{
    using Base::Base;
    using Base_t = Base;
    void setUsed() const {}
    bool getUsed() const { return true; }
};


/// Different types of keys for maps.
#define APPLY_FOR_JOIN_VARIANTS(M) \
    M(key8)                        \
    M(key16)                       \
    M(key32)                       \
    M(key64)                       \
    M(key_string)                  \
    M(key_strbinpadding)           \
    M(key_strbin)                  \
    M(key_fixed_string)            \
    M(keys128)                     \
    M(keys256)                     \
    M(serialized)

enum class JoinMapMethod
{
    EMPTY,
    CROSS,
#define M(NAME) NAME,
    APPLY_FOR_JOIN_VARIANTS(M)
#undef M
};


/** Different data structures, that are used to perform JOIN.
      */
template <typename Mapped>
struct ConcurrentMapsTemplate
{
    using MappedType = Mapped;
    using key8Type = ConcurrentHashMap<UInt8, Mapped, TrivialHash, HashTableFixedGrower<8>>;
    using key16Type = ConcurrentHashMap<UInt16, Mapped, TrivialHash, HashTableFixedGrower<16>>;
    using key32Type = ConcurrentHashMap<UInt32, Mapped, HashCRC32<UInt32>>;
    using key64Type = ConcurrentHashMap<UInt64, Mapped, HashCRC32<UInt64>>;
    using key_stringType = ConcurrentHashMapWithSavedHash<StringRef, Mapped>;
    using key_strbinpaddingType = ConcurrentHashMapWithSavedHash<StringRef, Mapped>;
    using key_strbinType = ConcurrentHashMapWithSavedHash<StringRef, Mapped>;
    using key_fixed_stringType = ConcurrentHashMapWithSavedHash<StringRef, Mapped>;
    using keys128Type = ConcurrentHashMap<UInt128, Mapped, HashCRC32<UInt128>>;
    using keys256Type = ConcurrentHashMap<UInt256, Mapped, HashCRC32<UInt256>>;
    using serializedType = ConcurrentHashMapWithSavedHash<StringRef, Mapped>;

    std::unique_ptr<key8Type> key8;
    std::unique_ptr<key16Type> key16;
    std::unique_ptr<key32Type> key32;
    std::unique_ptr<key64Type> key64;
    std::unique_ptr<key_stringType> key_string;
    std::unique_ptr<key_strbinpaddingType> key_strbinpadding;
    std::unique_ptr<key_strbinType> key_strbin;
    std::unique_ptr<key_fixed_stringType> key_fixed_string;
    std::unique_ptr<keys128Type> keys128;
    std::unique_ptr<keys256Type> keys256;
    std::unique_ptr<serializedType> serialized;
    // TODO: add more cases like Aggregator
};

template <typename Mapped>
struct MapsTemplate
{
    using MappedType = typename Mapped::Base_t;
    using key8Type = HashMap<UInt8, Mapped, TrivialHash, HashTableFixedGrower<8>>;
    using key16Type = HashMap<UInt16, Mapped, TrivialHash, HashTableFixedGrower<16>>;
    using key32Type = HashMap<UInt32, Mapped, HashCRC32<UInt32>>;
    using key64Type = HashMap<UInt64, Mapped, HashCRC32<UInt64>>;
    using key_stringType = HashMapWithSavedHash<StringRef, Mapped>;
    using key_strbinpaddingType = HashMapWithSavedHash<StringRef, Mapped>;
    using key_strbinType = HashMapWithSavedHash<StringRef, Mapped>;
    using key_fixed_stringType = HashMapWithSavedHash<StringRef, Mapped>;
    using keys128Type = HashMap<UInt128, Mapped, HashCRC32<UInt128>>;
    using keys256Type = HashMap<UInt256, Mapped, HashCRC32<UInt256>>;
    using serializedType = HashMapWithSavedHash<StringRef, Mapped>;

    std::unique_ptr<key8Type> key8;
    std::unique_ptr<key16Type> key16;
    std::unique_ptr<key32Type> key32;
    std::unique_ptr<key64Type> key64;
    std::unique_ptr<key_stringType> key_string;
    std::unique_ptr<key_strbinpaddingType> key_strbinpadding;
    std::unique_ptr<key_strbinType> key_strbin;
    std::unique_ptr<key_fixed_stringType> key_fixed_string;
    std::unique_ptr<keys128Type> keys128;
    std::unique_ptr<keys256Type> keys256;
    std::unique_ptr<serializedType> serialized;
    // TODO: add more cases like Aggregator
};

struct MapsAny
{
    using MappedType = VoidMapped;
    using key8Type = HashSet<UInt8, TrivialHash, HashTableFixedGrower<8>>;
    using key16Type = HashSet<UInt16, TrivialHash, HashTableFixedGrower<16>>;
    using key32Type = HashSet<UInt32, HashCRC32<UInt32>>;
    using key64Type = HashSet<UInt64, HashCRC32<UInt64>>;
    using key_stringType = HashSetWithSavedHash<StringRef>;
    using key_strbinpaddingType = HashSetWithSavedHash<StringRef>;
    using key_strbinType = HashSetWithSavedHash<StringRef>;
    using key_fixed_stringType = HashSetWithSavedHash<StringRef>;
    using keys128Type = HashSet<UInt128, HashCRC32<UInt128>>;
    using keys256Type = HashSet<UInt256, HashCRC32<UInt256>>;
    using serializedType = HashSetWithSavedHash<StringRef>;

    std::unique_ptr<key8Type> key8;
    std::unique_ptr<key16Type> key16;
    std::unique_ptr<key32Type> key32;
    std::unique_ptr<key64Type> key64;
    std::unique_ptr<key_stringType> key_string;
    std::unique_ptr<key_strbinpaddingType> key_strbinpadding;
    std::unique_ptr<key_strbinType> key_strbin;
    std::unique_ptr<key_fixed_stringType> key_fixed_string;
    std::unique_ptr<keys128Type> keys128;
    std::unique_ptr<keys256Type> keys256;
    std::unique_ptr<serializedType> serialized;
    // TODO: add more cases like Aggregator
};

using ConcurrentMapsAll = ConcurrentMapsTemplate<WithUsedFlag<false, RowRefList>>;
using ConcurrentMapsAllFull = ConcurrentMapsTemplate<WithUsedFlag<true, RowRefList>>;
using ConcurrentMapsAllFullWithRowFlag = ConcurrentMapsTemplate<RowRefListWithUsedFlag>;

using MapsAll = MapsTemplate<WithUsedFlag<false, RowRefList>>;
using MapsAllFull = MapsTemplate<WithUsedFlag<true, RowRefList>>;
using MapsAllFullWithRowFlag = MapsTemplate<RowRefListWithUsedFlag>; // With flag for every row ref

JoinMapMethod chooseJoinMapMethod(
    const ColumnRawPtrs & key_columns,
    Sizes & key_sizes,
    const TiDB::TiDBCollators & collators);
} // namespace DB
