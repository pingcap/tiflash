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


#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/ColumnsHashingImpl.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>
#include <Core/Defines.h>
#include <Interpreters/AggregationCommon.h>
#include <TiDB/Collation/Collator.h>
#include <common/memcpy.h>
#include <common/unaligned.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace ColumnsHashing
{
/// For the case when there is one numeric key.
/// UInt8/16/32/64 for any type with corresponding bit width.
template <typename Value, typename Mapped, typename FieldType, bool use_cache = true>
struct HashMethodOneNumber
    : public columns_hashing_impl::
          HashMethodBase<HashMethodOneNumber<Value, Mapped, FieldType, use_cache>, Value, Mapped, use_cache>
{
    using Self = HashMethodOneNumber<Value, Mapped, FieldType, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    const FieldType * vec;
    const size_t total_rows;

    /// If the keys of a fixed length then key_sizes contains their lengths, empty otherwise.
    HashMethodOneNumber(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const TiDB::TiDBCollators &)
        : total_rows(key_columns[0]->size())
    {
        vec = &static_cast<const ColumnVector<FieldType> *>(key_columns[0])->getData()[0];
    }

    explicit HashMethodOneNumber(const IColumn * column)
        : total_rows(column->size())
    {
        vec = &static_cast<const ColumnVector<FieldType> *>(column)->getData()[0];
    }

    /// Emplace key into HashTable or HashMap. If Data is HashMap, returns ptr to value, otherwise nullptr.
    /// Data is a HashTable where to insert key from column's row.
    /// For Serialized method, key may be placed in pool.
    using Base::emplaceKey; /// (Data & data, size_t row, Arena & pool) -> EmplaceResult

    /// Find key into HashTable or HashMap. If Data is HashMap and key was found, returns ptr to value, otherwise nullptr.
    using Base::findKey; /// (Data & data, size_t row, Arena & pool) -> FindResult

    /// Get hash value of row.
    using Base::getHash; /// (const Data & data, size_t row, Arena & pool) -> size_t

    /// Is used for default implementation in HashMethodBase.
    ALWAYS_INLINE inline FieldType getKeyHolder(size_t row, Arena *, std::vector<String> &) const
    {
        if constexpr (std::is_same_v<FieldType, Int256>)
            return vec[row];
        else
            return unalignedLoad<FieldType>(vec + row);
    }

    const FieldType * getKeyData() const { return vec; }
};


/// For the case when there is one string key.
template <typename Value, typename Mapped, bool use_cache = true>
struct HashMethodString
    : public columns_hashing_impl::HashMethodBase<HashMethodString<Value, Mapped, use_cache>, Value, Mapped, use_cache>
{
    using Self = HashMethodString<Value, Mapped, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    const IColumn::Offset * offsets;
    const UInt8 * chars;
    TiDB::TiDBCollatorPtr collator = nullptr;
    const size_t total_rows;

    HashMethodString(
        const ColumnRawPtrs & key_columns,
        const Sizes & /*key_sizes*/,
        const TiDB::TiDBCollators & collators)
        : total_rows(key_columns[0]->size())
    {
        const IColumn & column = *key_columns[0];
        const auto & column_string = assert_cast<const ColumnString &>(column);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();
        if (!collators.empty())
            collator = collators[0];
    }

    ALWAYS_INLINE inline ArenaKeyHolder getKeyHolder(
        ssize_t row,
        [[maybe_unused]] Arena * pool,
        [[maybe_unused]] std::vector<String> & sort_key_containers) const
    {
        auto key = getKey(row);
        if (likely(collator))
            key = collator->sortKey(key.data, key.size, sort_key_containers[0]);

        return ArenaKeyHolder{key, pool};
    }

    ALWAYS_INLINE inline ArenaKeyHolder getKeyHolder(ssize_t row, Arena * pool, Arena * sort_key_pool) const
    {
        auto key = getKey(row);
        if (likely(collator))
            key = collator->sortKey(key.data, key.size, *sort_key_pool);

        return ArenaKeyHolder{key, pool};
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

private:
    ALWAYS_INLINE inline StringRef getKey(size_t row) const
    {
        auto last_offset = row == 0 ? 0 : offsets[row - 1];
        // Remove last zero byte.
        return StringRef(chars + last_offset, offsets[row] - last_offset - 1);
    }
};

template <typename Value, typename Mapped, bool padding>
struct HashMethodStringBin
    : public columns_hashing_impl::HashMethodBase<HashMethodStringBin<Value, Mapped, padding>, Value, Mapped, false>
{
    using Self = HashMethodStringBin<Value, Mapped, padding>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    const IColumn::Offset * offsets;
    const UInt8 * chars;
    const size_t total_rows;

    HashMethodStringBin(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const TiDB::TiDBCollators &)
        : total_rows(key_columns[0]->size())
    {
        const IColumn & column = *key_columns[0];
        const auto & column_string = assert_cast<const ColumnString &>(column);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();
    }

    ALWAYS_INLINE inline auto getKeyHolder(ssize_t row, Arena * pool, std::vector<String> &) const
    {
        return getKeyHolder(row, pool, nullptr);
    }

    ALWAYS_INLINE inline auto getKeyHolder(ssize_t row, Arena * pool, Arena *) const
    {
        auto last_offset = row == 0 ? 0 : offsets[row - 1];
        StringRef key(chars + last_offset, offsets[row] - last_offset - 1);
        key = BinCollatorSortKey<padding>(key.data, key.size);
        return ArenaKeyHolder{key, pool};
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;
};

/*
/// For the case when there is multi string key.
template <typename Value, typename Mapped>
struct HashMethodMultiString
    : public columns_hashing_impl::HashMethodBase<HashMethodMultiString<Value, Mapped>, Value, Mapped, false>
{
    using Self = HashMethodMultiString<Value, Mapped>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    std::vector<const IColumn::Offset *> offsets;
    std::vector<const UInt8 *> chars;
    TiDB::TiDBCollators collators;
    bool all_collators_padding_bin = false;

    HashMethodMultiString(const ColumnRawPtrs & key_columns, const Sizes &, const TiDB::TiDBCollators & collators_)
        : collators(collators_)
    {
        size_t num = key_columns.size();
        offsets.resize(num);
        chars.resize(num);

        for (size_t i = 0; i < num; ++i)
        {
            const IColumn & column = *key_columns[i];
            const auto & column_string = assert_cast<const ColumnString &>(column);
            offsets[i] = column_string.getOffsets().data();
            chars[i] = column_string.getChars().data();
        }
        if (!collators.empty())
        {
            all_collators_padding_bin = std::all_of(collators.begin(), collators.end(), [](auto & x) {
                return x->isPaddingBinary();
            });
        }
    }

    template <typename F>
    ALWAYS_INLINE inline SerializedKeyHolder genSerializedKeyHolder(ssize_t row, Arena * pool, F && fn_handle_key) const
    {
        auto num = offsets.size();

        const char * begin = nullptr;
        size_t sum_size = 0;

        for (size_t key_index = 0; key_index < num; ++key_index)
        {
            auto last_offset = row == 0 ? 0 : offsets[key_index][row - 1];
            StringRef key(chars[key_index] + last_offset, offsets[key_index][row] - last_offset - 1);

            key = fn_handle_key(key_index, key);

            char * pos = pool->allocContinue(key.size + sizeof(key.size), begin);
            {
                memcpy(pos, &key.size, sizeof(key.size));
                inline_memcpy(pos + sizeof(key.size), key.data, key.size);
            }

            sum_size += key.size + sizeof(key.size);
        }
        return SerializedKeyHolder{{begin, sum_size}, *pool};
    }

    ALWAYS_INLINE inline auto getKeyHolder(ssize_t row, Arena * pool, std::vector<String> & sort_key_containers) const
    {
        if (likely(all_collators_padding_bin))
        {
            return genSerializedKeyHolder(row, pool, [](size_t, StringRef key) {
                return DB::BinCollatorSortKey<true>(key.data, key.size);
            });
        }

        if (unlikely(collators.empty()))
        {
            return genSerializedKeyHolder(row, pool, [](size_t, StringRef key) {
                return key;
            });
        }
        else
        {
            return genSerializedKeyHolder(row, pool, [&](size_t key_index, StringRef key) {
                if (collators[key_index])
                    return collators[key_index]->sortKey(key.data, key.size, sort_key_containers[key_index]);
                return key;
            });
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;
};
*/

static_assert(std::is_same_v<size_t, decltype(reinterpret_cast<const StringRef *>(0)->size)>);

struct KeyDescNumber64
{
    using ColumnType = ColumnUInt64;
    using AllocSize = size_t;
    static constexpr size_t ElementSize = sizeof(ColumnType::value_type);

    explicit KeyDescNumber64(const IColumn * key_column_) { column = static_cast<const ColumnType *>(key_column_); }
    static inline void serializeKey(char *& pos, const StringRef & ref)
    {
        std::memcpy(pos, ref.data, ElementSize);
        pos += ElementSize;
    }
    ALWAYS_INLINE inline AllocSize getKey(ssize_t row, StringRef & ref) const
    {
        const auto & element = column->getElement(row);
        ref = {reinterpret_cast<const char *>(&element), ElementSize};
        return ElementSize;
    }
    const ColumnType * column{};
};

struct KeyDescStringBin
{
    using ColumnType = ColumnString;
    using AllocSize = size_t;

    explicit KeyDescStringBin(const IColumn * key_column_) { column = static_cast<const ColumnType *>(key_column_); }
    static inline void serializeKey(char *& pos, const StringRef & ref)
    {
        std::memcpy(pos, &ref.size, sizeof(ref.size));
        pos += sizeof(ref.size);
        inline_memcpy(pos, ref.data, ref.size);
        pos += ref.size;
    }

    template <typename F>
    ALWAYS_INLINE inline AllocSize getKeyImpl(ssize_t row, StringRef & key, F && fn_handle_key) const
    {
        const auto * offsets = column->getOffsets().data();
        const auto * chars = column->getChars().data();

        size_t last_offset = 0;
        if (likely(row != 0))
            last_offset = offsets[row - 1];

        key = {chars + last_offset, offsets[row] - last_offset - 1};
        key = fn_handle_key(key);

        return key.size + sizeof(key.size);
    }

    ALWAYS_INLINE inline AllocSize getKey(ssize_t row, StringRef & ref) const
    {
        return getKeyImpl(row, ref, [](StringRef key) { return key; });
    }

    const ColumnType * column{};
};

struct KeyDescStringBinPadding : KeyDescStringBin
{
    explicit KeyDescStringBinPadding(const IColumn * key_column_)
        : KeyDescStringBin(key_column_)
    {}

    ALWAYS_INLINE inline AllocSize getKey(ssize_t row, StringRef & ref) const
    {
        return getKeyImpl(row, ref, [](StringRef key) { return DB::BinCollatorSortKey<true>(key.data, key.size); });
    }
};

/// For the case when there are 2 keys.
template <typename Key1Desc, typename Key2Desc, typename Value, typename Mapped>
struct HashMethodFastPathTwoKeysSerialized
    : public columns_hashing_impl::
          HashMethodBase<HashMethodFastPathTwoKeysSerialized<Key1Desc, Key2Desc, Value, Mapped>, Value, Mapped, false>
{
    using Self = HashMethodFastPathTwoKeysSerialized<Key1Desc, Key2Desc, Value, Mapped>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    Key1Desc key_1_desc;
    Key2Desc key_2_desc;
    const size_t total_rows;

    HashMethodFastPathTwoKeysSerialized(const ColumnRawPtrs & key_columns, const Sizes &, const TiDB::TiDBCollators &)
        : key_1_desc(key_columns[0])
        , key_2_desc(key_columns[1])
        , total_rows(key_columns[0]->size())
    {}

    ALWAYS_INLINE inline auto getKeyHolder(ssize_t row, Arena * pool, std::vector<String> &) const
    {
        StringRef key1;
        StringRef key2;
        size_t alloc_size = key_1_desc.getKey(row, key1) + key_2_desc.getKey(row, key2);
        char * start = pool->alloc(alloc_size);
        SerializedKeyHolder ret{{start, alloc_size}, *pool};
        Key1Desc::serializeKey(start, key1);
        Key2Desc::serializeKey(start, key2);
        return ret;
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;
};


/// For the case when there is one fixed-length string key.
template <typename Value, typename Mapped, bool use_cache = true>
struct HashMethodFixedString
    : public columns_hashing_impl::
          HashMethodBase<HashMethodFixedString<Value, Mapped, use_cache>, Value, Mapped, use_cache>
{
    using Self = HashMethodFixedString<Value, Mapped, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    size_t n;
    const ColumnFixedString::Chars_t * chars;
    TiDB::TiDBCollatorPtr collator = nullptr;
    const size_t total_rows;

    HashMethodFixedString(
        const ColumnRawPtrs & key_columns,
        const Sizes & /*key_sizes*/,
        const TiDB::TiDBCollators & collators)
        : total_rows(key_columns[0]->size())
    {
        const IColumn & column = *key_columns[0];
        const auto & column_string = assert_cast<const ColumnFixedString &>(column);
        n = column_string.getN();
        chars = &column_string.getChars();
        if (!collators.empty())
            collator = collators[0];
    }

    ALWAYS_INLINE inline ArenaKeyHolder getKeyHolder(
        size_t row,
        Arena * pool,
        std::vector<String> & sort_key_containers) const
    {
        StringRef key(&(*chars)[row * n], n);
        if (collator)
            key = collator->sortKeyFastPath(key.data, key.size, sort_key_containers[0]);

        return ArenaKeyHolder{key, pool};
    }

    ALWAYS_INLINE inline ArenaKeyHolder getKeyHolder(size_t row, Arena * pool, Arena * sort_key_pool) const
    {
        StringRef key(&(*chars)[row * n], n);
        if (collator)
            key = collator->sortKeyFastPath(key.data, key.size, *sort_key_pool);

        return ArenaKeyHolder{key, pool};
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
};

/// For the case when all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename Value, typename Key, typename Mapped, bool has_nullable_keys_ = false, bool use_cache = true>
struct HashMethodKeysFixed
    : private columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>
    , public columns_hashing_impl::HashMethodBase<
          HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, use_cache>,
          Value,
          Mapped,
          use_cache>
{
    using Self = HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, use_cache>;
    using BaseHashed = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
    using Base = columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>;

    static constexpr bool has_nullable_keys = has_nullable_keys_;

    Sizes key_sizes;
    size_t keys_size;
    const size_t total_rows;

    /// SSSE3 shuffle method can be used. Shuffle masks will be calculated and stored here.
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
    std::unique_ptr<uint8_t[]> masks;
    std::unique_ptr<const char *[]> columns_data;
#endif

    PaddedPODArray<Key> prepared_keys;

    static bool usePreparedKeys(const Sizes & key_sizes)
    {
        if (has_nullable_keys || sizeof(Key) > 16)
            return false;

        for (auto size : key_sizes)
            if (size != 1 && size != 2 && size != 4 && size != 8 && size != 16)
                return false;

        return true;
    }

    HashMethodKeysFixed(const ColumnRawPtrs & key_columns, const Sizes & key_sizes_, const TiDB::TiDBCollators &)
        : Base(key_columns)
        , key_sizes(std::move(key_sizes_))
        , keys_size(key_columns.size())
        , total_rows(key_columns[0]->size())
    {
        if (usePreparedKeys(key_sizes))
        {
            packFixedBatch(keys_size, Base::getActualColumns(), key_sizes, prepared_keys);
        }

#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
        else if constexpr (!has_nullable_keys && sizeof(Key) <= 16)
        {
            /** The task is to "pack" multiple fixed-size fields into single larger Key.
              * Example: pack UInt8, UInt32, UInt16, UInt64 into UInt128 key:
              * [- ---- -- -------- -] - the resulting uint128 key
              *  ^  ^   ^   ^       ^
              *  u8 u32 u16 u64    zero
              *
              * We can do it with the help of SSSE3 shuffle instruction.
              *
              * There will be a mask for every GROUP BY element (keys_size masks in total).
              * Every mask has 16 bytes but only sizeof(Key) bytes are used (other we don't care).
              *
              * Every byte in the mask has the following meaning:
              * - if it is 0..15, take the element at this index from source register and place here in the result;
              * - if it is 0xFF - set the elemend in the result to zero.
              *
              * Example:
              * We want to copy UInt32 to offset 1 in the destination and set other bytes in the destination as zero.
              * The corresponding mask will be: FF, 0, 1, 2, 3, FF, FF, FF, FF, FF, FF, FF, FF, FF, FF, FF
              *
              * The max size of destination is 16 bytes, because we cannot process more with SSSE3.
              *
              * The method is disabled under MSan, because it's allowed
              * to load into SSE register and process up to 15 bytes of uninitialized memory in columns padding.
              * We don't use this uninitialized memory but MSan cannot look "into" the shuffle instruction.
              *
              * 16-bytes masks can be placed overlapping, only first sizeof(Key) bytes are relevant in each mask.
              * We initialize them to 0xFF and then set the needed elements.
              */
            size_t total_masks_size = sizeof(Key) * keys_size + (16 - sizeof(Key));
            masks.reset(new uint8_t[total_masks_size]);
            memset(masks.get(), 0xFF, total_masks_size);

            size_t offset = 0;
            for (size_t i = 0; i < keys_size; ++i)
            {
                for (size_t j = 0; j < key_sizes[i]; ++j)
                {
                    masks[i * sizeof(Key) + offset] = j;
                    ++offset;
                }
            }

            columns_data.reset(new const char *[keys_size]);

            for (size_t i = 0; i < keys_size; ++i)
                columns_data[i] = Base::getActualColumns()[i]->getRawData().data;
        }
#endif
    }

    ALWAYS_INLINE inline Key getKeyHolder(size_t row, Arena *, std::vector<String> &) const
    {
        if constexpr (has_nullable_keys)
        {
            auto bitmap = Base::createBitmap(row);
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes, bitmap);
        }
        else
        {
            if (!prepared_keys.empty())
                return prepared_keys[row];

#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
            if constexpr (sizeof(Key) <= 16)
            {
                assert(!has_nullable_keys);
                return packFixedShuffle<Key>(columns_data.get(), keys_size, key_sizes.data(), row, masks.get());
            }
#endif
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes);
        }
    }

    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        if (!usePreparedKeys(key_sizes))
            return {};

        std::vector<IColumn *> new_columns;
        new_columns.reserve(key_columns.size());

        Sizes new_sizes;
        auto fill_size = [&](size_t size) {
            for (size_t i = 0; i < key_sizes.size(); ++i)
            {
                if (key_sizes[i] == size)
                {
                    new_columns.push_back(key_columns[i]);
                    new_sizes.push_back(size);
                }
            }
        };

        fill_size(16);
        fill_size(8);
        fill_size(4);
        fill_size(2);
        fill_size(1);

        key_columns.swap(new_columns);
        return new_sizes;
    }
};


/** Hash by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename Value, typename Mapped>
struct HashMethodSerialized
    : public columns_hashing_impl::HashMethodBase<HashMethodSerialized<Value, Mapped>, Value, Mapped, false>
{
    using Self = HashMethodSerialized<Value, Mapped>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ColumnRawPtrs key_columns;
    size_t keys_size;
    TiDB::TiDBCollators collators;
    const size_t total_rows;

    HashMethodSerialized(
        const ColumnRawPtrs & key_columns_,
        const Sizes & /*key_sizes*/,
        const TiDB::TiDBCollators & collators_)
        : key_columns(key_columns_)
        , keys_size(key_columns_.size())
        , collators(collators_)
        , total_rows(key_columns_[0]->size())
    {}

    ALWAYS_INLINE inline SerializedKeyHolder getKeyHolder(
        size_t row,
        Arena * pool,
        std::vector<String> & sort_key_containers) const
    {
        return SerializedKeyHolder{
            serializeKeysToPoolContiguous(row, keys_size, key_columns, collators, sort_key_containers, *pool),
            *pool};
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;
};

/// For the case when there is one string key.
template <typename Value, typename Mapped, bool use_cache = true>
struct HashMethodHashed
    : public columns_hashing_impl::HashMethodBase<HashMethodHashed<Value, Mapped, use_cache>, Value, Mapped, use_cache>
{
    using Key = UInt128;
    using Self = HashMethodHashed<Value, Mapped, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    ColumnRawPtrs key_columns;
    TiDB::TiDBCollators collators;
    const size_t total_rows;

    HashMethodHashed(ColumnRawPtrs key_columns_, const Sizes &, const TiDB::TiDBCollators & collators_)
        : key_columns(std::move(key_columns_))
        , collators(collators_)
        , total_rows(key_columns[0]->size())
    {}

    ALWAYS_INLINE inline Key getKeyHolder(size_t row, Arena *, std::vector<String> & sort_key_containers) const
    {
        return hash128(row, key_columns.size(), key_columns, collators, sort_key_containers);
    }
};

} // namespace ColumnsHashing
} // namespace DB
