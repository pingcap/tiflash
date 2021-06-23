#pragma once


#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ColumnsHashingImpl.h>
#include <Common/Arena.h>
#include <Common/assert_cast.h>
#include <common/unaligned.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include <Core/Defines.h>
#include <memory>

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
    : public columns_hashing_impl::HashMethodBase<HashMethodOneNumber<Value, Mapped, FieldType, use_cache>, Value, Mapped, use_cache>
{
    using Self = HashMethodOneNumber<Value, Mapped, FieldType, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    const char * vec;

    /// If the keys of a fixed length then key_sizes contains their lengths, empty otherwise.
    HashMethodOneNumber(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const TiDB::TiDBCollators &)
    {
        vec = key_columns[0]->getRawData().data;
    }

    HashMethodOneNumber(const IColumn * column)
    {
        vec = column->getRawData().data;
    }

    /// Emplace key into HashTable or HashMap. If Data is HashMap, returns ptr to value, otherwise nullptr.
    /// Data is a HashTable where to insert key from column's row.
    /// For Serialized method, key may be placed in pool.
    using Base::emplaceKey; /// (Data & data, size_t row, Arena & pool) -> EmplaceResult

    /// Find key into HashTable or HashMap. If Data is HashMap and key was found, returns ptr to value, otherwise nullptr.
    using Base::findKey;  /// (Data & data, size_t row, Arena & pool) -> FindResult

    /// Get hash value of row.
    using Base::getHash; /// (const Data & data, size_t row, Arena & pool) -> size_t

    /// Is used for default implementation in HashMethodBase.
    FieldType getKeyHolder(size_t row, Arena &, std::vector<String> &) const
    {
        if constexpr(std::is_same_v<FieldType, Int256>)
            return vec[i];
        else
            return unalignedLoad<FieldType>(vec + row * sizeof(FieldType));
    }
};


/// For the case when there is one string key.
template <typename Value, typename Mapped, bool place_string_to_arena = true, bool use_cache = true>
struct HashMethodString
    : public columns_hashing_impl::HashMethodBase<HashMethodString<Value, Mapped, place_string_to_arena, use_cache>, Value, Mapped, use_cache>
{
    using Self = HashMethodString<Value, Mapped, place_string_to_arena, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    const IColumn::Offset * offsets;
    const UInt8 * chars;

    HashMethodString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const TiDB::TiDBCollators & collators)
    {
        const IColumn & column = *key_columns[0];
        const ColumnString & column_string = assert_cast<const ColumnString &>(column);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();
        if (!collators.empty())
            throw Exception{"Internal error: shouldn't pass collator to HashMethodString", ErrorCodes::LOGICAL_ERROR};
    }

    auto getKeyHolder(ssize_t row, [[maybe_unused]] Arena & pool, std::vector<String> &) const
    {
        StringRef key(chars + offsets[row - 1], offsets[row] - offsets[row - 1] - 1);

        if constexpr (place_string_to_arena)
        {
            return ArenaKeyHolder{key, pool};
        }
        else
        {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
};

//
/// For the case when there is one string key.
template <typename Value, typename Mapped, bool place_string_to_arena = true>
struct HashMethodStringWithCollator
    : public columns_hashing_impl::HashMethodBase<HashMethodStringWithCollator<Value, Mapped, place_string_to_arena>, Value, Mapped, false>
{
    using Self = HashMethodStringWithCollator<Value, Mapped, place_string_to_arena>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    const IColumn::Offset * offsets;
    const UInt8 * chars;

    HashMethodStringWithCollator(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const TiDB::TiDBCollators & collators)
    {
        const IColumn & column = *key_columns[0];
        const ColumnString & column_string = assert_cast<const ColumnString &>(column);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();
        if (!collators.empty())
            collator = collators[0];
    }

    auto getKeyHolder(ssize_t row, [[maybe_unused]] Arena & pool, std::vector<String> & sort_key_containers) const
    {
        StringRef key(chars + offsets[row - 1], offsets[row] - offsets[row - 1] - 1);

        if (collator)
        {
            key = collator->sortKey(key.data, key.size, sort_key_containers[0]);
        }

        if constexpr (place_string_to_arena)
        {
            return ArenaKeyHolder{key, pool};
        }
        else
        {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
};


/// For the case when there is one fixed-length string key.
template <typename Value, typename Mapped, bool place_string_to_arena = true, bool use_cache = true>
struct HashMethodFixedString
    : public columns_hashing_impl::HashMethodBase<HashMethodFixedString<Value, Mapped, place_string_to_arena, use_cache>, Value, Mapped, use_cache>
{
    using Self = HashMethodFixedString<Value, Mapped, place_string_to_arena, use_cache>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;

    size_t n;
    const ColumnFixedString::Chars * chars;

    HashMethodFixedString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const TiDB::TiDBCollators &)
    {
        const IColumn & column = *key_columns[0];
        const ColumnFixedString & column_string = assert_cast<const ColumnFixedString &>(column);
        n = column_string.getN();
        chars = &column_string.getChars();
    }

    auto getKeyHolder(size_t row, [[maybe_unused]] Arena & pool, std::vector<String> &) const
    {
        StringRef key(&(*chars)[row * n], n);

        if constexpr (place_string_to_arena)
        {
            return ArenaKeyHolder{key, pool};
        }
        else
        {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
};


/// For the case when all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename Value, typename Key, typename Mapped, bool has_nullable_keys_ = false, bool use_cache = true>
struct HashMethodKeysFixed
    : private columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>
    , public columns_hashing_impl::HashMethodBase<HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, use_cache>, Value, Mapped, use_cache>
{
    using Self = HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, use_cache>;
    using BaseHashed = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
    using Base = columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>;

    static constexpr bool has_nullable_keys = has_nullable_keys_;

    Sizes key_sizes;
    size_t keys_size;

    HashMethodKeysFixed(const ColumnRawPtrs & key_columns, const Sizes & key_sizes_, const TiDB::TiDBCollators &)
        : Base(key_columns), key_sizes(std::move(key_sizes_)), keys_size(key_columns.size())
    {
    }

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &, std::vector<String> &) const
    {
        if constexpr (has_nullable_keys)
        {
            auto bitmap = Base::createBitmap(row);
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes, bitmap);
        }
        else
        {
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes);
        }
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

    HashMethodSerialized(const ColumnRawPtrs & key_columns_, const Sizes & /*key_sizes*/, const TiDB::TiDBCollators & collators_)
        : key_columns(key_columns_), keys_size(key_columns_.size()), collators(collators_) {}

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ALWAYS_INLINE SerializedKeyHolder getKeyHolder(size_t row, Arena & pool, std::vector<String> & sort_key_containers) const
    {
        return SerializedKeyHolder{
            serializeKeysToPoolContiguous(row, keys_size, key_columns, collators, sort_key_containers, pool),
            pool};
    }
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

    HashMethodHashed(ColumnRawPtrs key_columns_, const Sizes &, const TiDB::TiDBCollators &)
        : key_columns(std::move(key_columns_)) {}

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &, std::vector<String> &) const
    {
        return hash128(row, key_columns.size(), key_columns);
    }
};

} // namespace ColumnsHashing
} // namespace DB

