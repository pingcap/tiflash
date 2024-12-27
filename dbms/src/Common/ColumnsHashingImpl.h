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

#include <Columns/IColumn.h>
#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/HashTable/StringHashTable.h>
#include <Common/assert_cast.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/AggregationCommon.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace ColumnsHashing
{
namespace columns_hashing_impl
{
template <typename Value, bool consecutive_keys_optimization_>
struct LastElementCache
{
    static constexpr bool consecutive_keys_optimization = consecutive_keys_optimization_;
    Value value;
    bool empty = true;
    bool found = false;

    bool check(const Value & value_) { return !empty && value == value_; }

    template <typename Key>
    bool check(const Key & key)
    {
        return !empty && value.first == key;
    }
};

template <typename Data>
struct LastElementCache<Data, false>
{
    static constexpr bool consecutive_keys_optimization = false;
};

template <typename Mapped>
class EmplaceResultImpl
{
    Mapped * value = nullptr;
    Mapped * cached_value = nullptr;
    bool inserted = false;

public:
    EmplaceResultImpl(Mapped & value_, Mapped & cached_value_, bool inserted_)
        : value(&value_)
        , cached_value(&cached_value_)
        , inserted(inserted_)
    {}

    bool isInserted() const { return inserted; }
    auto & getMapped() const { return *value; }

    void setMapped(const Mapped & mapped)
    {
        *cached_value = mapped;
        *value = mapped;
    }
};

template <>
class EmplaceResultImpl<VoidMapped>
{
    bool inserted;

public:
    explicit EmplaceResultImpl(bool inserted_)
        : inserted(inserted_)
    {}
    bool isInserted() const { return inserted; }
};

template <typename Mapped>
class FindResultImpl
{
    Mapped * value;
    bool found;

public:
    FindResultImpl(Mapped * value_, bool found_)
        : value(value_)
        , found(found_)
    {}
    bool isFound() const { return found; }
    Mapped & getMapped() const { return *value; }
};

template <>
class FindResultImpl<VoidMapped>
{
    bool found;

public:
    explicit FindResultImpl(bool found_)
        : found(found_)
    {}
    bool isFound() const { return found; }
};

template <typename TDerived, typename Value, typename Mapped, bool consecutive_keys_optimization>
class HashMethodBase
{
public:
    using EmplaceResult = EmplaceResultImpl<Mapped>;
    using FindResult = FindResultImpl<Mapped>;
    static constexpr bool has_mapped = !std::is_same<Mapped, VoidMapped>::value;
    using Cache = LastElementCache<Value, consecutive_keys_optimization>;
    using Derived = TDerived;

    // Emplace key without hashval, and this method doesn't support prefetch.
    template <typename Data>
    ALWAYS_INLINE inline EmplaceResult emplaceKey(
        Data & data,
        size_t row,
        Arena & pool,
        std::vector<String> & sort_key_containers)
    {
        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, &pool, sort_key_containers);
        return emplaceImpl(key_holder, data);
    }

    template <typename KeyHolder, typename Data>
    ALWAYS_INLINE inline EmplaceResult emplaceKey(Data & data, KeyHolder && key_holder, size_t hashval)
    {
        return emplaceImpl(key_holder, data, hashval);
    }

    template <typename Data>
    ALWAYS_INLINE inline FindResult findKey(
        Data & data,
        size_t row,
        Arena & pool,
        std::vector<String> & sort_key_containers)
    {
        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, &pool, sort_key_containers);
        return findKeyImpl(keyHolderGetKey(key_holder), data);
    }

    template <typename KeyHolder, typename Data>
    ALWAYS_INLINE inline FindResult findKey(Data & data, KeyHolder && key_holder, size_t hashval)
    {
        return findKeyImpl(keyHolderGetKey(key_holder), data, hashval);
    }

    template <typename Data>
    ALWAYS_INLINE inline size_t getHash(
        const Data & data,
        size_t row,
        Arena & pool,
        std::vector<String> & sort_key_containers) const
    {
        auto key_holder = static_cast<const Derived &>(*this).getKeyHolder(row, &pool, sort_key_containers);
        return data.hash(keyHolderGetKey(key_holder));
    }

protected:
    Cache cache;

    HashMethodBase()
    {
        if constexpr (consecutive_keys_optimization)
        {
            if constexpr (has_mapped)
            {
                /// Init PairNoInit elements.
                cache.value.second = Mapped();
                cache.value.first = {};
            }
            else
                cache.value = Value();
        }
    }

    // This method is performance critical, so there are two emplaceImpl to make sure caller can use the one they need.
    template <typename Data, typename KeyHolder>
    ALWAYS_INLINE inline EmplaceResult emplaceImpl(KeyHolder & key_holder, Data & data, size_t hashval)
    {
        if constexpr (Cache::consecutive_keys_optimization)
        {
            if (cache.found && cache.check(keyHolderGetKey(key_holder)))
            {
                if constexpr (has_mapped)
                    return EmplaceResult(cache.value.second, cache.value.second, false);
                else
                    return EmplaceResult(false);
            }
        }

        typename Data::LookupResult it;
        bool inserted = false;

        data.emplace(key_holder, it, inserted, hashval);

        return handleEmplaceResult<Data>(it, inserted);
    }

    template <typename Data, typename KeyHolder>
    ALWAYS_INLINE inline EmplaceResult emplaceImpl(KeyHolder & key_holder, Data & data)
    {
        if constexpr (Cache::consecutive_keys_optimization)
        {
            if (cache.found && cache.check(keyHolderGetKey(key_holder)))
            {
                if constexpr (has_mapped)
                    return EmplaceResult(cache.value.second, cache.value.second, false);
                else
                    return EmplaceResult(false);
            }
        }

        typename Data::LookupResult it;
        bool inserted = false;

        data.emplace(key_holder, it, inserted);

        return handleEmplaceResult<Data>(it, inserted);
    }

    template <typename Data>
    ALWAYS_INLINE inline EmplaceResult handleEmplaceResult(typename Data::LookupResult & it, bool inserted)
    {
        [[maybe_unused]] Mapped * cached = nullptr;
        if constexpr (has_mapped)
            cached = &it->getMapped();

        if (inserted)
        {
            if constexpr (has_mapped)
            {
                new (&it->getMapped()) Mapped();
            }
        }

        if constexpr (consecutive_keys_optimization)
        {
            cache.found = true;
            cache.empty = false;

            if constexpr (has_mapped)
            {
                cache.value.first = it->getKey();
                cache.value.second = it->getMapped();
                cached = &cache.value.second;
            }
            else
            {
                cache.value = it->getKey();
            }
        }

        if constexpr (has_mapped)
            return EmplaceResult(it->getMapped(), *cached, inserted);
        else
            return EmplaceResult(inserted);
    }

    template <typename Data, typename Key>
    ALWAYS_INLINE inline FindResult findKeyImpl(Key & key, Data & data)
    {
        if constexpr (Cache::consecutive_keys_optimization)
        {
            if (cache.check(key))
            {
                if constexpr (has_mapped)
                    return FindResult(&cache.value.second, cache.found);
                else
                    return FindResult(cache.found);
            }
        }

        typename Data::LookupResult it;
        it = data.find(key);

        return handleFindResult<Data, Key>(key, it);
    }

    template <typename Data, typename Key>
    ALWAYS_INLINE inline FindResult findKeyImpl(Key & key, Data & data, size_t hashval)
    {
        if constexpr (Cache::consecutive_keys_optimization)
        {
            if (cache.check(key))
            {
                if constexpr (has_mapped)
                    return FindResult(&cache.value.second, cache.found);
                else
                    return FindResult(cache.found);
            }
        }

        typename Data::LookupResult it;
        it = data.find(key, hashval);

        return handleFindResult<Data, Key>(key, it);
    }

    template <typename Data, typename Key>
    ALWAYS_INLINE inline FindResult handleFindResult(Key & key, typename Data::LookupResult & it)
    {
        if constexpr (consecutive_keys_optimization)
        {
            cache.found = it != nullptr;
            cache.empty = false;

            if constexpr (has_mapped)
            {
                cache.value.first = key;
                if (it)
                {
                    cache.value.second = it->getMapped();
                }
            }
            else
            {
                cache.value = key;
            }
        }

        if constexpr (has_mapped)
            return FindResult(it ? &it->getMapped() : nullptr, it != nullptr);
        else
            return FindResult(it != nullptr);
    }
};


template <typename T>
struct MappedCache : public PaddedPODArray<T>
{
};

template <>
struct MappedCache<VoidMapped>
{
};


/// This class is designed to provide the functionality that is required for
/// supporting nullable keys in HashMethodKeysFixed. If there are
/// no nullable keys, this class is merely implemented as an empty shell.
template <typename Key, bool has_nullable_keys>
class BaseStateKeysFixed;

/// Case where nullable keys are supported.
template <typename Key>
class BaseStateKeysFixed<Key, true>
{
protected:
    explicit BaseStateKeysFixed(const ColumnRawPtrs & key_columns)
    {
        null_maps.reserve(key_columns.size());
        actual_columns.reserve(key_columns.size());

        for (const auto & col : key_columns)
        {
            if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(col))
            {
                actual_columns.push_back(&nullable_col->getNestedColumn());
                null_maps.push_back(&nullable_col->getNullMapColumn());
            }
            else
            {
                actual_columns.push_back(col);
                null_maps.push_back(nullptr);
            }
        }
    }

    /// Return the columns which actually contain the values of the keys.
    /// For a given key column, if it is nullable, we return its nested
    /// column. Otherwise we return the key column itself.
    inline const ColumnRawPtrs & getActualColumns() const { return actual_columns; }

    /// Create a bitmap that indicates whether, for a particular row,
    /// a key column bears a null value or not.
    KeysNullMap<Key> createBitmap(size_t row) const
    {
        KeysNullMap<Key> bitmap{};

        for (size_t k = 0; k < null_maps.size(); ++k)
        {
            if (null_maps[k] != nullptr)
            {
                const auto & null_map = assert_cast<const ColumnUInt8 &>(*null_maps[k]).getData();
                if (null_map[row] == 1)
                {
                    size_t bucket = k / 8;
                    size_t offset = k % 8;
                    bitmap[bucket] |= UInt8(1) << offset;
                }
            }
        }

        return bitmap;
    }

private:
    ColumnRawPtrs actual_columns;
    ColumnRawPtrs null_maps;
};

/// Case where nullable keys are not supported.
template <typename Key>
class BaseStateKeysFixed<Key, false>
{
protected:
    explicit BaseStateKeysFixed(const ColumnRawPtrs & columns)
        : actual_columns(columns)
    {}

    const ColumnRawPtrs & getActualColumns() const { return actual_columns; }

    KeysNullMap<Key> createBitmap(size_t) const
    {
        throw Exception{
            "Internal error: calling createBitmap() for non-nullable keys"
            " is forbidden",
            ErrorCodes::LOGICAL_ERROR};
    }

private:
    ColumnRawPtrs actual_columns;
};

} // namespace columns_hashing_impl

} // namespace ColumnsHashing

} // namespace DB
