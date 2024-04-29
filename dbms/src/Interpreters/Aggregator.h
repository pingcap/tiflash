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

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/Decimal.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <Common/HashTable/TwoLevelStringHashMap.h>
#include <Common/Logger.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/AggSpillContext.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/AggregationCommon.h>
#include <TiDB/Collation/Collator.h>
#include <common/StringRef.h>
#include <common/logger_useful.h>

#include <functional>
#include <memory>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

class IBlockOutputStream;
template <typename Method>
class AggHashTableToBlocksBlockInputStream;


/** Different data structures that can be used for aggregation
  * For efficiency, the aggregation data itself is put into the pool.
  * Data and pool ownership (states of aggregate functions)
  *  is acquired later - in `convertToBlocks` function, by the ColumnAggregateFunction object.
  *
  * Most data structures exist in two versions: normal and two-level (TwoLevel).
  * A two-level hash table works a little slower with a small number of different keys,
  *  but with a large number of different keys scales better, because it allows
  *  parallelize some operations (merging, post-processing) in a natural way.
  *
  * To ensure efficient work over a wide range of conditions,
  *  first single-level hash tables are used,
  *  and when the number of different keys is large enough,
  *  they are converted to two-level ones.
  *
  * PS. There are many different approaches to the effective implementation of parallel and distributed aggregation,
  *  best suited for different cases, and this approach is just one of them, chosen for a combination of reasons.
  */

using AggregatedDataWithoutKey = AggregateDataPtr;

using AggregatedDataWithUInt8Key = FixedImplicitZeroHashMapWithCalculatedSize<UInt8, AggregateDataPtr>;
using AggregatedDataWithUInt16Key = FixedImplicitZeroHashMap<UInt16, AggregateDataPtr>;

using AggregatedDataWithUInt32Key = HashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64Key = HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;

using AggregatedDataWithShortStringKey = StringHashMap<AggregateDataPtr>;
using AggregatedDataWithStringKey = HashMapWithSavedHash<StringRef, AggregateDataPtr>;

using AggregatedDataWithInt256Key = HashMap<Int256, AggregateDataPtr, HashCRC32<Int256>>;

using AggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, HashCRC32<UInt128>>;
using AggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, HashCRC32<UInt256>>;

using AggregatedDataWithUInt32KeyTwoLevel = TwoLevelHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using AggregatedDataWithUInt64KeyTwoLevel = TwoLevelHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;

using AggregatedDataWithInt256KeyTwoLevel = TwoLevelHashMap<Int256, AggregateDataPtr, HashCRC32<Int256>>;

using AggregatedDataWithShortStringKeyTwoLevel = TwoLevelStringHashMap<AggregateDataPtr>;
using AggregatedDataWithStringKeyTwoLevel = TwoLevelHashMapWithSavedHash<StringRef, AggregateDataPtr>;

using AggregatedDataWithKeys128TwoLevel = TwoLevelHashMap<UInt128, AggregateDataPtr, HashCRC32<UInt128>>;
using AggregatedDataWithKeys256TwoLevel = TwoLevelHashMap<UInt256, AggregateDataPtr, HashCRC32<UInt256>>;

/** Variants with better hash function, using more than 32 bits for hash.
  * Using for merging phase of external aggregation, where number of keys may be far greater than 4 billion,
  *  but we keep in memory and merge only sub-partition of them simultaneously.
  * TODO We need to switch for better hash function not only for external aggregation,
  *  but also for huge aggregation results on machines with terabytes of RAM.
  */

using AggregatedDataWithUInt64KeyHash64 = HashMap<UInt64, AggregateDataPtr, DefaultHash<UInt64>>;
using AggregatedDataWithStringKeyHash64 = HashMapWithSavedHash<StringRef, AggregateDataPtr, StringRefHash64>;
using AggregatedDataWithKeys128Hash64 = HashMap<UInt128, AggregateDataPtr, DefaultHash<UInt128>>;
using AggregatedDataWithKeys256Hash64 = HashMap<UInt256, AggregateDataPtr, DefaultHash<UInt256>>;

/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData, bool consecutive_keys_optimization = true>
struct AggregationMethodOneNumber
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodOneNumber() = default;

    template <typename Other>
    explicit AggregationMethodOneNumber(const Other & other)
        : data(other.data)
    {}

    /// To use one `Method` in different threads, use different `State`.
    using State = ColumnsHashing::
        HashMethodOneNumber<typename Data::value_type, Mapped, FieldType, consecutive_keys_optimization>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    // Insert the key from the hash table into columns.
    static void insertKeyIntoColumns(
        const Key & key,
        std::vector<IColumn *> & key_columns,
        const Sizes & /*key_sizes*/,
        const TiDB::TiDBCollators &)
    {
        const auto * key_holder = reinterpret_cast<const char *>(&key);
        auto * column = static_cast<ColumnVectorHelper *>(key_columns[0]);
        column->insertRawData<sizeof(FieldType)>(key_holder);
    }
};

/// For the case where there is one string key.
template <typename TData>
struct AggregationMethodString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodString() = default;

    template <typename Other>
    explicit AggregationMethodString(const Other & other)
        : data(other.data)
    {}

    using State = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        const Sizes &,
        const TiDB::TiDBCollators &)
    {
        static_cast<ColumnString *>(key_columns[0])->insertData(key.data, key.size);
    }
};

/// Same as above but without cache
template <typename TData>
struct AggregationMethodStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodStringNoCache() = default;

    template <typename Other>
    explicit AggregationMethodStringNoCache(const Other & other)
        : data(other.data)
    {}

    // Remove last zero byte.
    using State = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped, true, false>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        const Sizes &,
        const TiDB::TiDBCollators &)
    {
        // Add last zero byte.
        static_cast<ColumnString *>(key_columns[0])->insertData(key.data, key.size);
    }
};

template <bool bin_padding, typename TData>
struct AggregationMethodOneKeyStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodOneKeyStringNoCache() = default;

    template <typename Other>
    explicit AggregationMethodOneKeyStringNoCache(const Other & other)
        : data(other.data)
    {}

    using State = ColumnsHashing::HashMethodStringBin<typename Data::value_type, Mapped, bin_padding>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    ALWAYS_INLINE static inline void insertKeyIntoColumns(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        size_t)
    {
        /// still need to insert data to key because spill may will use this
        static_cast<ColumnString *>(key_columns[0])->insertData(key.data, key.size);
    }
    ALWAYS_INLINE static inline void initAggKeys(size_t, IColumn *) {}
};

/*
/// Same as above but without cache
template <typename TData>
struct AggregationMethodMultiStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodMultiStringNoCache() = default;

    template <typename Other>
    explicit AggregationMethodMultiStringNoCache(const Other & other)
        : data(other.data)
    {}

    using State = ColumnsHashing::HashMethodMultiString<typename Data::value_type, Mapped>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(const StringRef & key, std::vector<IColumn *> & key_columns, const Sizes &, const TiDB::TiDBCollators &)
    {
        const auto * pos = key.data;
        for (auto & key_column : key_columns)
            pos = static_cast<ColumnString *>(key_column)->deserializeAndInsertFromArena(pos, nullptr);
    }
};
*/

template <typename Key1Desc, typename Key2Desc, typename TData>
struct AggregationMethodFastPathTwoKeysNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodFastPathTwoKeysNoCache() = default;

    template <typename Other>
    explicit AggregationMethodFastPathTwoKeysNoCache(const Other & other)
        : data(other.data)
    {}

    using State
        = ColumnsHashing::HashMethodFastPathTwoKeysSerialized<Key1Desc, Key2Desc, typename Data::value_type, Mapped>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    template <typename KeyType>
    ALWAYS_INLINE static inline void initAggKeys(size_t rows, IColumn * key_column)
    {
        auto * column = static_cast<typename KeyType::ColumnType *>(key_column);
        column->getData().resize_fill(rows);
    }

    ALWAYS_INLINE static inline const char * insertAggKeyIntoColumnString(const char * pos, IColumn * key_column)
    {
        /// still need to insert data to key because spill may will use this
        const size_t string_size = *reinterpret_cast<const size_t *>(pos);
        pos += sizeof(string_size);
        static_cast<ColumnString *>(key_column)->insertData(pos, string_size);
        return pos + string_size;
    }
    ALWAYS_INLINE static inline void initAggKeyString(size_t, IColumn *) {}

    template <>
    ALWAYS_INLINE static inline void initAggKeys<ColumnsHashing::KeyDescStringBin>(size_t rows, IColumn * key_column)
    {
        return initAggKeyString(rows, key_column);
    }
    template <>
    ALWAYS_INLINE static inline void initAggKeys<ColumnsHashing::KeyDescStringBinPadding>(
        size_t rows,
        IColumn * key_column)
    {
        return initAggKeyString(rows, key_column);
    }

    template <typename KeyType>
    ALWAYS_INLINE static inline const char * insertAggKeyIntoColumn(
        const char * pos,
        IColumn * key_column,
        size_t index)
    {
        auto * column = static_cast<typename KeyType::ColumnType *>(key_column);
        column->getElement(index) = *reinterpret_cast<const typename KeyType::ColumnType::value_type *>(pos);
        return pos + KeyType::ElementSize;
    }
    template <>
    ALWAYS_INLINE static inline const char * insertAggKeyIntoColumn<ColumnsHashing::KeyDescStringBin>(
        const char * pos,
        IColumn * key_column,
        size_t)
    {
        return insertAggKeyIntoColumnString(pos, key_column);
    }
    template <>
    ALWAYS_INLINE static inline const char * insertAggKeyIntoColumn<ColumnsHashing::KeyDescStringBinPadding>(
        const char * pos,
        IColumn * key_column,
        size_t)
    {
        return insertAggKeyIntoColumnString(pos, key_column);
    }

    ALWAYS_INLINE static inline void insertKeyIntoColumnsOneKey(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        size_t index)
    {
        insertAggKeyIntoColumn<Key1Desc>(key.data, key_columns[0], index);
    }

    ALWAYS_INLINE static inline void insertKeyIntoColumnsTwoKey(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        size_t index)
    {
        const auto * pos = key.data;
        {
            pos = insertAggKeyIntoColumn<Key1Desc>(pos, key_columns[0], index);
        }
        {
            pos = insertAggKeyIntoColumn<Key2Desc>(pos, key_columns[1], index);
        }
    }
};


/// For the case where there is one fixed-length string key.
template <typename TData>
struct AggregationMethodFixedString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodFixedString() = default;

    template <typename Other>
    explicit AggregationMethodFixedString(const Other & other)
        : data(other.data)
    {}

    using State = ColumnsHashing::HashMethodFixedString<typename Data::value_type, Mapped>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        const Sizes &,
        const TiDB::TiDBCollators &)
    {
        static_cast<ColumnFixedString *>(key_columns[0])->insertData(key.data, key.size);
    }
};

/// Same as above but without cache
template <typename TData>
struct AggregationMethodFixedStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodFixedStringNoCache() = default;

    template <typename Other>
    explicit AggregationMethodFixedStringNoCache(const Other & other)
        : data(other.data)
    {}

    using State = ColumnsHashing::HashMethodFixedString<typename Data::value_type, Mapped, true, false>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        const Sizes &,
        const TiDB::TiDBCollators &)
    {
        static_cast<ColumnFixedString *>(key_columns[0])->insertData(key.data, key.size);
    }
};


/// For the case where all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename TData, bool has_nullable_keys_ = false, bool use_cache = true>
struct AggregationMethodKeysFixed
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    static constexpr bool has_nullable_keys = has_nullable_keys_;

    Data data;

    AggregationMethodKeysFixed() = default;

    template <typename Other>
    explicit AggregationMethodKeysFixed(const Other & other)
        : data(other.data)
    {}

    using State
        = ColumnsHashing::HashMethodKeysFixed<typename Data::value_type, Key, Mapped, has_nullable_keys, use_cache>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        return State::shuffleKeyColumns(key_columns, key_sizes);
    }

    static void insertKeyIntoColumns(
        const Key & key,
        std::vector<IColumn *> & key_columns,
        const Sizes & key_sizes,
        const TiDB::TiDBCollators &)
    {
        size_t keys_size = key_columns.size();

        static constexpr auto bitmap_size = has_nullable_keys ? std::tuple_size<KeysNullMap<Key>>::value : 0;
        /// In any hash key value, column values to be read start just after the bitmap, if it exists.
        size_t pos = bitmap_size;

        for (size_t i = 0; i < keys_size; ++i)
        {
            IColumn * observed_column;
            ColumnUInt8 * null_map;

            bool column_nullable = false;
            if constexpr (has_nullable_keys)
                column_nullable = key_columns[i]->isColumnNullable();

            /// If we have a nullable column, get its nested column and its null map.
            if (column_nullable)
            {
                auto & nullable_col = assert_cast<ColumnNullable &>(*key_columns[i]);
                observed_column = &nullable_col.getNestedColumn();
                null_map = assert_cast<ColumnUInt8 *>(&nullable_col.getNullMapColumn());
            }
            else
            {
                observed_column = key_columns[i];
                null_map = nullptr;
            }

            bool is_null = false;
            if (column_nullable)
            {
                /// The current column is nullable. Check if the value of the
                /// corresponding key is nullable. Update the null map accordingly.
                size_t bucket = i / 8;
                size_t offset = i % 8;
                UInt8 val = (reinterpret_cast<const UInt8 *>(&key)[bucket] >> offset) & 1;
                null_map->insert(val);
                is_null = val == 1;
            }

            if (has_nullable_keys && is_null)
                observed_column->insertDefault();
            else
            {
                size_t size = key_sizes[i];
                observed_column->insertData(reinterpret_cast<const char *>(&key) + pos, size);
                pos += size;
            }
        }
    }
};


/** Aggregates by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename TData>
struct AggregationMethodSerialized
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodSerialized() = default;

    template <typename Other>
    explicit AggregationMethodSerialized(const Other & other)
        : data(other.data)
    {}

    using State = ColumnsHashing::HashMethodSerialized<typename Data::value_type, Mapped>;
    using EmplaceResult = ColumnsHashing::columns_hashing_impl::EmplaceResultImpl<Mapped>;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        const Sizes &,
        const TiDB::TiDBCollators & collators)
    {
        const auto * pos = key.data;
        for (size_t i = 0; i < key_columns.size(); ++i)
            pos = key_columns[i]->deserializeAndInsertFromArena(pos, collators.empty() ? nullptr : collators[i]);
    }
};


class Aggregator;

#define AggregationMethodName(NAME) AggregatedDataVariants::AggregationMethod_##NAME

struct AggregatedDataVariants : private boost::noncopyable
{
    /** Working with states of aggregate functions in the pool is arranged in the following (inconvenient) way:
      * - when aggregating, states are created in the pool using IAggregateFunction::create (inside - `placement new` of arbitrary structure);
      * - they must then be destroyed using IAggregateFunction::destroy (inside - calling the destructor of arbitrary structure);
      * - if aggregation is complete, then, in the Aggregator::convertToBlocks function, pointers to the states of aggregate functions
      *   are written to ColumnAggregateFunction; ColumnAggregateFunction "acquires ownership" of them, that is - calls `destroy` in its destructor.
      * - if during the aggregation, before call to Aggregator::convertToBlocks, an exception was thrown,
      *   then the states of aggregate functions must still be destroyed,
      *   otherwise, for complex states (eg, AggregateFunctionUniq), there will be memory leaks;
      * - in this case, to destroy states, the destructor calls Aggregator::destroyAggregateStates method,
      *   but only if the variable aggregator (see below) is not nullptr;
      * - that is, until you transfer ownership of the aggregate function states in the ColumnAggregateFunction, set the variable `aggregator`,
      *   so that when an exception occurs, the states are correctly destroyed.
      *
      * PS. This can be corrected by making a pool that knows about which states of aggregate functions and in which order are put in it, and knows how to destroy them.
      * But this can hardly be done simply because it is planned to put variable-length strings into the same pool.
      * In this case, the pool will not be able to know with what offsets objects are stored.
      */
    Aggregator * aggregator = nullptr;

    size_t keys_size{}; /// Number of keys. NOTE do we need this field?
    Sizes key_sizes; /// Dimensions of keys, if keys of fixed length

    /// Pools for states of aggregate functions. Ownership will be later transferred to ColumnAggregateFunction.
    Arenas aggregates_pools;
    Arena * aggregates_pool{}; /// The pool that is currently used for allocation.

    void * aggregation_method_impl{};

    /** Specialization for the case when there are no keys.
      */
    AggregatedDataWithoutKey without_key = nullptr;

    using AggregationMethod_key8 = AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key, false>;
    using AggregationMethod_key16 = AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key, false>;
    using AggregationMethod_key32 = AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64Key>;
    using AggregationMethod_key64 = AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>;
    using AggregationMethod_key_int256 = AggregationMethodOneNumber<Int256, AggregatedDataWithInt256Key>;
    using AggregationMethod_key_string = AggregationMethodStringNoCache<AggregatedDataWithShortStringKey>;
    using AggregationMethod_one_key_strbin
        = AggregationMethodOneKeyStringNoCache<false, AggregatedDataWithShortStringKey>;
    using AggregationMethod_one_key_strbinpadding
        = AggregationMethodOneKeyStringNoCache<true, AggregatedDataWithShortStringKey>;
    using AggregationMethod_key_fixed_string = AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKey>;
    using AggregationMethod_keys16 = AggregationMethodKeysFixed<AggregatedDataWithUInt16Key, false, false>;
    using AggregationMethod_keys32 = AggregationMethodKeysFixed<AggregatedDataWithUInt32Key>;
    using AggregationMethod_keys64 = AggregationMethodKeysFixed<AggregatedDataWithUInt64Key>;
    using AggregationMethod_keys128 = AggregationMethodKeysFixed<AggregatedDataWithKeys128>;
    using AggregationMethod_keys256 = AggregationMethodKeysFixed<AggregatedDataWithKeys256>;
    using AggregationMethod_serialized = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggregationMethod_key32_two_level = AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt64KeyTwoLevel>;
    using AggregationMethod_key64_two_level = AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyTwoLevel>;
    using AggregationMethod_key_int256_two_level
        = AggregationMethodOneNumber<Int256, AggregatedDataWithInt256KeyTwoLevel>;
    using AggregationMethod_key_string_two_level
        = AggregationMethodStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>;
    using AggregationMethod_one_key_strbin_two_level
        = AggregationMethodOneKeyStringNoCache<false, AggregatedDataWithShortStringKeyTwoLevel>;
    using AggregationMethod_one_key_strbinpadding_two_level
        = AggregationMethodOneKeyStringNoCache<true, AggregatedDataWithShortStringKeyTwoLevel>;
    using AggregationMethod_key_fixed_string_two_level
        = AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>;
    using AggregationMethod_keys32_two_level = AggregationMethodKeysFixed<AggregatedDataWithUInt32KeyTwoLevel>;
    using AggregationMethod_keys64_two_level = AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyTwoLevel>;
    using AggregationMethod_keys128_two_level = AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel>;
    using AggregationMethod_keys256_two_level = AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel>;
    using AggregationMethod_serialized_two_level = AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel>;
    using AggregationMethod_key64_hash64 = AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyHash64>;
    using AggregationMethod_key_string_hash64 = AggregationMethodStringNoCache<AggregatedDataWithStringKeyHash64>;
    using AggregationMethod_key_fixed_string_hash64 = AggregationMethodFixedString<AggregatedDataWithStringKeyHash64>;
    using AggregationMethod_keys128_hash64 = AggregationMethodKeysFixed<AggregatedDataWithKeys128Hash64>;
    using AggregationMethod_keys256_hash64 = AggregationMethodKeysFixed<AggregatedDataWithKeys256Hash64>;
    using AggregationMethod_serialized_hash64 = AggregationMethodSerialized<AggregatedDataWithStringKeyHash64>;

    /// Support for nullable keys.
    using AggregationMethod_nullable_keys128 = AggregationMethodKeysFixed<AggregatedDataWithKeys128, true>;
    using AggregationMethod_nullable_keys256 = AggregationMethodKeysFixed<AggregatedDataWithKeys256, true>;
    using AggregationMethod_nullable_keys128_two_level
        = AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel, true>;
    using AggregationMethod_nullable_keys256_two_level
        = AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel, true>;

    // 2 keys
    using AggregationMethod_two_keys_num64_strbin = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescNumber64,
        ColumnsHashing::KeyDescStringBin,
        AggregatedDataWithStringKey>;
    using AggregationMethod_two_keys_num64_strbinpadding = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescNumber64,
        ColumnsHashing::KeyDescStringBinPadding,
        AggregatedDataWithStringKey>;
    using AggregationMethod_two_keys_strbin_num64 = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescStringBin,
        ColumnsHashing::KeyDescNumber64,
        AggregatedDataWithStringKey>;
    using AggregationMethod_two_keys_strbin_strbin = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescStringBin,
        ColumnsHashing::KeyDescStringBin,
        AggregatedDataWithStringKey>;
    using AggregationMethod_two_keys_strbinpadding_num64 = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescStringBinPadding,
        ColumnsHashing::KeyDescNumber64,
        AggregatedDataWithStringKey>;
    using AggregationMethod_two_keys_strbinpadding_strbinpadding = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescStringBinPadding,
        ColumnsHashing::KeyDescStringBinPadding,
        AggregatedDataWithStringKey>;

    using AggregationMethod_two_keys_num64_strbin_two_level = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescNumber64,
        ColumnsHashing::KeyDescStringBin,
        AggregatedDataWithStringKeyTwoLevel>;
    using AggregationMethod_two_keys_num64_strbinpadding_two_level = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescNumber64,
        ColumnsHashing::KeyDescStringBinPadding,
        AggregatedDataWithStringKeyTwoLevel>;
    using AggregationMethod_two_keys_strbin_num64_two_level = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescStringBin,
        ColumnsHashing::KeyDescNumber64,
        AggregatedDataWithStringKeyTwoLevel>;
    using AggregationMethod_two_keys_strbin_strbin_two_level = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescStringBin,
        ColumnsHashing::KeyDescStringBin,
        AggregatedDataWithStringKeyTwoLevel>;
    using AggregationMethod_two_keys_strbinpadding_num64_two_level = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescStringBinPadding,
        ColumnsHashing::KeyDescNumber64,
        AggregatedDataWithStringKeyTwoLevel>;
    using AggregationMethod_two_keys_strbinpadding_strbinpadding_two_level = AggregationMethodFastPathTwoKeysNoCache<
        ColumnsHashing::KeyDescStringBinPadding,
        ColumnsHashing::KeyDescStringBinPadding,
        AggregatedDataWithStringKeyTwoLevel>;

    // 3 keys
    // TODO: use 3 keys if necessary

/// In this and similar macros, the option without_key is not considered.
#define APPLY_FOR_AGGREGATED_VARIANTS(M)                    \
    M(key8, false)                                          \
    M(key16, false)                                         \
    M(key32, false)                                         \
    M(key64, false)                                         \
    M(key_string, false)                                    \
    M(key_fixed_string, false)                              \
    M(keys16, false)                                        \
    M(keys32, false)                                        \
    M(keys64, false)                                        \
    M(keys128, false)                                       \
    M(keys256, false)                                       \
    M(key_int256, false)                                    \
    M(serialized, false)                                    \
    M(key64_hash64, false)                                  \
    M(key_string_hash64, false)                             \
    M(key_fixed_string_hash64, false)                       \
    M(keys128_hash64, false)                                \
    M(keys256_hash64, false)                                \
    M(serialized_hash64, false)                             \
    M(nullable_keys128, false)                              \
    M(nullable_keys256, false)                              \
    M(two_keys_num64_strbin, false)                         \
    M(two_keys_num64_strbinpadding, false)                  \
    M(two_keys_strbin_num64, false)                         \
    M(two_keys_strbin_strbin, false)                        \
    M(two_keys_strbinpadding_num64, false)                  \
    M(two_keys_strbinpadding_strbinpadding, false)          \
    M(one_key_strbin, false)                                \
    M(one_key_strbinpadding, false)                         \
    M(key32_two_level, true)                                \
    M(key64_two_level, true)                                \
    M(key_int256_two_level, true)                           \
    M(key_string_two_level, true)                           \
    M(key_fixed_string_two_level, true)                     \
    M(keys32_two_level, true)                               \
    M(keys64_two_level, true)                               \
    M(keys128_two_level, true)                              \
    M(keys256_two_level, true)                              \
    M(serialized_two_level, true)                           \
    M(nullable_keys128_two_level, true)                     \
    M(nullable_keys256_two_level, true)                     \
    M(two_keys_num64_strbin_two_level, true)                \
    M(two_keys_num64_strbinpadding_two_level, true)         \
    M(two_keys_strbin_num64_two_level, true)                \
    M(two_keys_strbin_strbin_two_level, true)               \
    M(two_keys_strbinpadding_num64_two_level, true)         \
    M(two_keys_strbinpadding_strbinpadding_two_level, true) \
    M(one_key_strbin_two_level, true)                       \
    M(one_key_strbinpadding_two_level, true)

    enum class Type
    {
        EMPTY = 0,
        without_key,

#define M(NAME, IS_TWO_LEVEL) NAME,
        APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    };

    Type type{Type::EMPTY};

    bool need_spill = false;

    bool tryMarkNeedSpill();

    void destroyAggregationMethodImpl();

    AggregatedDataVariants()
        : aggregates_pools(1, std::make_shared<Arena>())
        , aggregates_pool(aggregates_pools.back().get())
    {}
    bool inited() const { return type != Type::EMPTY; }
    bool empty() const { return size() == 0; }
    void invalidate() { type = Type::EMPTY; }

    ~AggregatedDataVariants();

    void init(Type variants_type);

    /// Number of rows (different keys).
    size_t size() const
    {
        switch (type)
        {
        case Type::EMPTY:
            return 0;
        case Type::without_key:
            return 1;

#define M(NAME, IS_TWO_LEVEL)                                                                              \
    case Type::NAME:                                                                                       \
    {                                                                                                      \
        const auto * ptr = reinterpret_cast<const AggregationMethodName(NAME) *>(aggregation_method_impl); \
        return ptr->data.size();                                                                           \
    }

            APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
    }

    size_t revocableBytes() const
    {
        if (empty())
            return 0;
        return bytesCount();
    }

    size_t bytesCount() const
    {
        size_t bytes_count = 0;
        switch (type)
        {
        case Type::EMPTY:
        case Type::without_key:
            break;

#define M(NAME, IS_TWO_LEVEL)                                                                              \
    case Type::NAME:                                                                                       \
    {                                                                                                      \
        const auto * ptr = reinterpret_cast<const AggregationMethodName(NAME) *>(aggregation_method_impl); \
        bytes_count = ptr->data.getBufferSizeInBytes();                                                    \
        break;                                                                                             \
    }

            APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
        for (const auto & pool : aggregates_pools)
            bytes_count += pool->size();
        return bytes_count;
    }

    const char * getMethodName() const { return getMethodName(type); }
    static const char * getMethodName(Type type)
    {
        switch (type)
        {
        case Type::EMPTY:
            return "EMPTY";
        case Type::without_key:
            return "without_key";

#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME:          \
        return #NAME;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
    }

    bool isTwoLevel() const
    {
        switch (type)
        {
        case Type::EMPTY:
            return false;
        case Type::without_key:
            return false;

#define M(NAME, IS_TWO_LEVEL) \
    case Type::NAME:          \
        return IS_TWO_LEVEL;
            APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
    }

#define APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M) \
    M(key32)                                           \
    M(key64)                                           \
    M(key_int256)                                      \
    M(key_string)                                      \
    M(key_fixed_string)                                \
    M(keys32)                                          \
    M(keys64)                                          \
    M(keys128)                                         \
    M(keys256)                                         \
    M(serialized)                                      \
    M(nullable_keys128)                                \
    M(nullable_keys256)                                \
    M(two_keys_num64_strbin)                           \
    M(two_keys_num64_strbinpadding)                    \
    M(two_keys_strbin_num64)                           \
    M(two_keys_strbin_strbin)                          \
    M(two_keys_strbinpadding_num64)                    \
    M(two_keys_strbinpadding_strbinpadding)            \
    M(one_key_strbin)                                  \
    M(one_key_strbinpadding)


#define APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M) \
    M(key8)                                                \
    M(key16)                                               \
    M(keys16)                                              \
    M(key64_hash64)                                        \
    M(key_fixed_string_hash64)                             \
    M(keys128_hash64)                                      \
    M(keys256_hash64)                                      \
    M(key_string_hash64)                                   \
    M(serialized_hash64)

#define APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)             \
    APPLY_FOR_VARIANTS_NOT_CONVERTIBLE_TO_TWO_LEVEL(M) \
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    bool isConvertibleToTwoLevel() const { return isConvertibleToTwoLevel(type); }

    static size_t getBucketNumberForTwoLevelHashTable(Type type);

    static bool isConvertibleToTwoLevel(Type type)
    {
        switch (type)
        {
#define M(NAME)      \
    case Type::NAME: \
        return true;

            APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

#undef M
        default:
            return false;
        }
    }

    void convertToTwoLevel();

    void setResizeCallbackIfNeeded(size_t thread_num) const;

#define APPLY_FOR_VARIANTS_TWO_LEVEL(M)               \
    M(key32_two_level)                                \
    M(key64_two_level)                                \
    M(key_int256_two_level)                           \
    M(key_string_two_level)                           \
    M(key_fixed_string_two_level)                     \
    M(keys32_two_level)                               \
    M(keys64_two_level)                               \
    M(keys128_two_level)                              \
    M(keys256_two_level)                              \
    M(serialized_two_level)                           \
    M(nullable_keys128_two_level)                     \
    M(nullable_keys256_two_level)                     \
    M(two_keys_num64_strbin_two_level)                \
    M(two_keys_num64_strbinpadding_two_level)         \
    M(two_keys_strbin_num64_two_level)                \
    M(two_keys_strbin_strbin_two_level)               \
    M(two_keys_strbinpadding_num64_two_level)         \
    M(two_keys_strbinpadding_strbinpadding_two_level) \
    M(one_key_strbin_two_level)                       \
    M(one_key_strbinpadding_two_level)
};

using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;
using ManyAggregatedDataVariants = std::vector<AggregatedDataVariantsPtr>;

/// Combines aggregation states together, turns them into blocks, and outputs.
class MergingBuckets
{
public:
    /** The input is a set of non-empty sets of partially aggregated data,
      *  which are all either single-level, or are two-level.
      */
    MergingBuckets(
        const Aggregator & aggregator_,
        const ManyAggregatedDataVariants & data_,
        bool final_,
        size_t concurrency_);

    Block getHeader() const;

    Block getData(size_t concurrency_index, bool enable_convert_key_optimization);

    size_t getConcurrency() const { return concurrency; }

private:
    Block getDataForSingleLevel(bool enable_convert_key_optimization);

    Block getDataForTwoLevel(size_t concurrency_index, bool enable_convert_key_optimization);

    void doLevelMerge(Int32 bucket_num, size_t concurrency_index, bool enable_convert_key_optimization);

private:
    const LoggerPtr log;
    const Aggregator & aggregator;
    ManyAggregatedDataVariants data;
    bool final;
    size_t concurrency;

    bool is_two_level = false;

    BlocksList single_level_blocks;

    // use unique_ptr to avoid false sharing.
    std::vector<std::unique_ptr<BlocksList>> two_level_parallel_merge_data;

    std::atomic<Int32> current_bucket_num = 0;
    static constexpr Int32 NUM_BUCKETS = 256;
};
using MergingBucketsPtr = std::shared_ptr<MergingBuckets>;

/** Aggregates the source of the blocks.
  */
class Aggregator
{
public:
    struct Params
    {
        /// Data structure of source blocks.
        Block src_header;
        /// Data structure of intermediate blocks before merge.
        Block intermediate_header;

        /// What to count.
        ColumnNumbers keys;
        std::unordered_map<String, String> key_from_agg_func;
        AggregateDescriptions aggregates;
        size_t keys_size;
        size_t aggregates_size;

        /// Return empty result when aggregating without keys on empty set.
        bool empty_result_for_aggregation_by_empty_set;

        SpillConfig spill_config;

        UInt64 max_block_size;
        TiDB::TiDBCollators collators;

        Params(
            const Block & src_header_,
            const ColumnNumbers & keys_,
            const std::unordered_map<String, String> & key_from_agg_func_,
            const AggregateDescriptions & aggregates_,
            size_t group_by_two_level_threshold_,
            size_t group_by_two_level_threshold_bytes_,
            size_t max_bytes_before_external_group_by_,
            bool empty_result_for_aggregation_by_empty_set_,
            const SpillConfig & spill_config_,
            UInt64 max_block_size_,
            const TiDB::TiDBCollators & collators_ = TiDB::dummy_collators)
            : src_header(src_header_)
            , keys(keys_)
            , key_from_agg_func(key_from_agg_func_)
            , aggregates(aggregates_)
            , keys_size(keys.size())
            , aggregates_size(aggregates.size())
            , empty_result_for_aggregation_by_empty_set(empty_result_for_aggregation_by_empty_set_)
            , spill_config(spill_config_)
            , max_block_size(max_block_size_)
            , collators(collators_)
            , group_by_two_level_threshold(group_by_two_level_threshold_)
            , group_by_two_level_threshold_bytes(group_by_two_level_threshold_bytes_)
            , max_bytes_before_external_group_by(max_bytes_before_external_group_by_)
        {}

        /// Only parameters that matter during merge.
        Params(
            const Block & intermediate_header_,
            const ColumnNumbers & keys_,
            const std::unordered_map<String, String> & key_from_agg_func_,
            const AggregateDescriptions & aggregates_,
            const SpillConfig & spill_config,
            UInt64 max_block_size_,
            const TiDB::TiDBCollators & collators_ = TiDB::dummy_collators)
            : Params(
                Block(),
                keys_,
                key_from_agg_func_,
                aggregates_,
                0,
                0,
                0,
                false,
                spill_config,
                max_block_size_,
                collators_)
        {
            intermediate_header = intermediate_header_;
        }

        static Block getHeader(
            const Block & src_header,
            const Block & intermediate_header,
            const ColumnNumbers & keys,
            const AggregateDescriptions & aggregates,
            bool final);

        Block getHeader(bool final) const
        {
            return getHeader(src_header, intermediate_header, keys, aggregates, final);
        }

        /// Calculate the column numbers in `keys` and `aggregates`.
        void calculateColumnNumbers(const Block & block);

        size_t getGroupByTwoLevelThreshold() const { return group_by_two_level_threshold; }
        size_t getGroupByTwoLevelThresholdBytes() const { return group_by_two_level_threshold_bytes; }
        size_t getMaxBytesBeforeExternalGroupBy() const { return max_bytes_before_external_group_by; }
        void setMaxBytesBeforeExternalGroupBy(size_t threshold) { max_bytes_before_external_group_by = threshold; }

    private:
        /// Note these thresholds should not be used directly, they are only used to
        /// init the threshold in Aggregator
        const size_t group_by_two_level_threshold;
        const size_t group_by_two_level_threshold_bytes;
        size_t max_bytes_before_external_group_by; /// 0 - do not use external aggregation.
    };


    Aggregator(
        const Params & params_,
        const String & req_id,
        size_t concurrency,
        const RegisterOperatorSpillContext & register_operator_spill_context);

    /// Aggregate the source. Get the result in the form of one of the data structures.
    void execute(const BlockInputStreamPtr & stream, AggregatedDataVariants & result, size_t thread_num);

    bool isCancelled() { return is_cancelled(); }

    using AggregateColumns = std::vector<ColumnRawPtrs>;
    using AggregateColumnsData = std::vector<ColumnAggregateFunction::Container *>;
    using AggregateColumnsConstData = std::vector<const ColumnAggregateFunction::Container *>;
    using AggregateFunctionsPlainPtrs = std::vector<IAggregateFunction *>;

    /** This array serves two purposes.
      *
      * Function arguments are collected side by side, and they do not need to be collected from different places. Also the array is made zero-terminated.
      * The inner loop (for the case without_key) is almost twice as compact; performance gain of about 30%.
      */
    struct AggregateFunctionInstruction
    {
        const IAggregateFunction * that{};
        IAggregateFunction::AddFunc func{};
        size_t state_offset{};
        const IColumn ** arguments{};
        const IAggregateFunction * batch_that{};
        const IColumn ** batch_arguments{};
    };

    using AggregateFunctionInstructions = std::vector<AggregateFunctionInstruction>;
    struct AggProcessInfo
    {
        AggProcessInfo(Aggregator * aggregator_)
            : aggregator(aggregator_)
        {
            assert(aggregator);
        }
        Block block;
        size_t start_row = 0;
        size_t end_row = 0;
        bool prepare_for_agg_done = false;
        Columns materialized_columns;
        Columns input_columns;
        ColumnRawPtrs key_columns;
        AggregateColumns aggregate_columns;
        AggregateFunctionInstructions aggregate_functions_instructions;
        Aggregator * aggregator;
        void prepareForAgg();
        bool allBlockDataHandled() const
        {
            assert(start_row <= end_row);
            return start_row == end_row || aggregator->isCancelled();
        }
        void resetBlock(const Block & block_)
        {
            RUNTIME_CHECK_MSG(allBlockDataHandled(), "Previous block is not processed yet");
            block = block_;
            start_row = 0;
            end_row = 0;
            materialized_columns.clear();
            prepare_for_agg_done = false;
        }
    };

    /// Process one block. Return false if the processing should be aborted.
    bool executeOnBlock(AggProcessInfo & agg_process_info, AggregatedDataVariants & result, size_t thread_num);

    /** Merge several aggregation data structures and output the MergingBucketsPtr used to merge.
      * Return nullptr if there are no non empty data_variant.
      */
    MergingBucketsPtr mergeAndConvertToBlocks(
        ManyAggregatedDataVariants & data_variants,
        bool final,
        size_t max_threads) const;

    /// Merge several partially aggregated blocks into one.
    BlocksList vstackBlocks(BlocksList & blocks, bool final);

    bool isConvertibleToTwoLevel() { return AggregatedDataVariants::isConvertibleToTwoLevel(method_chosen); }
    /** Split block with partially-aggregated data to many blocks, as if two-level method of aggregation was used.
      * This is needed to simplify merging of that data with other results, that are already two-level.
      */
    Blocks convertBlockToTwoLevel(const Block & block);

    using CancellationHook = std::function<bool()>;

    /** Set a function that checks whether the current task can be aborted.
      */
    void setCancellationHook(CancellationHook cancellation_hook);

    /// For external aggregation.
    void spill(AggregatedDataVariants & data_variants, size_t thread_num);
    void finishSpill();
    BlockInputStreams restoreSpilledData();
    bool hasSpilledData() const { return agg_spill_context->hasSpilledData(); }
    void useTwoLevelHashTable() { use_two_level_hash_table = true; }
    void initThresholdByAggregatedDataVariantsSize(size_t aggregated_data_variants_size);
    AggSpillContextPtr & getAggSpillContext() { return agg_spill_context; }

    /// Get data structure of the result.
    Block getHeader(bool final) const;
    Block getSourceHeader() const;

protected:
    friend struct AggregatedDataVariants;
    friend class MergingBuckets;

    Params params;

    AggregatedDataVariants::Type method_chosen;


    Sizes key_sizes;

    AggregateFunctionsPlainPtrs aggregate_functions;

    Sizes offsets_of_aggregate_states; /// The offset to the n-th aggregate function in a row of aggregate functions.
    size_t total_size_of_aggregate_states = 0; /// The total size of the row from the aggregate functions.

    // add info to track alignment requirement
    // If there are states whose alignment are v1, ..vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;

    bool all_aggregates_has_trivial_destructor = false;

    std::atomic<bool> use_two_level_hash_table = false;

    const LoggerPtr log;

    /// Returns true if you can abort the current task.
    CancellationHook is_cancelled;

    /// Two-level aggregation settings (used for a large number of keys).
    /** With how many keys or the size of the aggregation state in bytes,
          *  two-level aggregation begins to be used. Enough to reach of at least one of the thresholds.
          * 0 - the corresponding threshold is not specified.
          */
    size_t group_by_two_level_threshold = 0;
    size_t group_by_two_level_threshold_bytes = 0;

    /// For external aggregation.
    AggSpillContextPtr agg_spill_context;
    std::atomic<bool> spill_triggered{false};

    /** Select the aggregation method based on the number and types of keys. */
    AggregatedDataVariants::Type chooseAggregationMethod();

    /** Create states of aggregate functions for one key.
      */
    void createAggregateStates(AggregateDataPtr & aggregate_data) const;

    /** Call `destroy` methods for states of aggregate functions.
      * Used in the exception handler for aggregation, since RAII in this case is not applicable.
      */
    void destroyAllAggregateStates(AggregatedDataVariants & result);


    /// Process one data block, aggregate the data into a hash table.
    template <typename Method>
    void executeImpl(
        Method & method,
        Arena * aggregates_pool,
        AggProcessInfo & agg_process_info,
        TiDB::TiDBCollators & collators) const;

    template <typename Method>
    void executeImplBatch(
        Method & method,
        typename Method::State & state,
        Arena * aggregates_pool,
        AggProcessInfo & agg_process_info) const;

    template <typename Method>
    std::optional<typename Method::EmplaceResult> emplaceKey(
        Method & method,
        typename Method::State & state,
        size_t index,
        Arena & aggregates_pool,
        std::vector<std::string> & sort_key_containers) const;

    /// For case when there are no keys (all aggregate into one row).
    static void executeWithoutKeyImpl(AggregatedDataWithoutKey & res, AggProcessInfo & agg_process_info, Arena * arena);

    template <typename Method>
    void spillImpl(AggregatedDataVariants & data_variants, Method & method, size_t thread_num);

protected:
    /// Merge data from hash table `src` into `dst`.
    template <typename Method, typename Table>
    void mergeDataImpl(Table & table_dst, Table & table_src, Arena * arena) const;

    void mergeWithoutKeyDataImpl(ManyAggregatedDataVariants & non_empty_data) const;

    template <typename Method>
    void mergeSingleLevelDataImpl(ManyAggregatedDataVariants & non_empty_data) const;

    // enable_convert_key_optimization will only be true when output.
    // It will be false when spilling to disk.
    // Because need to make sure the block inserting into HashMap is same as the child output block.
    template <typename Method, typename Table, bool skip_serialize_key>
    void convertToBlockImpl(
        Method & method,
        Table & data,
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        Arena * arena,
        bool final) const;

    // The template parameter skip_serialize_key indicates whether the key in the HashMap can be skipped to serialize.
    // For example, select first_row(c1) from t group by c1, only the result of first_row(c1) needs to be serialized.
    // The key c1 does not need to be serialized into Column. It only needs to reference to the result column of first_row(c1).
    template <typename Method, typename Table, bool skip_serialize_key>
    void convertToBlocksImpl(
        Method & method,
        Table & data,
        std::vector<MutableColumns> & key_columns_vec,
        std::vector<AggregateColumnsData> & aggregate_columns_vec,
        std::vector<MutableColumns> & final_aggregate_columns_vec,
        Arena * arena,
        bool final) const;

    template <typename Method, typename Table, bool skip_serialize_key>
    void convertToBlockImplFinal(
        Method & method,
        Table & data,
        std::vector<IColumn *> key_columns,
        MutableColumns & final_aggregate_columns,
        Arena * arena) const;

    template <typename Method, typename Table, bool skip_serialize_key>
    void convertToBlocksImplFinal(
        Method & method,
        Table & data,
        std::vector<std::vector<IColumn *>> && key_columns_vec,
        std::vector<MutableColumns> & final_aggregate_columns_vec,
        Arena * arena) const;

    template <typename Method, typename Table, bool skip_serialize_key>
    void convertToBlockImplNotFinal(
        Method & method,
        Table & data,
        std::vector<IColumn *> key_columns,
        AggregateColumnsData & aggregate_columns) const;

    template <typename Method, typename Table, bool skip_serialize_key>
    void convertToBlocksImplNotFinal(
        Method & method,
        Table & data,
        std::vector<std::vector<IColumn *>> && key_columns_vec,
        std::vector<AggregateColumnsData> & aggregate_columns_vec) const;

    template <typename Filler>
    Block prepareBlockAndFill(
            AggregatedDataVariants & data_variants,
            bool final,
            size_t rows,
            Filler && filler,
            size_t serialize_key_size,
            const std::unordered_map<String, String> & key_ref_agg_func) const;

    template <typename Filler>
    BlocksList prepareBlocksAndFill(
            AggregatedDataVariants & data_variants,
            bool final,
            size_t rows,
            Filler && filler,
            size_t serialize_key_size,
            const std::unordered_map<String, String> & key_ref_agg_func) const;

    template <typename Method>
    Block convertOneBucketToBlock(
        AggregatedDataVariants & data_variants,
        Method & method,
        Arena * arena,
        bool final,
        size_t bucket,
        bool enable_convert_key_optimization) const;

    template <typename Method>
    BlocksList convertOneBucketToBlocks(
        AggregatedDataVariants & data_variants,
        Method & method,
        Arena * arena,
        bool final,
        size_t bucket,
        bool enable_convert_key_optimization) const;

    template <typename Mapped>
    void insertAggregatesIntoColumns(Mapped & mapped, MutableColumns & final_aggregate_columns, Arena * arena) const;

    void prepareAggregateInstructions(
        Columns columns,
        AggregateColumns & aggregate_columns,
        Columns & materialized_columns,
        AggregateFunctionInstructions & instructions);

    BlocksList prepareBlocksAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final) const;
    BlocksList prepareBlocksAndFillSingleLevel(
        AggregatedDataVariants & data_variants,
        bool final,
        bool enable_convert_key_optimizatioenable_convert_key_optimization) const;

    template <typename Method, typename Table>
    void mergeStreamsImplCase(Block & block, Arena * aggregates_pool, Method & method, Table & data) const;

    template <typename Method, typename Table>
    void mergeStreamsImpl(Block & block, Arena * aggregates_pool, Method & method, Table & data) const;

    void mergeWithoutKeyStreamsImpl(Block & block, AggregatedDataVariants & result) const;

    template <typename Method>
    void mergeBucketImpl(ManyAggregatedDataVariants & data, Int32 bucket, Arena * arena) const;

    template <typename Method>
    void convertBlockToTwoLevelImpl(
        Method & method,
        Arena * pool,
        ColumnRawPtrs & key_columns,
        const Block & source,
        Blocks & destinations) const;

    template <typename Method, typename Table>
    void destroyImpl(Table & table) const;

    void destroyWithoutKey(AggregatedDataVariants & result) const;

    template <typename Method>
    friend class AggHashTableToBlocksBlockInputStream;
};

/** Get the aggregation variant by its type. */
template <typename Method>
Method & getDataVariant(AggregatedDataVariants & variants);

#define M(NAME, IS_TWO_LEVEL)                                                                      \
    template <>                                                                                    \
        inline AggregationMethodName(NAME) & /*NOLINT*/                                            \
        getDataVariant<AggregationMethodName(NAME)>(AggregatedDataVariants & variants)             \
    {                                                                                              \
        return *reinterpret_cast<AggregationMethodName(NAME) *>(variants.aggregation_method_impl); \
    }

APPLY_FOR_AGGREGATED_VARIANTS(M)

#undef M
#undef AggregationMethodName

} // namespace DB
