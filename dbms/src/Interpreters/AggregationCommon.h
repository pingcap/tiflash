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
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>

#include <array>

namespace DB
{
using Sizes = std::vector<size_t>;

/// When packing the values of nullable columns at a given row, we have to
/// store the fact that these values are nullable or not. This is achieved
/// by encoding this information as a bitmap. Let S be the size in bytes of
/// a packed values binary blob and T the number of bytes we may place into
/// this blob, the size that the bitmap shall occupy in the blob is equal to:
/// ceil(T/8). Thus we must have: S = T + ceil(T/8). Below we indicate for
/// each value of S, the corresponding value of T, and the bitmap size:
///
/// 32,28,4
/// 16,14,2
/// 8,7,1
/// 4,3,1
/// 2,1,1
///

namespace
{
template <typename T>
constexpr auto getBitmapSize()
{
    return (sizeof(T) == 32) ? 4
        : (sizeof(T) == 16)  ? 2
                             : ((sizeof(T) == 8) ? 1 : ((sizeof(T) == 4) ? 1 : ((sizeof(T) == 2) ? 1 : 0)));
}

} // namespace

template <typename T>
using KeysNullMap = std::array<UInt8, getBitmapSize<T>()>;

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all the keys fit into the
/// binary blob, they are disposed in it consecutively.
template <typename T>
static inline T ALWAYS_INLINE
packFixed(size_t i, size_t keys_size, const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    union
    {
        T key;
        char bytes[sizeof(key)] = {};
    };

    size_t offset = 0;

    for (size_t j = 0; j < keys_size; ++j)
    {
        switch (key_sizes[j])
        {
        case 1:
            memcpy(bytes + offset, &static_cast<const ColumnUInt8 *>(key_columns[j])->getData()[i], 1);
            offset += 1;
            break;
        case 2:
            memcpy(bytes + offset, &static_cast<const ColumnUInt16 *>(key_columns[j])->getData()[i], 2);
            offset += 2;
            break;
        case 4:
            memcpy(bytes + offset, &static_cast<const ColumnUInt32 *>(key_columns[j])->getData()[i], 4);
            offset += 4;
            break;
        case 8:
            memcpy(bytes + offset, &static_cast<const ColumnUInt64 *>(key_columns[j])->getData()[i], 8);
            offset += 8;
            break;
        default:
            memcpy(
                bytes + offset,
                &static_cast<const ColumnFixedString *>(key_columns[j])->getChars()[i * key_sizes[j]],
                key_sizes[j]);
            offset += key_sizes[j];
        }
    }

    return key;
}

/// Similar as above but supports nullable values.
template <typename T>
static inline T ALWAYS_INLINE packFixed(
    size_t i,
    size_t keys_size,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const KeysNullMap<T> & bitmap)
{
    union
    {
        T key;
        char bytes[sizeof(key)] = {};
    };

    size_t offset = 0;

    static constexpr auto bitmap_size = std::tuple_size<KeysNullMap<T>>::value;
    static constexpr bool has_bitmap = bitmap_size > 0;

    if (has_bitmap)
    {
        memcpy(bytes + offset, bitmap.data(), bitmap_size * sizeof(UInt8));
        offset += bitmap_size;
    }

    for (size_t j = 0; j < keys_size; ++j)
    {
        bool is_null;

        if (!has_bitmap)
            is_null = false;
        else
        {
            size_t bucket = j / 8;
            size_t off = j % 8;
            is_null = ((bitmap[bucket] >> off) & 1) == 1;
        }

        if (is_null)
            continue;

        switch (key_sizes[j])
        {
        case 1:
            memcpy(bytes + offset, &static_cast<const ColumnUInt8 *>(key_columns[j])->getData()[i], 1);
            offset += 1;
            break;
        case 2:
            memcpy(bytes + offset, &static_cast<const ColumnUInt16 *>(key_columns[j])->getData()[i], 2);
            offset += 2;
            break;
        case 4:
            memcpy(bytes + offset, &static_cast<const ColumnUInt32 *>(key_columns[j])->getData()[i], 4);
            offset += 4;
            break;
        case 8:
            memcpy(bytes + offset, &static_cast<const ColumnUInt64 *>(key_columns[j])->getData()[i], 8);
            offset += 8;
            break;
        default:
            memcpy(
                bytes + offset,
                &static_cast<const ColumnFixedString *>(key_columns[j])->getChars()[i * key_sizes[j]],
                key_sizes[j]);
            offset += key_sizes[j];
        }
    }

    return key;
}

/*
/// Hash a set of keys into a UInt128 value.
static inline UInt128 ALWAYS_INLINE hash128(
    size_t i,
    size_t keys_size,
    const ColumnRawPtrs & key_columns,
    StringRefs & keys,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & sort_key_containers)
{
    UInt128 key;
    SipHash hash;

    for (size_t j = 0; j < keys_size; ++j)
    {
        /// Hashes the key.
        keys[j] = key_columns[j]->getDataAtWithTerminatingZero(i);
        if (!collators.empty() && collators[j] != nullptr)
        {
            // todo check if need to handle the terminating zero
            /// Note if collation is enabled, keys only exists before next call to hash128 since it
            /// will be overwritten in the next call
            keys[j] = collators[j]->sortKey(keys[j].data, keys[j].size - 1, sort_key_containers[j]);
        }
        hash.update(keys[j].data, keys[j].size);
    }

    hash.get128(key);

    return key;
}
*/

/// Almost the same as above but it doesn't return any reference to key data.
static inline UInt128 ALWAYS_INLINE hash128(
    size_t i,
    size_t keys_size,
    const ColumnRawPtrs & key_columns,
    const TiDB::TiDBCollators & collators,
    std::vector<std::string> & sort_key_containers)
{
    UInt128 key{};
    SipHash hash;

    if (collators.empty())
    {
        for (size_t j = 0; j < keys_size; ++j)
            key_columns[j]->updateHashWithValue(i, hash, nullptr, TiDB::dummy_sort_key_contaner);
    }
    else
    {
        for (size_t j = 0; j < keys_size; ++j)
            key_columns[j]->updateHashWithValue(i, hash, collators[j], sort_key_containers[j]);
    }

    hash.get128(key);

    return key;
}


/// Copy keys to the pool. Then put into pool StringRefs to them and return the pointer to the first.
static inline StringRef * ALWAYS_INLINE placeKeysInPool(size_t keys_size, StringRefs & keys, Arena & pool)
{
    for (size_t j = 0; j < keys_size; ++j)
    {
        char * place = pool.alloc(keys[j].size);
        memcpy(place, keys[j].data, keys[j].size); /// TODO padding in Arena and memcpySmall
        keys[j].data = place;
    }

    /// Place the StringRefs on the newly copied keys in the pool.
    char * res = pool.alloc(keys_size * sizeof(StringRef));
    memcpy(res, keys.data(), keys_size * sizeof(StringRef));

    return reinterpret_cast<StringRef *>(res);
}

/*
/// Copy keys to the pool. Then put into pool StringRefs to them and return the pointer to the first.
static inline StringRef * ALWAYS_INLINE extractKeysAndPlaceInPool(
    size_t i,
    size_t keys_size,
    const ColumnRawPtrs & key_columns,
    StringRefs & keys,
    Arena & pool)
{
    for (size_t j = 0; j < keys_size; ++j)
    {
        keys[j] = key_columns[j]->getDataAtWithTerminatingZero(i);
        char * place = pool.alloc(keys[j].size);
        memcpy(place, keys[j].data, keys[j].size);
        keys[j].data = place;
    }

    /// Place the StringRefs on the newly copied keys in the pool.
    char * res = pool.alloc(keys_size * sizeof(StringRef));
    memcpy(res, keys.data(), keys_size * sizeof(StringRef));

    return reinterpret_cast<StringRef *>(res);
}


/// Copy the specified keys to a continuous memory chunk of a pool.
/// Subsequently append StringRef objects referring to each key.
///
/// [key1][key2]...[keyN][ref1][ref2]...[refN]
///   ^     ^        :     |     |
///   +-----|--------:-----+     |
///   :     +--------:-----------+
///   :              :
///   <-------------->
///        (1)
///
/// Return a StringRef object, referring to the area (1) of the memory
/// chunk that contains the keys. In other words, we ignore their StringRefs.
inline StringRef ALWAYS_INLINE extractKeysAndPlaceInPoolContiguous(
    size_t i,
    size_t keys_size,
    const ColumnRawPtrs & key_columns,
    StringRefs & keys,
    const TiDB::TiDBCollators & collators,
    std::vector<std::string> & sort_key_containers,
    Arena & pool)
{
    size_t sum_keys_size = 0;
    for (size_t j = 0; j < keys_size; ++j)
    {
        keys[j] = key_columns[j]->getDataAtWithTerminatingZero(i);
        if (!collators.empty() && collators[j] != nullptr)
        {
            // todo check if need to handle the terminating zero
            keys[j] = collators[j]->sortKey(keys[j].data, keys[j].size - 1, sort_key_containers[j]);
        }
        sum_keys_size += keys[j].size;
    }

    char * res = pool.alloc(sum_keys_size + keys_size * sizeof(StringRef));
    char * place = res;

    for (size_t j = 0; j < keys_size; ++j)
    {
        memcpy(place, keys[j].data, keys[j].size);
        keys[j].data = place;
        place += keys[j].size;
    }

    /// Place the StringRefs on the newly copied keys in the pool.
    memcpy(place, keys.data(), keys_size * sizeof(StringRef));

    return {res, sum_keys_size};
}
*/

/** Serialize keys into a continuous chunk of memory.
  */
static inline StringRef ALWAYS_INLINE serializeKeysToPoolContiguous(
    size_t i,
    size_t keys_size,
    const ColumnRawPtrs & key_columns,
    const TiDB::TiDBCollators & collators,
    std::vector<String> & sort_key_containers,
    Arena & pool)
{
    const char * begin = nullptr;

    size_t sum_size = 0;
    if (!collators.empty())
    {
        for (size_t j = 0; j < keys_size; ++j)
            sum_size
                += key_columns[j]->serializeValueIntoArena(i, pool, begin, collators[j], sort_key_containers[j]).size;
    }
    else
    {
        for (size_t j = 0; j < keys_size; ++j)
            sum_size
                += key_columns[j]->serializeValueIntoArena(i, pool, begin, nullptr, TiDB::dummy_sort_key_contaner).size;
    }

    return {begin, sum_size};
}

/** Pack elements with shuffle instruction.
  * See the explanation in ColumnsHashing.h
  */
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
template <typename T>
static T inline packFixedShuffle(
    const char * __restrict * __restrict srcs,
    size_t num_srcs,
    const size_t * __restrict elem_sizes,
    size_t idx,
    const uint8_t * __restrict masks)
{
    assert(num_srcs > 0);

    __m128i res = _mm_shuffle_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(srcs[0] + elem_sizes[0] * idx)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(masks)));

    for (size_t i = 1; i < num_srcs; ++i)
    {
        res = _mm_xor_si128(
            res,
            _mm_shuffle_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(srcs[i] + elem_sizes[i] * idx)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&masks[i * sizeof(T)]))));
    }

    T out;
    __builtin_memcpy(&out, &res, sizeof(T));
    return out;
}
#endif


template <typename T, size_t step>
void fillFixedBatch(size_t num_rows, const T * source, T * dest)
{
    for (size_t i = 0; i < num_rows; ++i)
    {
        *dest = *source;
        ++source;
        dest += step;
    }
}

/// Move keys of size T into binary blob, starting from offset.
/// It is assumed that offset is aligned to sizeof(T).
/// Example: sizeof(key) = 16, sizeof(T) = 4, offset = 8
/// out[0] : [--------****----]
/// out[1] : [--------****----]
/// ...
template <typename T, typename Key>
void fillFixedBatch(
    size_t keys_size,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    PaddedPODArray<Key> & out,
    size_t & offset)
{
    for (size_t i = 0; i < keys_size; ++i)
    {
        if (key_sizes[i] == sizeof(T))
        {
            const auto * column = key_columns[i];
            size_t num_rows = column->size();
            out.resize_fill(num_rows);

            /// Note: here we violate strict aliasing.
            /// It should be ok as log as we do not reffer to any value from `out` before filling.
            const char * source = static_cast<const ColumnVectorHelper *>(column)->getRawDataBegin<sizeof(T)>();
            T * dest = reinterpret_cast<T *>(reinterpret_cast<char *>(out.data()) + offset);
            static_assert(sizeof(Key) % sizeof(T) == 0);
            fillFixedBatch<T, sizeof(Key) / sizeof(T)>(num_rows, reinterpret_cast<const T *>(source), dest);
            offset += sizeof(T);
        }
    }
}

/// Pack into a binary blob of type T a set of fixed-size keys. Granted that all the keys fit into the
/// binary blob. Keys are placed starting from the longest one.
template <typename T>
void packFixedBatch(
    size_t keys_size,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    PaddedPODArray<T> & out)
{
    size_t offset = 0;
    fillFixedBatch<UInt128>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt64>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt32>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt16>(keys_size, key_columns, key_sizes, out, offset);
    fillFixedBatch<UInt8>(keys_size, key_columns, key_sizes, out, offset);
}

} // namespace DB
