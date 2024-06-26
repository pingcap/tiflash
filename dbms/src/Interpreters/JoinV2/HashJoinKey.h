// Copyright 2024 PingCAP, Inc.
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
#include <Interpreters/JoinV2/HashJoinRowSchema.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
} // namespace ErrorCodes

#define APPLY_FOR_HASH_JOIN_VARIANTS(M) \
    M(OneKey8)                          \
    M(OneKey16)                         \
    M(OneKey32)                         \
    M(OneKey64)                         \
    M(OneKey128)                        \
    M(KeysFixed32)                      \
    M(KeysFixed64)                      \
    M(KeysFixed128)                     \
    M(KeysFixed256)                     \
    M(KeysFixedLonger)                  \
    M(OneKeyStringBin)                  \
    M(OneKeyStringBinPadding)           \
    M(OneKeyString)                     \
    M(KeySerialized)


enum class HashJoinKeyMethod
{
    Empty,
    Cross,
#define M(NAME) NAME,
    APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M
};

struct KeyBuffer
{
    String key_buffer;
    ColumnString::MutablePtr keys_buffer = ColumnString::create();
};

template <typename T>
struct HashJoinKeyOneNumber
{
    const T * vec;
    HashJoinKeyOneNumber(const ColumnRawPtrs & key_columns, const TiDB::TiDBCollators &, KeyBuffer &)
    {
        RUNTIME_ASSERT(key_columns.size() == 1);
        vec = reinterpret_cast<const T *>(key_columns[0]->getRawData().data);
    }

    ALWAYS_INLINE inline T getJoinKeyForBuild(size_t row) { return getJoinKey(row); }
    ALWAYS_INLINE inline T getJoinKeyForProbe(size_t row) { return getJoinKey(row); }

    ALWAYS_INLINE inline T getJoinKey(size_t row) { return vec[row]; }

    ALWAYS_INLINE inline size_t getJoinKeySize(const T &) { return sizeof(T); }

    ALWAYS_INLINE inline void ALWAYS_INLINE serializeJoinKey(const T & t, UInt8 * pos) { memcpy(pos, &t, sizeof(T)); }
};

template <typename T>
struct HashJoinKeysFixed
{
    std::vector<const char *> vec;
    std::vector<size_t> fixed_size;

    HashJoinKeysFixed(const ColumnRawPtrs & key_columns, const TiDB::TiDBCollators &, KeyBuffer &)
    {
        size_t sz = key_columns.size();
        vec.resize(sz);
        fixed_size.resize(sz);
        size_t fixed_size_sum = 0;
        for (size_t i = 0; i < sz; ++i)
        {
            vec[i] = key_columns[i]->getRawData().data;
            fixed_size[i] = key_columns[i]->sizeOfValueIfFixed();
            fixed_size_sum += fixed_size[i];
        }

        RUNTIME_ASSERT(fixed_size_sum <= sizeof(T));
    }

    ALWAYS_INLINE inline T getJoinKeyForBuild(size_t row) { return getJoinKey(row); }
    ALWAYS_INLINE inline T getJoinKeyForProbe(size_t row) { return getJoinKey(row); }

    ALWAYS_INLINE inline T getJoinKey(size_t row)
    {
        union
        {
            T key;
            char data[sizeof(T)];
        };
        size_t sz = vec.size();
        size_t offset = 0;
        for (size_t i = 0; i < sz; ++i)
        {
            memcpy(&data[offset], vec[i] + fixed_size[i] * row, fixed_size[i]);
            offset += fixed_size[i];
        }
        return key;
    }

    ALWAYS_INLINE inline size_t getJoinKeySize(const T &) { return sizeof(T); }

    ALWAYS_INLINE inline void ALWAYS_INLINE serializeJoinKey(const T & t, UInt8 * pos) { memcpy(pos, &t, sizeof(T)); }
};

struct HashJoinKeysFixedLonger
{
    std::vector<const char *> vec;
    std::vector<size_t> fixed_size;
    size_t fixed_size_sum = 0;

    KeyBuffer & buf;

    HashJoinKeysFixedLonger(const ColumnRawPtrs & key_columns, const TiDB::TiDBCollators &, KeyBuffer & key_buffer)
        : buf(key_buffer)
    {
        size_t sz = key_columns.size();
        vec.resize(sz);
        fixed_size.resize(sz);
        for (size_t i = 0; i < sz; ++i)
        {
            vec[i] = key_columns[i]->getRawData().data;
            fixed_size[i] = key_columns[i]->sizeOfValueIfFixed();
            fixed_size_sum += fixed_size[i];
        }
        buf.key_buffer.resize(fixed_size_sum);
    }

    ALWAYS_INLINE inline StringRef getJoinKeyForBuild(size_t row) { return getJoinKey(row); }
    ALWAYS_INLINE inline StringRef getJoinKeyForProbe(size_t row) { return getJoinKey(row); }

    ALWAYS_INLINE inline StringRef getJoinKey(size_t row)
    {
        size_t sz = vec.size();
        size_t offset = 0;
        for (size_t i = 0; i < sz; ++i)
        {
            memcpy(&buf.key_buffer[offset], vec[i] + fixed_size[i] * row, fixed_size[i]);
            offset += fixed_size[i];
        }
        return StringRef(buf.key_buffer.data(), fixed_size_sum);
    }

    ALWAYS_INLINE inline size_t getJoinKeySize(const StringRef &) { return fixed_size_sum; }

    ALWAYS_INLINE inline void ALWAYS_INLINE serializeJoinKey(const StringRef & s, UInt8 * pos)
    {
        memcpy(pos, s.data, fixed_size_sum);
    }
};

template <bool padding>
struct HashJoinKeyStringBin
{
    const ColumnString * column_string;

    HashJoinKeyStringBin(const ColumnRawPtrs & key_columns, const TiDB::TiDBCollators &, KeyBuffer &)
    {
        column_string = assert_cast<const ColumnString *>(key_columns[0]);
    }

    ALWAYS_INLINE inline StringRef getJoinKeyForBuild(size_t row) { return getJoinKey(row); }
    ALWAYS_INLINE inline StringRef getJoinKeyForProbe(size_t row) { return getJoinKey(row); }

    ALWAYS_INLINE inline StringRef getJoinKey(size_t row)
    {
        StringRef key = column_string->getDataAt(row);
        key = BinCollatorSortKey<padding>(key.data, key.size);
        return key;
    }

    ALWAYS_INLINE inline size_t getJoinKeySize(const StringRef & s) { return sizeof(size_t) + s.size; }

    ALWAYS_INLINE inline void ALWAYS_INLINE serializeJoinKey(const StringRef & s, UInt8 * pos)
    {
        memcpy(pos, &s.size, sizeof(size_t));
        inline_memcpy(pos + sizeof(size_t), s.data, s.size);
    }
};

struct HashJoinKeyString
{
    const ColumnString * column_string;
    TiDB::TiDBCollatorPtr collator = nullptr;

    KeyBuffer & buf;

    bool init_for_build = false;

    HashJoinKeyString(const ColumnRawPtrs & key_columns, const TiDB::TiDBCollators & collators, KeyBuffer & key_buffer)
        : buf(key_buffer)
    {
        column_string = assert_cast<const ColumnString *>(key_columns[0]);
        RUNTIME_ASSERT(!collators.empty());
        collator = collators[0];
    }

    ALWAYS_INLINE inline StringRef getJoinKeyForBuild(size_t row)
    {
        if unlikely (!init_for_build)
        {
            size_t sz = column_string->size();
            size_t byte_size = column_string->byteSize();
            buf.keys_buffer->reserveWithTotalMemoryHint(sz, byte_size);
            buf.keys_buffer->popBack(buf.keys_buffer->size());
            for (size_t i = 0; i < sz; ++i)
            {
                StringRef key = column_string->getDataAt(i);
                key = collator->sortKey(key.data, key.size, buf.key_buffer);
                buf.keys_buffer->insertData(key.data, key.size);
            }
            init_for_build = true;
        }
        assert(row < buf.keys_buffer->size());
        return buf.keys_buffer->getDataAt(row);
    }

    ALWAYS_INLINE inline StringRef getJoinKeyForProbe(size_t row)
    {
        StringRef key = column_string->getDataAt(row);
        key = collator->sortKey(key.data, key.size, buf.key_buffer);
        return key;
    }

    ALWAYS_INLINE inline size_t getJoinKeySize(const StringRef & s) { return sizeof(size_t) + s.size; }

    ALWAYS_INLINE inline void ALWAYS_INLINE serializeJoinKey(const StringRef & s, UInt8 * pos)
    {
        memcpy(pos, &s.size, sizeof(size_t));
        inline_memcpy(pos + sizeof(size_t), s.data, s.size);
    }
};

struct HashJoinKeySerialized
{
    ColumnRawPtrs key_columns;
    size_t keys_size;
    TiDB::TiDBCollators collators;

    KeyBuffer & buf;

    bool init_for_build = false;

    std::vector<String> sort_key_containers;
    Arena pool;

    size_t last_key_size = 0;

    HashJoinKeySerialized(
        const ColumnRawPtrs & key_columns_,
        const TiDB::TiDBCollators & collators_,
        KeyBuffer & key_buffer)
        : key_columns(key_columns_)
        , keys_size(key_columns_.size())
        , collators(collators_)
        , buf(key_buffer)
    {
        RUNTIME_ASSERT(keys_size > 0);
        sort_key_containers.resize(keys_size);
    }

    //TODO: use a more effient way instead of calling `serializeKeysToPoolContiguous` for each row.
    ALWAYS_INLINE inline StringRef getJoinKeyForBuild(size_t row)
    {
        if unlikely (!init_for_build)
        {
            size_t sz = key_columns[0]->size();
            buf.keys_buffer->popBack(buf.keys_buffer->size());
            for (size_t i = 0; i < sz; ++i)
            {
                StringRef key
                    = serializeKeysToPoolContiguous(i, keys_size, key_columns, collators, sort_key_containers, pool);
                buf.keys_buffer->insertData(key.data, key.size);
                pool.rollback(key.size);
            }
            init_for_build = true;
        }
        assert(row < buf.keys_buffer->size());
        return buf.keys_buffer->getDataAt(row);
    }

    ALWAYS_INLINE inline StringRef getJoinKeyForProbe(size_t row)
    {
        pool.rollback(last_key_size);
        StringRef key
            = serializeKeysToPoolContiguous(row, keys_size, key_columns, collators, sort_key_containers, pool);
        last_key_size = key.size;
        return key;
    }

    ALWAYS_INLINE inline size_t getJoinKeySize(const StringRef & s) { return sizeof(size_t) + s.size; }

    ALWAYS_INLINE inline void ALWAYS_INLINE serializeJoinKey(const StringRef & s, UInt8 * pos)
    {
        memcpy(pos, &s.size, sizeof(size_t));
        inline_memcpy(pos + sizeof(size_t), s.data, s.size);
    }
};

template <HashJoinKeyMethod method>
struct HashJoinKeyGetterForType;

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey8>
{
    using Type = HashJoinKeyOneNumber<UInt8>;
    using Hash = TrivialHash;
    using HashValueType = UInt8;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey16>
{
    using Type = HashJoinKeyOneNumber<UInt16>;
    using Hash = TrivialHash;
    using HashValueType = UInt16;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey32>
{
    using Type = HashJoinKeyOneNumber<UInt32>;
    using Hash = HashCRC32<UInt32>;
    using HashValueType = UInt32;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey64>
{
    using Type = HashJoinKeyOneNumber<UInt64>;
    using Hash = HashCRC32<UInt64>;
    using HashValueType = UInt32;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey128>
{
    using Type = HashJoinKeyOneNumber<UInt128>;
    using Hash = HashCRC32<UInt128>;
    using HashValueType = UInt32;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixed32>
{
    using Type = HashJoinKeysFixed<UInt32>;
    using Hash = HashCRC32<UInt32>;
    using HashValueType = UInt32;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixed64>
{
    using Type = HashJoinKeysFixed<UInt64>;
    using Hash = HashCRC32<UInt64>;
    using HashValueType = UInt32;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixed128>
{
    using Type = HashJoinKeysFixed<UInt128>;
    using Hash = HashCRC32<UInt128>;
    using HashValueType = UInt32;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixed256>
{
    using Type = HashJoinKeysFixed<UInt256>;
    using Hash = HashCRC32<UInt256>;
    using HashValueType = UInt32;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixedLonger>
{
    using Type = HashJoinKeysFixedLonger;
    using Hash = StringRefHash;
    using HashValueType = size_t;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKeyStringBin>
{
    using Type = HashJoinKeyStringBin<false>;
    using Hash = StringRefHash;
    using HashValueType = size_t;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKeyStringBinPadding>
{
    using Type = HashJoinKeyStringBin<true>;
    using Hash = StringRefHash;
    using HashValueType = size_t;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKeyString>
{
    using Type = HashJoinKeyString;
    using Hash = StringRefHash;
    using HashValueType = size_t;
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeySerialized>
{
    using Type = HashJoinKeySerialized;
    using Hash = StringRefHash;
    using HashValueType = size_t;
};

} // namespace DB