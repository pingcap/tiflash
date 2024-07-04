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

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/Hash.h>
#include <Common/assert_cast.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/JoinV2/HashJoinRowLayout.h>
#include <common/mem_utils_opt.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
extern const int UNKNOWN_SET_DATA_VARIANT;
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
    M(KeysFixedOther)                   \
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

template <typename T>
class HashJoinKeyOneNumber
{
public:
    using KeyType = T;
    explicit HashJoinKeyOneNumber(const TiDB::TiDBCollators &) {}

    void reset(const ColumnRawPtrs & key_columns)
    {
        RUNTIME_ASSERT(key_columns.size() == 1);
        vec = reinterpret_cast<const T *>(key_columns[0]->getRawData().data);
    }

    ALWAYS_INLINE T getJoinKey(size_t row) { return vec[row]; }
    ALWAYS_INLINE T getJoinKeyWithBufferHint(size_t row) { return vec[row]; }

    ALWAYS_INLINE size_t getJoinKeySize(const T &) { return sizeof(T); }

    ALWAYS_INLINE void serializeJoinKey(const T & t, UInt8 * pos) { memcpy(pos, &t, sizeof(T)); }

    ALWAYS_INLINE T deserializeJoinKey(const UInt8 * pos)
    {
        T t;
        memcpy(&t, pos, sizeof(T));
        return t;
    }

    ALWAYS_INLINE bool joinKeyIsEqual(T key1, T key2) { return key1 == key2; }

private:
    const T * vec = nullptr;
};

template <typename T>
class HashJoinKeysFixed
{
public:
    using KeyType = T;
    explicit HashJoinKeysFixed(const TiDB::TiDBCollators &) {}

    void reset(const ColumnRawPtrs & key_columns)
    {
        size_t sz = key_columns.size();
        vec.resize(sz);
        fixed_size.resize(sz);
        fixed_size_sum = 0;
        for (size_t i = 0; i < sz; ++i)
        {
            vec[i] = key_columns[i]->getRawData().data;
            fixed_size[i] = key_columns[i]->sizeOfValueIfFixed();
            fixed_size_sum += fixed_size[i];
        }

        RUNTIME_ASSERT(fixed_size_sum <= sizeof(T));
    }

    ALWAYS_INLINE T getJoinKey(size_t row)
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

    ALWAYS_INLINE T getJoinKeyWithBufferHint(size_t row) { return getJoinKey(row); }

    ALWAYS_INLINE size_t getJoinKeySize(const T &) { return fixed_size_sum; }

    ALWAYS_INLINE void serializeJoinKey(const T & t, UInt8 * pos)
    {
        union
        {
            T key;
            char data[sizeof(T)];
        };
        key = t;
        memcpy(pos, data, fixed_size_sum);
    }

    ALWAYS_INLINE T deserializeJoinKey(const UInt8 * pos)
    {
        union
        {
            T key;
            char data[sizeof(T)];
        };
        memcpy(data, pos, fixed_size_sum);
        return key;
    }

    ALWAYS_INLINE bool joinKeyIsEqual(T key1, T key2) { return key1 == key2; }

private:
    std::vector<const char *> vec;
    std::vector<size_t> fixed_size;
    size_t fixed_size_sum;
};

class HashJoinKeysFixedOther
{
public:
    using KeyType = const char *;
    explicit HashJoinKeysFixedOther(const TiDB::TiDBCollators &) {}

    void reset(const ColumnRawPtrs & key_columns)
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
        key_buffer.resize(fixed_size_sum);
    }

    ALWAYS_INLINE KeyType getJoinKey(size_t row)
    {
        size_t sz = vec.size();
        size_t offset = 0;
        for (size_t i = 0; i < sz; ++i)
        {
            memcpy(&key_buffer[offset], vec[i] + fixed_size[i] * row, fixed_size[i]);
            offset += fixed_size[i];
        }
        return key_buffer.data();
    }

    ALWAYS_INLINE KeyType getJoinKeyWithBufferHint(size_t row) { return getJoinKey(row); }

    ALWAYS_INLINE size_t getJoinKeySize(KeyType) { return fixed_size_sum; }

    ALWAYS_INLINE void serializeJoinKey(KeyType s, UInt8 * pos) { memcpy(pos, s, fixed_size_sum); }

    ALWAYS_INLINE KeyType deserializeJoinKey(const UInt8 * pos) { return reinterpret_cast<KeyType>(pos); }

    ALWAYS_INLINE bool joinKeyIsEqual(KeyType key1, KeyType key2)
    {
        return mem_utils::IsStrEqualWithSameSize(key1, key2, fixed_size_sum);
    }

private:
    std::vector<const char *> vec;
    std::vector<size_t> fixed_size;
    size_t fixed_size_sum = 0;

    String key_buffer;
};

template <bool padding>
class HashJoinKeyStringBin
{
public:
    using KeyType = StringRef;
    explicit HashJoinKeyStringBin(const TiDB::TiDBCollators &) {}

    void reset(const ColumnRawPtrs & key_columns) { column_string = assert_cast<const ColumnString *>(key_columns[0]); }

    ALWAYS_INLINE StringRef getJoinKey(size_t row)
    {
        StringRef key = column_string->getDataAt(row);
        key = BinCollatorSortKey<padding>(key.data, key.size);
        return key;
    }

    ALWAYS_INLINE StringRef getJoinKeyWithBufferHint(size_t row) { return getJoinKey(row); }

    ALWAYS_INLINE size_t getJoinKeySize(const StringRef & s) { return sizeof(size_t) + s.size; }

    ALWAYS_INLINE void serializeJoinKey(const StringRef & s, UInt8 * pos)
    {
        memcpy(pos, &s.size, sizeof(size_t));
        inline_memcpy(pos + sizeof(size_t), s.data, s.size);
    }

    ALWAYS_INLINE KeyType deserializeJoinKey(const UInt8 * pos)
    {
        StringRef s;
        memcpy(&s.size, pos, sizeof(size_t));
        s.data = reinterpret_cast<const char *>(pos + sizeof(size_t));
        return s;
    }

    ALWAYS_INLINE bool joinKeyIsEqual(const StringRef & key1, const StringRef & key2) { return key1 == key2; }

private:
    const ColumnString * column_string = nullptr;
};

class HashJoinKeyString
{
public:
    using KeyType = StringRef;
    explicit HashJoinKeyString(const TiDB::TiDBCollators & collators)
    {
        RUNTIME_ASSERT(!collators.empty());
        collator = collators[0];
    }

    void reset(const ColumnRawPtrs & key_columns)
    {
        column_string = assert_cast<const ColumnString *>(key_columns[0]);
        buffer_initialized = false;
    }

    ALWAYS_INLINE StringRef getJoinKey(size_t row)
    {
        StringRef key = column_string->getDataAt(row);
        key = collator->sortKey(key.data, key.size, key_buffer);
        return key;
    }

    ALWAYS_INLINE StringRef getJoinKeyWithBufferHint(size_t row)
    {
        if unlikely (!buffer_initialized)
        {
            size_t sz = column_string->size();
            size_t byte_size = column_string->byteSize();
            keys_buffer->reserveWithTotalMemoryHint(sz, byte_size);
            keys_buffer->popBack(keys_buffer->size());
            for (size_t i = 0; i < sz; ++i)
            {
                StringRef key = column_string->getDataAt(i);
                key = collator->sortKey(key.data, key.size, key_buffer);
                keys_buffer->insertData(key.data, key.size);
            }
            buffer_initialized = true;
        }
        assert(row < keys_buffer->size());
        return keys_buffer->getDataAt(row);
    }

    ALWAYS_INLINE size_t getJoinKeySize(const StringRef & s) { return sizeof(size_t) + s.size; }

    ALWAYS_INLINE void serializeJoinKey(const StringRef & s, UInt8 * pos)
    {
        memcpy(pos, &s.size, sizeof(size_t));
        inline_memcpy(pos + sizeof(size_t), s.data, s.size);
    }

    ALWAYS_INLINE KeyType deserializeJoinKey(const UInt8 * pos)
    {
        StringRef s;
        memcpy(&s.size, pos, sizeof(size_t));
        s.data = reinterpret_cast<const char *>(pos + sizeof(size_t));
        return s;
    }

    ALWAYS_INLINE bool joinKeyIsEqual(const StringRef & key1, const StringRef & key2) { return key1 == key2; }

private:
    const ColumnString * column_string = nullptr;
    TiDB::TiDBCollatorPtr collator = nullptr;

    String key_buffer;
    ColumnString::MutablePtr keys_buffer = ColumnString::create();

    bool buffer_initialized = false;
};

class HashJoinKeySerialized
{
public:
    using KeyType = StringRef;
    explicit HashJoinKeySerialized(const TiDB::TiDBCollators & collators_) { collators = collators_; }

    void reset(const ColumnRawPtrs & key_columns_)
    {
        key_columns = key_columns_;
        keys_size = key_columns.size();
        RUNTIME_ASSERT(keys_size > 0);
        RUNTIME_ASSERT(keys_size <= collators.size());
        sort_key_containers.resize(keys_size);

        buffer_initialized = false;
        pool.rollback(last_key_size);
        last_key_size = 0;
    }

    ALWAYS_INLINE StringRef getJoinKey(size_t row)
    {
        pool.rollback(last_key_size);
        StringRef key
            = serializeKeysToPoolContiguous(row, keys_size, key_columns, collators, sort_key_containers, pool);
        last_key_size = key.size;
        return key;
    }

    //TODO: use a more effient way instead of calling `serializeKeysToPoolContiguous` for each row.
    ALWAYS_INLINE StringRef getJoinKeyWithBufferHint(size_t row)
    {
        if unlikely (!buffer_initialized)
        {
            size_t sz = key_columns[0]->size();
            keys_buffer->popBack(keys_buffer->size());
            for (size_t i = 0; i < sz; ++i)
            {
                StringRef key
                    = serializeKeysToPoolContiguous(i, keys_size, key_columns, collators, sort_key_containers, pool);
                keys_buffer->insertData(key.data, key.size);
                pool.rollback(key.size);
            }
            buffer_initialized = true;
        }
        assert(row < keys_buffer->size());
        return keys_buffer->getDataAt(row);
    }

    ALWAYS_INLINE size_t getJoinKeySize(const StringRef & s) { return sizeof(size_t) + s.size; }

    ALWAYS_INLINE void serializeJoinKey(const StringRef & s, UInt8 * pos)
    {
        memcpy(pos, &s.size, sizeof(size_t));
        inline_memcpy(pos + sizeof(size_t), s.data, s.size);
    }

    ALWAYS_INLINE KeyType deserializeJoinKey(const UInt8 * pos)
    {
        StringRef s;
        memcpy(&s.size, pos, sizeof(size_t));
        s.data = reinterpret_cast<const char *>(pos + sizeof(size_t));
        return s;
    }

    ALWAYS_INLINE bool joinKeyIsEqual(const StringRef & key1, const StringRef & key2) { return key1 == key2; }

private:
    ColumnRawPtrs key_columns;
    size_t keys_size = 0;
    TiDB::TiDBCollators collators;

    String key_buffer;
    ColumnString::MutablePtr keys_buffer = ColumnString::create();

    bool buffer_initialized = false;

    std::vector<String> sort_key_containers;
    Arena pool;
    size_t last_key_size = 0;
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
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixedOther>
{
    using Type = HashJoinKeysFixedOther;
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

size_t getHashValueByteSize(HashJoinKeyMethod method);

HashJoinKeyMethod findFixedSizeJoinKeyMethod(size_t keys_size, size_t all_key_fixed_size);

std::unique_ptr<void, std::function<void(void *)>> createHashJoinKeyGetter(
    HashJoinKeyMethod method,
    const TiDB::TiDBCollators & collators);

void resetHashJoinKeyGetter(
    HashJoinKeyMethod method,
    const ColumnRawPtrs & key_columns,
    std::unique_ptr<void, std::function<void(void *)>> & key_getter);

} // namespace DB
