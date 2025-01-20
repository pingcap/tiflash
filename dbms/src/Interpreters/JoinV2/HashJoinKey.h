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
#include <Common/Exception.h>
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
class alignas(CPU_CACHE_LINE_SIZE) HashJoinKeyOneNumber
{
public:
    using KeyType = T;
    explicit HashJoinKeyOneNumber(const TiDB::TiDBCollators &) {}

    void reset(const ColumnRawPtrs & key_columns, size_t raw_required_key_size)
    {
        RUNTIME_CHECK(key_columns.size() == 1);
        vec = reinterpret_cast<const T *>(key_columns[0]->getRawData().data);
        if (raw_required_key_size == 0)
            required_key_offset = sizeof(T);
        else
            required_key_offset = 0;
    }

    ALWAYS_INLINE T getJoinKey(size_t row) { return unalignedLoad<T>(vec + row); }
    ALWAYS_INLINE T getJoinKeyWithBuffer(size_t row) { return unalignedLoad<T>(vec + row); }

    ALWAYS_INLINE size_t getJoinKeyByteSize(const T &) const { return sizeof(T); }

    ALWAYS_INLINE void serializeJoinKey(const T & t, char * pos) const
    {
        tiflash_compiler_builtin_memcpy(pos, &t, sizeof(T));
    }

    ALWAYS_INLINE T deserializeJoinKey(const char * pos) const
    {
        T t;
        tiflash_compiler_builtin_memcpy(&t, pos, sizeof(T));
        return t;
    }

    static constexpr bool joinKeyCompareHashFirst() { return false; }

    ALWAYS_INLINE bool joinKeyIsEqual(T key1, T key2) const { return key1 == key2; }

    ALWAYS_INLINE size_t getRequiredKeyOffset(const T &) const { return required_key_offset; }

private:
    const T * vec = nullptr;
    size_t required_key_offset = 0;
};

template <typename T>
class alignas(CPU_CACHE_LINE_SIZE) HashJoinKeysFixed
{
public:
    using KeyType = T;
    explicit HashJoinKeysFixed(const TiDB::TiDBCollators &) {}

    void reset(const ColumnRawPtrs & key_columns, size_t raw_required_key_size)
    {
        RUNTIME_CHECK(!key_columns.empty());
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

        RUNTIME_CHECK(fixed_size_sum <= sizeof(T));

        RUNTIME_CHECK(raw_required_key_size <= sz);
        required_key_offset = sizeof(T);
        for (size_t i = 0; i < raw_required_key_size; ++i)
            required_key_offset -= fixed_size[sz - 1 - i];
    }

    ALWAYS_INLINE T getJoinKey(size_t row)
    {
        union
        {
            T key;
            char data[sizeof(T)]{};
        };
        size_t sz = vec.size();
        size_t offset = sizeof(T) - fixed_size_sum;
        for (size_t i = 0; i < sz; ++i)
        {
            switch (fixed_size[i])
            {
            case 1:
                tiflash_compiler_builtin_memcpy(&data[offset], vec[i] + fixed_size[i] * row, 1);
                break;
            case 2:
                tiflash_compiler_builtin_memcpy(&data[offset], vec[i] + fixed_size[i] * row, 2);
                break;
            case 4:
                tiflash_compiler_builtin_memcpy(&data[offset], vec[i] + fixed_size[i] * row, 4);
                break;
            case 8:
                tiflash_compiler_builtin_memcpy(&data[offset], vec[i] + fixed_size[i] * row, 8);
                break;
            case 16:
                tiflash_compiler_builtin_memcpy(&data[offset], vec[i] + fixed_size[i] * row, 16);
                break;
            case 32:
                tiflash_compiler_builtin_memcpy(&data[offset], vec[i] + fixed_size[i] * row, 32);
                break;
            default:
                inline_memcpy(&data[offset], vec[i] + fixed_size[i] * row, fixed_size[i]);
            }
            offset += fixed_size[i];
        }
        return key;
    }

    ALWAYS_INLINE T getJoinKeyWithBuffer(size_t row) { return getJoinKey(row); }

    ALWAYS_INLINE size_t getJoinKeyByteSize(const T &) const { return sizeof(T); }

    ALWAYS_INLINE void serializeJoinKey(const T & t, char * pos) const
    {
        tiflash_compiler_builtin_memcpy(pos, &t, sizeof(T));
    }

    ALWAYS_INLINE T deserializeJoinKey(const char * pos) const
    {
        T key;
        tiflash_compiler_builtin_memcpy(&key, pos, sizeof(T));
        return key;
    }

    static constexpr bool joinKeyCompareHashFirst() { return false; }

    ALWAYS_INLINE bool joinKeyIsEqual(T key1, T key2) const { return key1 == key2; }

    ALWAYS_INLINE size_t getRequiredKeyOffset(const T &) const { return required_key_offset; }

private:
    std::vector<const char *> vec;
    std::vector<size_t> fixed_size;
    size_t fixed_size_sum = 0;
    size_t required_key_offset = 0;
};

class alignas(CPU_CACHE_LINE_SIZE) HashJoinKeysFixedOther
{
public:
    using KeyType = StringRef;
    explicit HashJoinKeysFixedOther(const TiDB::TiDBCollators &) {}

    void reset(const ColumnRawPtrs & key_columns, size_t raw_required_key_size)
    {
        RUNTIME_CHECK(!key_columns.empty());
        rows = key_columns[0]->size();
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
        key_buffer.resize(fixed_size_sum);

        RUNTIME_CHECK(raw_required_key_size <= sz);
        required_key_offset = fixed_size_sum;
        for (size_t i = 0; i < raw_required_key_size; ++i)
            required_key_offset -= fixed_size[sz - 1 - i];

        buffer_initialized = false;
    }

    ALWAYS_INLINE KeyType getJoinKey(size_t row)
    {
        size_t sz = vec.size();
        size_t offset = 0;
        for (size_t i = 0; i < sz; ++i)
        {
            switch (fixed_size[i])
            {
            case 1:
                tiflash_compiler_builtin_memcpy(&key_buffer[offset], vec[i] + fixed_size[i] * row, 1);
                break;
            case 2:
                tiflash_compiler_builtin_memcpy(&key_buffer[offset], vec[i] + fixed_size[i] * row, 2);
                break;
            case 4:
                tiflash_compiler_builtin_memcpy(&key_buffer[offset], vec[i] + fixed_size[i] * row, 4);
                break;
            case 8:
                tiflash_compiler_builtin_memcpy(&key_buffer[offset], vec[i] + fixed_size[i] * row, 8);
                break;
            case 16:
                tiflash_compiler_builtin_memcpy(&key_buffer[offset], vec[i] + fixed_size[i] * row, 16);
                break;
            case 32:
                tiflash_compiler_builtin_memcpy(&key_buffer[offset], vec[i] + fixed_size[i] * row, 32);
                break;
            default:
                inline_memcpy(&key_buffer[offset], vec[i] + fixed_size[i] * row, fixed_size[i]);
            }
            offset += fixed_size[i];
        }
        return StringRef(key_buffer.data(), fixed_size_sum);
    }

    ALWAYS_INLINE KeyType getJoinKeyWithBuffer(size_t row)
    {
        if unlikely (!buffer_initialized)
        {
            keys_buffer.resize(rows * fixed_size_sum);
            size_t offset = 0;
            size_t sz = vec.size();
            for (size_t i = 0; i < rows; ++i)
            {
                for (size_t j = 0; j < sz; ++j)
                {
                    inline_memcpy(&keys_buffer[offset], vec[j] + fixed_size[j] * i, fixed_size[j]);
                    offset += fixed_size[j];
                }
            }
            buffer_initialized = true;
        }
        assert(row < rows);
        return StringRef(&keys_buffer[row * fixed_size_sum], fixed_size_sum);
    }

    ALWAYS_INLINE size_t getJoinKeyByteSize(const KeyType &) const { return fixed_size_sum; }

    ALWAYS_INLINE void serializeJoinKey(const KeyType & s, char * pos) const
    {
        inline_memcpy(pos, s.data, fixed_size_sum);
    }

    ALWAYS_INLINE KeyType deserializeJoinKey(const char * pos) const { return StringRef(pos, fixed_size_sum); }

    static constexpr bool joinKeyCompareHashFirst() { return true; }

    ALWAYS_INLINE bool joinKeyIsEqual(const KeyType & key1, const KeyType & key2) const
    {
        return mem_utils::IsStrEqualWithSameSize(key1.data, key2.data, fixed_size_sum);
    }

    ALWAYS_INLINE size_t getRequiredKeyOffset(const KeyType &) const { return required_key_offset; }

private:
    size_t rows = 0;
    std::vector<const char *> vec;
    std::vector<size_t> fixed_size;
    size_t fixed_size_sum = 0;

    String key_buffer;
    std::vector<char> keys_buffer;

    size_t required_key_offset = 0;

    bool buffer_initialized = false;
};

template <bool padding>
class alignas(CPU_CACHE_LINE_SIZE) HashJoinKeyStringBin
{
public:
    using KeyType = StringRef;
    explicit HashJoinKeyStringBin(const TiDB::TiDBCollators &) {}

    void reset(const ColumnRawPtrs & key_columns, size_t raw_required_key_size)
    {
        RUNTIME_CHECK(key_columns.size() == 1);
        column_string = assert_cast<const ColumnString *>(key_columns[0]);
        required_key_size = raw_required_key_size > 0 ? 1 : 0;
        if constexpr (padding)
        {
            RUNTIME_CHECK(required_key_size == 0);
        }
    }

    ALWAYS_INLINE StringRef getJoinKey(size_t row)
    {
        StringRef key;
        if (!padding && required_key_size)
        {
            /// If `required_key_size == 1`, the serialized data will be used in `ColumnString::deserializeAndInsertFromPos`,
            /// which does not add a terminating zero (`\0`) at the end of the string. This behavior aligns with the
            /// implementation of `ColumnString::serializeToPos`, where the serialized data is produced without removing
            /// the terminating zero.
            key = column_string->getDataAtWithTerminatingZero(row);
        }
        else
        {
            key = column_string->getDataAt(row);
        }
        key = BinCollatorSortKey<padding>(key.data, key.size);
        return key;
    }

    ALWAYS_INLINE StringRef getJoinKeyWithBuffer(size_t row) { return getJoinKey(row); }

    ALWAYS_INLINE size_t getJoinKeyByteSize(const StringRef & s) const { return sizeof(UInt32) + s.size; }

    ALWAYS_INLINE void serializeJoinKey(const StringRef & s, char * pos) const
    {
        tiflash_compiler_builtin_memcpy(pos, &s.size, sizeof(UInt32));
        inline_memcpy(pos + sizeof(UInt32), s.data, s.size);
    }

    ALWAYS_INLINE KeyType deserializeJoinKey(const char * pos) const
    {
        StringRef s;
        tiflash_compiler_builtin_memcpy(&s.size, pos, sizeof(UInt32));
        s.data = reinterpret_cast<const char *>(pos + sizeof(UInt32));
        return s;
    }

    static constexpr bool joinKeyCompareHashFirst() { return true; }

    ALWAYS_INLINE bool joinKeyIsEqual(const StringRef & key1, const StringRef & key2) const { return key1 == key2; }

    ALWAYS_INLINE size_t getRequiredKeyOffset(const StringRef & key) const
    {
        return required_key_size == 0 ? getJoinKeyByteSize(key) : 0;
    }

private:
    const ColumnString * column_string = nullptr;
    size_t required_key_size = 0;
};

class alignas(CPU_CACHE_LINE_SIZE) HashJoinKeyString
{
public:
    using KeyType = StringRef;
    explicit HashJoinKeyString(const TiDB::TiDBCollators & collators)
    {
        RUNTIME_CHECK(!collators.empty());
        collator = collators[0];
    }

    void reset(const ColumnRawPtrs & key_columns, size_t raw_required_key_size)
    {
        RUNTIME_CHECK(key_columns.size() == 1);
        column_string = assert_cast<const ColumnString *>(key_columns[0]);
        buffer_initialized = false;
        RUNTIME_CHECK(raw_required_key_size == 0);
    }

    ALWAYS_INLINE StringRef getJoinKey(size_t row)
    {
        StringRef key = column_string->getDataAt(row);
        key = collator->sortKey(key.data, key.size, key_buffer);
        return key;
    }

    ALWAYS_INLINE StringRef getJoinKeyWithBuffer(size_t row)
    {
        if unlikely (!buffer_initialized)
        {
            size_t rows = column_string->size();
            size_t byte_size = column_string->byteSize();
            keys_buffer->reserveWithTotalMemoryHint(rows, byte_size);
            keys_buffer->popBack(keys_buffer->size());
            for (size_t i = 0; i < rows; ++i)
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

    ALWAYS_INLINE size_t getJoinKeyByteSize(const StringRef & s) const { return sizeof(UInt32) + s.size; }

    ALWAYS_INLINE void serializeJoinKey(const StringRef & s, char * pos) const
    {
        tiflash_compiler_builtin_memcpy(pos, &s.size, sizeof(UInt32));
        inline_memcpy(pos + sizeof(UInt32), s.data, s.size);
    }

    ALWAYS_INLINE KeyType deserializeJoinKey(const char * pos) const
    {
        StringRef s;
        tiflash_compiler_builtin_memcpy(&s.size, pos, sizeof(UInt32));
        s.data = reinterpret_cast<const char *>(pos + sizeof(UInt32));
        return s;
    }

    static constexpr bool joinKeyCompareHashFirst() { return true; }

    ALWAYS_INLINE bool joinKeyIsEqual(const StringRef & key1, const StringRef & key2) const { return key1 == key2; }

    ALWAYS_INLINE size_t getRequiredKeyOffset(const StringRef & key) const { return getJoinKeyByteSize(key); }

private:
    const ColumnString * column_string = nullptr;
    TiDB::TiDBCollatorPtr collator = nullptr;

    String key_buffer;
    ColumnString::MutablePtr keys_buffer = ColumnString::create();

    bool buffer_initialized = false;
};

class alignas(CPU_CACHE_LINE_SIZE) HashJoinKeySerialized
{
public:
    using KeyType = StringRef;
    explicit HashJoinKeySerialized(const TiDB::TiDBCollators & collators_) { collators = collators_; }

    void reset(const ColumnRawPtrs & key_columns_, size_t raw_required_key_size)
    {
        key_columns = key_columns_;
        keys_size = key_columns.size();
        RUNTIME_CHECK(keys_size > 0);
        RUNTIME_CHECK(keys_size <= collators.size());
        sort_key_containers.resize(keys_size);

        buffer_initialized = false;
        pool.rollback(last_key_size);
        last_key_size = 0;

        RUNTIME_CHECK(raw_required_key_size == 0);
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
    ALWAYS_INLINE StringRef getJoinKeyWithBuffer(size_t row)
    {
        if unlikely (!buffer_initialized)
        {
            size_t rows = key_columns[0]->size();
            keys_buffer->popBack(keys_buffer->size());
            for (size_t i = 0; i < rows; ++i)
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

    ALWAYS_INLINE size_t getJoinKeyByteSize(const StringRef & s) const { return sizeof(UInt32) + s.size; }

    ALWAYS_INLINE void serializeJoinKey(const StringRef & s, char * pos) const
    {
        tiflash_compiler_builtin_memcpy(pos, &s.size, sizeof(UInt32));
        inline_memcpy(pos + sizeof(UInt32), s.data, s.size);
    }

    ALWAYS_INLINE KeyType deserializeJoinKey(const char * pos) const
    {
        StringRef s;
        tiflash_compiler_builtin_memcpy(&s.size, pos, sizeof(UInt32));
        s.data = reinterpret_cast<const char *>(pos + sizeof(UInt32));
        return s;
    }

    static constexpr bool joinKeyCompareHashFirst() { return true; }

    ALWAYS_INLINE bool joinKeyIsEqual(const StringRef & key1, const StringRef & key2) const { return key1 == key2; }

    ALWAYS_INLINE size_t getRequiredKeyOffset(const StringRef & key) const { return getJoinKeyByteSize(key); }

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
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, UInt8>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey16>
{
    using Type = HashJoinKeyOneNumber<UInt16>;
    using Hash = TrivialHash;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, UInt16>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey32>
{
    using Type = HashJoinKeyOneNumber<UInt32>;
    using Hash = HashCRC32<UInt32>;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, UInt32>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey64>
{
    using Type = HashJoinKeyOneNumber<UInt64>;
    using Hash = HashCRC32<UInt64>;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, UInt32>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKey128>
{
    using Type = HashJoinKeyOneNumber<UInt128>;
    using Hash = HashCRC32<UInt128>;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, UInt32>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixed32>
{
    using Type = HashJoinKeysFixed<UInt32>;
    using Hash = HashCRC32<UInt32>;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, UInt32>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixed64>
{
    using Type = HashJoinKeysFixed<UInt64>;
    using Hash = HashCRC32<UInt64>;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, UInt32>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixed128>
{
    using Type = HashJoinKeysFixed<UInt128>;
    using Hash = HashCRC32<UInt128>;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, UInt32>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixed256>
{
    using Type = HashJoinKeysFixed<UInt256>;
    using Hash = HashCRC32<UInt256>;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, UInt32>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeysFixedOther>
{
    using Type = HashJoinKeysFixedOther;
    using Hash = StringRefHash;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, size_t>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKeyStringBin>
{
    using Type = HashJoinKeyStringBin<false>;
    using Hash = StringRefHash;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, size_t>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKeyStringBinPadding>
{
    using Type = HashJoinKeyStringBin<true>;
    using Hash = StringRefHash;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, size_t>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::OneKeyString>
{
    using Type = HashJoinKeyString;
    using Hash = StringRefHash;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, size_t>);
};

template <>
struct HashJoinKeyGetterForType<HashJoinKeyMethod::KeySerialized>
{
    using Type = HashJoinKeySerialized;
    using Hash = StringRefHash;
    using HashValueType = std::invoke_result_t<Hash, Type::KeyType>;
    static_assert(std::is_same_v<HashValueType, size_t>);
};

size_t getHashValueByteSize(HashJoinKeyMethod method);

HashJoinKeyMethod findFixedSizeJoinKeyMethod(size_t keys_size, size_t all_key_fixed_size);

std::unique_ptr<void, std::function<void(void *)>> createHashJoinKeyGetter(
    HashJoinKeyMethod method,
    const TiDB::TiDBCollators & collators);

void resetHashJoinKeyGetter(
    HashJoinKeyMethod method,
    std::unique_ptr<void, std::function<void(void *)>> & key_getter,
    const ColumnRawPtrs & key_columns,
    const HashJoinRowLayout & row_layout);

} // namespace DB
