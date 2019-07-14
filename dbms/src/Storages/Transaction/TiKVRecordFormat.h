#pragma once

#include <sstream>

#include <common/likely.h>

#include <Common/Exception.h>
#include <Core/Types.h>
#include <IO/Endian.h>

#include <Storages/Transaction/Codec.h>
#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/TiKVVarInt.h>
#include <Storages/Transaction/Types.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace RecordKVFormat
{

static const char TABLE_PREFIX = 't';
static const char * RECORD_PREFIX_SEP = "_r";
static const char SHORT_VALUE_PREFIX = 'v';

static const size_t SHORT_VALUE_MAX_LEN = 64;

static const UInt64 SIGN_MARK = UInt64(1) << 63;

static const size_t RAW_KEY_NO_HANDLE_SIZE = 1 + 8 + 2;
static const size_t RAW_KEY_SIZE = RAW_KEY_NO_HANDLE_SIZE + 8;

inline std::vector<Field> DecodeRow(const TiKVValue & value)
{
    std::vector<Field> vec;
    const String & raw_value = value.getStr();
    size_t cursor = 0;
    while (cursor < raw_value.size())
    {
        vec.push_back(DecodeDatum(cursor, raw_value));
    }

    if (cursor != raw_value.size())
        throw Exception("DecodeRow cursor is not end", ErrorCodes::LOGICAL_ERROR);

    return vec;
}

// Key format is here:
// https://docs.google.com/document/d/1J9Dsp8l5Sbvzjth77hK8yx3SzpEJ4SXaR_wIvswRhro/edit
// https://github.com/tikv/tikv/blob/289ce2ddac505d7883ec616c078e184c00844d17/src/util/codec/bytes.rs#L33-L63
inline void encodeAsTiKVKey(const String & ori_str, std::stringstream & ss) { EncodeBytes(ori_str, ss); }

inline TiKVKey encodeAsTiKVKey(const String & ori_str)
{
    std::stringstream ss;
    encodeAsTiKVKey(ori_str, ss);
    return TiKVKey(ss.str());
}

inline UInt64 encodeUInt64(const UInt64 x) { return toBigEndian(x); }

inline UInt64 encodeInt64(const Int64 x) { return encodeUInt64(static_cast<UInt64>(x) ^ SIGN_MARK); }

inline UInt64 encodeUInt64Desc(const UInt64 x) { return encodeUInt64(~x); }

inline UInt64 decodeUInt64(const UInt64 x) { return toBigEndian(x); }

inline UInt64 decodeUInt64Desc(const UInt64 x) { return ~decodeUInt64(x); }

inline Int64 decodeInt64(const UInt64 x) { return static_cast<Int64>(decodeUInt64(x) ^ SIGN_MARK); }

inline TiKVValue EncodeRow(const TiDB::TableInfo & table_info, const std::vector<Field> & fields)
{
    if (table_info.columns.size() != fields.size())
        throw Exception("Encoding row has different sizes between columns and values", ErrorCodes::LOGICAL_ERROR);
    std::stringstream ss;
    for (size_t i = 0; i < fields.size(); i++)
    {
        const TiDB::ColumnInfo & column = table_info.columns[i];
        EncodeDatum(Field(column.id), TiDB::CodecFlagInt, ss);
        EncodeDatum(fields[i], column.getCodecFlag(), ss);
    }
    return TiKVValue(ss.str());
}

template <typename T>
inline T read(const char * s)
{
    return *(reinterpret_cast<const T *>(s));
}

inline String genRawKey(const TableID tableId, const HandleID handleId)
{
    String key(RecordKVFormat::RAW_KEY_SIZE, 0);
    memcpy(key.data(), &RecordKVFormat::TABLE_PREFIX, 1);
    auto big_endian_table_id = encodeInt64(tableId);
    memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id), 8);
    memcpy(key.data() + 1 + 8, RecordKVFormat::RECORD_PREFIX_SEP, 2);
    auto big_endian_handle_id = encodeInt64(handleId);
    memcpy(key.data() + RAW_KEY_NO_HANDLE_SIZE, reinterpret_cast<const char *>(&big_endian_handle_id), 8);
    return key;
}

inline TiKVKey genKey(const TableID tableId, const HandleID handleId) { return encodeAsTiKVKey(genRawKey(tableId, handleId)); }

inline std::tuple<String, size_t> decodeTiKVKeyFull(const TiKVKey & key)
{
    std::stringstream res;
    const char * ptr = key.data();
    const size_t chunk_len = ENC_GROUP_SIZE + 1;
    for (const char * next_ptr = ptr;; next_ptr += chunk_len)
    {
        ptr = next_ptr;
        if (ptr + chunk_len > key.dataSize() + key.data())
            throw Exception("Unexpected eof", ErrorCodes::LOGICAL_ERROR);
        auto marker = (UInt8) * (ptr + ENC_GROUP_SIZE);
        size_t pad_size = (ENC_MARKER - marker);
        if (pad_size == 0)
        {
            res.write(ptr, ENC_GROUP_SIZE);
            continue;
        }
        if (pad_size > ENC_GROUP_SIZE)
            throw Exception("Key padding", ErrorCodes::LOGICAL_ERROR);
        res.write(ptr, ENC_GROUP_SIZE - pad_size);
        for (const char * p = ptr + ENC_GROUP_SIZE - pad_size; p < ptr + ENC_GROUP_SIZE; ++p)
        {
            if (*p != 0)
                throw Exception("Key padding, wrong end", ErrorCodes::LOGICAL_ERROR);
        }
        // raw string and the offset of remaining string such as timestamp
        return std::make_tuple(res.str(), ptr - key.data() + chunk_len);
    }
}

inline String decodeTiKVKey(const TiKVKey & key) { return std::get<0>(decodeTiKVKeyFull(key)); }

inline Timestamp getTs(const TiKVKey & key) { return decodeUInt64Desc(read<UInt64>(key.data() + key.dataSize() - 8)); }

inline TableID getTableId(const String & key) { return decodeInt64(read<UInt64>(key.data() + 1)); }

inline HandleID getHandle(const String & key) { return decodeInt64(read<UInt64>(key.data() + RAW_KEY_NO_HANDLE_SIZE)); }

inline TableID getTableId(const TiKVKey & key) { return getTableId(decodeTiKVKey(key)); }

inline HandleID getHandle(const TiKVKey & key) { return getHandle(decodeTiKVKey(key)); }

inline bool isRecord(const String & raw_key)
{
    return raw_key.size() >= RAW_KEY_SIZE && raw_key[0] == TABLE_PREFIX && memcmp(raw_key.data() + 9, RECORD_PREFIX_SEP, 2) == 0;
}

inline TiKVKey truncateTs(const TiKVKey & key) { return TiKVKey(String(key.data(), key.dataSize() - sizeof(Timestamp))); }

inline TiKVKey appendTs(const TiKVKey & key, Timestamp ts)
{
    auto big_endian_ts = encodeUInt64Desc(ts);
    auto str = key.getStr();
    str.append(reinterpret_cast<const char *>(&big_endian_ts), sizeof(big_endian_ts));
    return TiKVKey(std::move(str));
}

inline TiKVKey genKey(TableID tableId, HandleID handleId, Timestamp ts)
{
    TiKVKey key = genKey(tableId, handleId);
    return appendTs(key, ts);
}

inline TiKVValue internalEncodeLockCfValue(UInt8 lock_type, const String & primary, Timestamp ts, UInt64 ttl, const String * short_value)
{
    std::stringstream res;
    res.put(lock_type);
    TiKV::writeVarInt(static_cast<Int64>(primary.size()), res);
    res.write(primary.data(), primary.size());
    TiKV::writeVarUInt(ts, res);
    TiKV::writeVarUInt(ttl, res);
    if (short_value)
    {
        res.put(SHORT_VALUE_PREFIX);
        res.put(static_cast<char>(short_value->size()));
        res.write(short_value->data(), short_value->size());
    }
    return TiKVValue(res.str());
}


inline TiKVValue encodeLockCfValue(UInt8 lock_type, const String & primary, Timestamp ts, UInt64 ttl, const String & short_value)
{
    return internalEncodeLockCfValue(lock_type, primary, ts, ttl, &short_value);
}


inline TiKVValue encodeLockCfValue(UInt8 lock_type, const String & primary, Timestamp ts, UInt64 ttl)
{
    return internalEncodeLockCfValue(lock_type, primary, ts, ttl, nullptr);
}

using DecodedLockCFValue = std::tuple<UInt8, String, Timestamp, UInt64, std::shared_ptr<TiKVValue>>;

inline DecodedLockCFValue decodeLockCfValue(const TiKVValue & value)
{
    UInt8 lock_type;
    String primary;
    UInt64 ts;
    UInt64 ttl = 0;

    const char * data = value.data();
    size_t len = value.dataSize();
    lock_type = static_cast<UInt8>(*data);
    data += 1, len -= 1; //lock type
    Int64 primary_len = 0;
    auto cur = TiKV::readVarInt(primary_len, data, len); // primary
    len -= cur - data, data = cur;
    primary.append(data, static_cast<size_t>(primary_len));
    len -= primary_len, data += primary_len;
    cur = TiKV::readVarUInt(ts, data, len); // ts
    len -= cur - data, data = cur;
    if (len == 0)
        return std::make_tuple(lock_type, primary, ts, ttl, nullptr);
    cur = TiKV::readVarUInt(ttl, data, len); // ttl
    len -= cur - data, data = cur;
    if (len == 0)
        return std::make_tuple(lock_type, primary, ts, ttl, nullptr);
    char flag = *data;
    data += 1, len -= 1; //  SHORT_VALUE_PREFIX
    assert(flag == SHORT_VALUE_PREFIX);
    (void)flag;
    auto slen = (size_t)*data;
    data += 1, len -= 1;
    assert(len == slen);
    (void)slen;
    return std::make_tuple(lock_type, primary, ts, ttl, std::make_shared<TiKVValue>(data, len));
}

using DecodedWriteCFValue = std::tuple<UInt8, Timestamp, std::shared_ptr<TiKVValue>>;

inline DecodedWriteCFValue decodeWriteCfValue(const TiKVValue & value)
{
    const char * data = value.data();
    size_t len = value.dataSize();

    auto write_type = static_cast<UInt8>(*data);
    data += 1, len -= 1; //write type

    Timestamp ts;
    const char * res = TiKV::readVarUInt(ts, data, len);
    len -= res - data, data = res; // ts

    if (len == 0)
        return std::make_tuple(write_type, ts, nullptr);
    assert(*data == SHORT_VALUE_PREFIX);
    data += 1, len -= 1;
    auto slen = (size_t)*data;
    data += 1, len -= 1;
    if (slen != len)
        throw Exception("unexpected eof.", ErrorCodes::LOGICAL_ERROR);
    return std::make_tuple(write_type, ts, std::make_shared<TiKVValue>(data, len));
}


inline TiKVValue internalEncodeWriteCfValue(UInt8 write_type, Timestamp ts, const String * short_value)
{
    std::stringstream res;
    res.put(write_type);
    TiKV::writeVarUInt(ts, res);
    if (short_value)
    {
        res.put(SHORT_VALUE_PREFIX);
        res.put(static_cast<char>(short_value->size()));
        res.write(short_value->data(), short_value->size());
    }
    return TiKVValue(res.str());
}


inline TiKVValue encodeWriteCfValue(UInt8 write_type, Timestamp ts, const String & short_value)
{
    return internalEncodeWriteCfValue(write_type, ts, &short_value);
}


inline TiKVValue encodeWriteCfValue(UInt8 write_type, Timestamp ts) { return internalEncodeWriteCfValue(write_type, ts, nullptr); }

} // namespace RecordKVFormat

} // namespace DB
