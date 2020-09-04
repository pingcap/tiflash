#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>
#include <IO/Endian.h>
#include <Storages/Transaction/Datum.h>
#include <Storages/Transaction/DatumCodec.h>
#include <Storages/Transaction/TiKVHandle.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <Storages/Transaction/TiKVVarInt.h>
#include <Storages/Transaction/Types.h>
#include <common/likely.h>

#include <sstream>

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
static const char MIN_COMMIT_TS_PREFIX = 'c';
static const char FOR_UPDATE_TS_PREFIX = 'f';
static const char TXN_SIZE_PREFIX = 't';

static const size_t SHORT_VALUE_MAX_LEN = 64;

static const size_t RAW_KEY_NO_HANDLE_SIZE = 1 + 8 + 2;
static const size_t RAW_KEY_SIZE = RAW_KEY_NO_HANDLE_SIZE + 8;

// Key format is here:
// https://docs.google.com/document/d/1J9Dsp8l5Sbvzjth77hK8yx3SzpEJ4SXaR_wIvswRhro/edit
// https://github.com/tikv/tikv/blob/289ce2ddac505d7883ec616c078e184c00844d17/src/util/codec/bytes.rs#L33-L63
inline TiKVKey encodeAsTiKVKey(const String & ori_str)
{
    std::stringstream ss;
    EncodeBytes(ori_str, ss);
    return TiKVKey(ss.str());
}

inline UInt64 encodeUInt64(const UInt64 x) { return toBigEndian(x); }

inline UInt64 encodeInt64(const Int64 x) { return encodeUInt64(static_cast<UInt64>(x) ^ SIGN_MASK); }

inline UInt64 encodeUInt64Desc(const UInt64 x) { return encodeUInt64(~x); }

inline UInt64 decodeUInt64(const UInt64 x) { return toBigEndian(x); }

inline UInt64 decodeUInt64Desc(const UInt64 x) { return ~decodeUInt64(x); }

inline Int64 decodeInt64(const UInt64 x) { return static_cast<Int64>(decodeUInt64(x) ^ SIGN_MASK); }

template <typename T>
inline T read(const char * s)
{
    return *(reinterpret_cast<const T *>(s));
}

inline DecodedTiKVKey genRawKey(const TableID tableId, const HandleID handleId)
{
    std::string key(RecordKVFormat::RAW_KEY_SIZE, 0);
    memcpy(key.data(), &RecordKVFormat::TABLE_PREFIX, 1);
    auto big_endian_table_id = encodeInt64(tableId);
    memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id), 8);
    memcpy(key.data() + 1 + 8, RecordKVFormat::RECORD_PREFIX_SEP, 2);
    auto big_endian_handle_id = encodeInt64(handleId);
    memcpy(key.data() + RAW_KEY_NO_HANDLE_SIZE, reinterpret_cast<const char *>(&big_endian_handle_id), 8);
    return std::move(key);
}

inline TiKVKey genKey(const TableID tableId, const HandleID handleId) { return encodeAsTiKVKey(genRawKey(tableId, handleId)); }

inline TiKVKey genKey(const TiDB::TableInfo & table_info, std::vector<Field> keys)
{
    std::string key(RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, 0);
    memcpy(key.data(), &RecordKVFormat::TABLE_PREFIX, 1);
    auto big_endian_table_id = encodeInt64(table_info.id);
    memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id), 8);
    memcpy(key.data() + 1 + 8, RecordKVFormat::RECORD_PREFIX_SEP, 2);
    std::stringstream ss;
    for (size_t i = 0; i < keys.size(); i++)
    {
        DB::EncodeDatum(keys[i], table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset].getCodecFlag(), ss);
    }
    return encodeAsTiKVKey(key + ss.str());
}

inline bool checkKeyPaddingValid(const char * ptr, const UInt8 pad_size)
{
    UInt64 p = (*reinterpret_cast<const UInt64 *>(ptr)) >> ((ENC_GROUP_SIZE - pad_size) * 8);
    return p == 0;
}

inline std::tuple<DecodedTiKVKey, size_t> decodeTiKVKeyFull(const TiKVKey & key)
{
    const size_t chunk_len = ENC_GROUP_SIZE + 1;
    std::string res;
    res.reserve(key.dataSize() / chunk_len * ENC_GROUP_SIZE);
    for (const char * ptr = key.data();; ptr += chunk_len)
    {
        if (ptr + chunk_len > key.dataSize() + key.data())
            throw Exception("Unexpected eof", ErrorCodes::LOGICAL_ERROR);
        auto marker = (UInt8) * (ptr + ENC_GROUP_SIZE);
        UInt8 pad_size = (ENC_MARKER - marker);
        if (pad_size == 0)
        {
            res.append(ptr, ENC_GROUP_SIZE);
            continue;
        }
        if (pad_size > ENC_GROUP_SIZE)
            throw Exception("Key padding", ErrorCodes::LOGICAL_ERROR);
        res.append(ptr, ENC_GROUP_SIZE - pad_size);

        if (!checkKeyPaddingValid(ptr, pad_size))
            throw Exception("Key padding, wrong end", ErrorCodes::LOGICAL_ERROR);

        // raw string and the offset of remaining string such as timestamp
        return std::make_tuple(std::move(res), ptr - key.data() + chunk_len);
    }
}

inline DecodedTiKVKey decodeTiKVKey(const TiKVKey & key) { return std::get<0>(decodeTiKVKeyFull(key)); }

inline Timestamp getTs(const TiKVKey & key) { return decodeUInt64Desc(read<UInt64>(key.data() + key.dataSize() - 8)); }

inline TableID getTableId(const DecodedTiKVKey & key) { return decodeInt64(read<UInt64>(key.data() + 1)); }

inline HandleID getHandle(const DecodedTiKVKey & key) { return decodeInt64(read<UInt64>(key.data() + RAW_KEY_NO_HANDLE_SIZE)); }

inline RawTiDBPK getRawTiDBPK(const DecodedTiKVKey & key)
{
    return std::make_shared<const std::string>(key.begin() + RAW_KEY_NO_HANDLE_SIZE, key.end());
}

inline TableID getTableId(const TiKVKey & key) { return getTableId(decodeTiKVKey(key)); }

inline HandleID getHandle(const TiKVKey & key) { return getHandle(decodeTiKVKey(key)); }

inline bool isRecord(const DecodedTiKVKey & raw_key)
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

using DecodedLockCFValue = std::tuple<UInt8, String, Timestamp, UInt64, Timestamp>;

inline DecodedLockCFValue decodeLockCfValue(const TiKVValue & value)
{
    UInt8 lock_type;
    String primary;
    Timestamp ts;
    UInt64 ttl = 0;
    Timestamp min_commit_ts = 0;

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
    if (len > 0)
    {
        cur = TiKV::readVarUInt(ttl, data, len); // ttl
        len -= cur - data, data = cur;
        while (len > 0)
        {
            char flag = *data;
            data += 1, len -= 1;
            switch (flag)
            {
                case SHORT_VALUE_PREFIX:
                {
                    size_t slen = static_cast<UInt8>(*data);
                    data += 1, len -= 1;
                    if (len < slen)
                        throw Exception("content len shorter than short value len", ErrorCodes::LOGICAL_ERROR);
                    // no need short value
                    data += slen, len -= slen;
                    break;
                };
                case MIN_COMMIT_TS_PREFIX:
                {
                    min_commit_ts = readBigEndian<UInt64>(data);
                    data += sizeof(UInt64);
                    len -= sizeof(UInt64);
                    break;
                }
                case FOR_UPDATE_TS_PREFIX:
                {
                    readBigEndian<UInt64>(data);
                    data += sizeof(UInt64);
                    len -= sizeof(UInt64);
                    break;
                }
                case TXN_SIZE_PREFIX:
                {
                    readBigEndian<UInt64>(data);
                    data += sizeof(UInt64);
                    len -= sizeof(UInt64);
                    break;
                }
                default:
                {
                    std::string msg = std::string() + "invalid flag " + flag + " in lock value " + value.toHex();
                    throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
                }
            }
        }
    }
    if (len != 0)
        throw Exception("invalid lock value " + value.toHex(), ErrorCodes::LOGICAL_ERROR);

    return std::make_tuple(lock_type, primary, ts, ttl, min_commit_ts);
}

using DecodedWriteCFValue = std::tuple<UInt8, Timestamp, std::shared_ptr<const TiKVValue>>;

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
    size_t slen = static_cast<UInt8>(*data);
    data += 1, len -= 1;
    if (slen != len)
        throw Exception("content len not equal to short value len", ErrorCodes::LOGICAL_ERROR);
    return std::make_tuple(write_type, ts, std::make_shared<const TiKVValue>(data, len));
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

inline std::string DecodedTiKVKeyToHexWithoutTableID(const DecodedTiKVKey & decoded_key)
{
    return ToHex(decoded_key.data() + RAW_KEY_NO_HANDLE_SIZE, decoded_key.size() - RAW_KEY_NO_HANDLE_SIZE);
}

} // namespace RecordKVFormat

} // namespace DB
