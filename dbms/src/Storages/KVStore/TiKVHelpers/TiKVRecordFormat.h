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

#include <Common/Exception.h>
#include <Core/Types.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Endian.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Decode/TiKVHandle.h>
#include <Storages/KVStore/TiKVHelpers/TiKVVarInt.h>
#include <Storages/KVStore/Types.h>
#include <TiDB/Decode/Datum.h>
#include <TiDB/Decode/DatumCodec.h>
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
enum CFModifyFlag : UInt8
{
    PutFlag = 'P',
    DelFlag = 'D',
    // useless for TiFLASH
    /*
    LockFlag = 'L',
    // In write_cf, only raft leader will use RollbackFlag in txn mode. Learner should ignore it.
    RollbackFlag = 'R',
    */
};

enum UselessCFModifyFlag : UInt8
{
    LockFlag = 'L',
    RollbackFlag = 'R',
};

static const char TABLE_PREFIX = 't';
static const char * RECORD_PREFIX_SEP = "_r";
static const char SHORT_VALUE_PREFIX = 'v';
static const char MIN_COMMIT_TS_PREFIX = 'c';
static const char FOR_UPDATE_TS_PREFIX = 'f';
static const char TXN_SIZE_PREFIX = 't';
static const char ASYNC_COMMIT_PREFIX = 'a';
static const char ROLLBACK_TS_PREFIX = 'r';
static const char FLAG_OVERLAPPED_ROLLBACK = 'R';
static const char GC_FENCE_PREFIX = 'F';
static const char LAST_CHANGE_PREFIX = 'l';
static const char TXN_SOURCE_PREFIX_FOR_WRITE = 'S';
static const char TXN_SOURCE_PREFIX_FOR_LOCK = 's';
static const char PESSIMISTIC_LOCK_WITH_CONFLICT_PREFIX = 'F';

static const size_t SHORT_VALUE_MAX_LEN = 64;

static const size_t RAW_KEY_NO_HANDLE_SIZE = 1 + 8 + 2;
static const size_t RAW_KEY_SIZE = RAW_KEY_NO_HANDLE_SIZE + 8;

// Key format is here:
// https://github.com/tikv/tikv/blob/289ce2ddac505d7883ec616c078e184c00844d17/src/util/codec/bytes.rs#L33-L63
inline TiKVKey encodeAsTiKVKey(const String & ori_str)
{
    WriteBufferFromOwnString ss;
    EncodeBytes(ori_str, ss);
    return TiKVKey(ss.releaseStr());
}

inline UInt64 encodeUInt64(const UInt64 x)
{
    return toBigEndian(x);
}

inline UInt64 encodeInt64(const Int64 x)
{
    return encodeUInt64(static_cast<UInt64>(x) ^ SIGN_MASK);
}

inline UInt64 encodeUInt64Desc(const UInt64 x)
{
    return encodeUInt64(~x);
}

inline UInt64 decodeUInt64(const UInt64 x)
{
    return toBigEndian(x);
}

inline UInt64 decodeUInt64Desc(const UInt64 x)
{
    return ~decodeUInt64(x);
}

inline Int64 decodeInt64(const UInt64 x)
{
    return static_cast<Int64>(decodeUInt64(x) ^ SIGN_MASK);
}

inline void encodeInt64(const Int64 x, WriteBuffer & ss)
{
    auto u = RecordKVFormat::encodeInt64(x);
    ss.write(reinterpret_cast<const char *>(&u), sizeof(u));
}

inline void encodeUInt64(const UInt64 x, WriteBuffer & ss)
{
    auto u = RecordKVFormat::encodeUInt64(x);
    ss.write(reinterpret_cast<const char *>(&u), sizeof(u));
}

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
    return key;
}

inline TiKVKey genKey(const TableID tableId, const HandleID handleId)
{
    return encodeAsTiKVKey(genRawKey(tableId, handleId));
}

inline TiKVKey genKey(const TiDB::TableInfo & table_info, std::vector<Field> keys)
{
    std::string key(RecordKVFormat::RAW_KEY_NO_HANDLE_SIZE, 0);
    memcpy(key.data(), &RecordKVFormat::TABLE_PREFIX, 1);
    auto big_endian_table_id = encodeInt64(table_info.id);
    memcpy(key.data() + 1, reinterpret_cast<const char *>(&big_endian_table_id), 8);
    memcpy(key.data() + 1 + 8, RecordKVFormat::RECORD_PREFIX_SEP, 2);
    WriteBufferFromOwnString ss;

    for (size_t i = 0; i < keys.size(); i++)
    {
        DB::EncodeDatum(
            keys[i],
            table_info.columns[table_info.getPrimaryIndexInfo().idx_cols[i].offset].getCodecFlag(),
            ss);
    }
    return encodeAsTiKVKey(key + ss.releaseStr());
}

inline bool checkKeyPaddingValid(const char * ptr, const UInt8 pad_size)
{
    UInt64 p = (*reinterpret_cast<const UInt64 *>(ptr)) >> ((ENC_GROUP_SIZE - pad_size) * 8);
    return p == 0;
}

inline std::tuple<DecodedTiKVKey, size_t> decodeTiKVKeyFull(const TiKVKey & key)
{
    const size_t chunk_len = ENC_GROUP_SIZE + 1; // 8 + 1
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

inline DecodedTiKVKey decodeTiKVKey(const TiKVKey & key)
{
    return std::get<0>(decodeTiKVKeyFull(key));
}

inline Timestamp getTs(const TiKVKey & key)
{
    return decodeUInt64Desc(read<UInt64>(key.data() + key.dataSize() - 8));
}

template <typename T>
inline TableID getTableId(const T & key)
{
    return decodeInt64(read<UInt64>(key.data() + 1));
}

inline HandleID getHandle(const DecodedTiKVKey & key)
{
    return decodeInt64(read<UInt64>(key.data() + RAW_KEY_NO_HANDLE_SIZE));
}

inline std::string_view getRawTiDBPKView(const DecodedTiKVKey & key)
{
    auto user_key = key.getUserKey();
    return std::string_view(user_key.data() + RAW_KEY_NO_HANDLE_SIZE, user_key.size() - RAW_KEY_NO_HANDLE_SIZE);
}

// DecodedTiKVKey is from decodeTiKVKey.
inline RawTiDBPK getRawTiDBPK(const DecodedTiKVKey & key)
{
    return std::make_shared<const std::string>(getRawTiDBPKView(key));
}


inline TableID getTableId(const TiKVKey & key)
{
    return getTableId(decodeTiKVKey(key));
}

inline HandleID getHandle(const TiKVKey & key)
{
    return getHandle(decodeTiKVKey(key));
}

inline bool isRecord(const DecodedTiKVKey & raw_key)
{
    return raw_key.size() >= RAW_KEY_SIZE && raw_key[0] == TABLE_PREFIX
        && memcmp(raw_key.data() + 9, RECORD_PREFIX_SEP, 2) == 0;
}

inline TiKVKey truncateTs(const TiKVKey & key)
{
    return TiKVKey(String(key.data(), key.dataSize() - sizeof(Timestamp)));
}

inline TiKVKey appendTs(const TiKVKey & key, Timestamp ts)
{
    auto big_endian_ts = encodeUInt64Desc(ts);
    auto str = key.getStr();
    str.append(reinterpret_cast<const char *>(&big_endian_ts), sizeof(big_endian_ts));
    return TiKVKey(std::move(str));
}

// Not begin with 'z'
inline TiKVKey genKey(TableID tableId, HandleID handleId, Timestamp ts)
{
    TiKVKey key = genKey(tableId, handleId);
    return appendTs(key, ts);
}

inline TiKVValue encodeLockCfValue(
    UInt8 lock_type,
    const String & primary,
    Timestamp ts,
    UInt64 ttl,
    const String * short_value = nullptr,
    Timestamp min_commit_ts = 0)
{
    WriteBufferFromOwnString res;
    res.write(lock_type);
    TiKV::writeVarInt(static_cast<Int64>(primary.size()), res);
    res.write(primary.data(), primary.size());
    TiKV::writeVarUInt(ts, res);
    TiKV::writeVarUInt(ttl, res);
    if (short_value)
    {
        res.write(SHORT_VALUE_PREFIX);
        res.write(static_cast<char>(short_value->size()));
        res.write(short_value->data(), short_value->size());
    }
    if (min_commit_ts)
    {
        res.write(MIN_COMMIT_TS_PREFIX);
        encodeUInt64(min_commit_ts, res);
    }
    return TiKVValue(res.releaseStr());
}

struct DecodedLockCFValue : boost::noncopyable
{
    DecodedLockCFValue(std::shared_ptr<const TiKVKey> key_, std::shared_ptr<const TiKVValue> val_);
    std::unique_ptr<kvrpcpb::LockInfo> intoLockInfo() const;
    void intoLockInfo(kvrpcpb::LockInfo &) const;

    std::shared_ptr<const TiKVKey> key;
    std::shared_ptr<const TiKVValue> val;
    UInt64 lock_version{0};
    UInt64 lock_ttl{0};
    UInt64 txn_size{0};
    UInt64 lock_for_update_ts{0};
    kvrpcpb::Op lock_type{kvrpcpb::Op_MIN};
    bool use_async_commit{0};
    UInt64 min_commit_ts{0};
    std::string_view secondaries;
    std::string_view primary_lock;
};

template <typename R = Int64>
inline R readVarInt(const char *& data, size_t & len)
{
    static_assert(std::is_same_v<R, UInt64> || std::is_same_v<R, Int64>);

    R res = 0;
    auto cur = data;
    if constexpr (std::is_same_v<R, UInt64>)
    {
        cur = TiKV::readVarUInt(res, data, len);
    }
    else if constexpr (std::is_same_v<R, Int64>)
    {
        cur = TiKV::readVarInt(res, data, len);
    }
    len -= cur - data, data = cur;
    return res;
}

inline UInt64 readVarUInt(const char *& data, size_t & len)
{
    return readVarInt<UInt64>(data, len);
}

inline UInt8 readUInt8(const char *& data, size_t & len)
{
    UInt8 res = static_cast<UInt8>(*data);
    data += sizeof(UInt8), len -= sizeof(UInt8);
    return res;
}

inline UInt64 readUInt64(const char *& data, size_t & len)
{
    UInt64 res = readBigEndian<UInt64>(data);
    data += sizeof(UInt64), len -= sizeof(UInt64);
    return res;
}

template <typename R>
inline R readRawString(const char *& data, size_t & len, size_t str_len)
{
    R res{};
    if constexpr (!std::is_same_v<R, std::nullptr_t>)
    {
        res = R(data, str_len);
    }
    len -= str_len, data += str_len;
    return res;
}

template <typename R>
inline R readVarString(const char *& data, size_t & len)
{
    auto str_len = readVarInt(data, len);
    return readRawString<R>(data, len, str_len);
}

enum LockType : UInt8
{
    Put = 'P',
    Delete = 'D',
    Lock = 'L',
    Pessimistic = 'S',
};

struct InnerDecodedWriteCFValue
{
    UInt8 write_type;
    Timestamp prewrite_ts;
    std::shared_ptr<const TiKVValue> short_value;
};

typedef std::optional<InnerDecodedWriteCFValue> DecodedWriteCFValue;

inline DecodedWriteCFValue decodeWriteCfValue(const TiKVValue & value)
{
    const char * data = value.data();
    size_t len = value.dataSize();

    auto write_type = RecordKVFormat::readUInt8(data, len); //write type

    bool can_ignore = write_type != CFModifyFlag::DelFlag && write_type != CFModifyFlag::PutFlag;
    if (can_ignore)
        return std::nullopt;

    Timestamp prewrite_ts = RecordKVFormat::readVarUInt(data, len); // ts

    std::string_view short_value;
    while (len)
    {
        auto flag = RecordKVFormat::readUInt8(data, len);
        switch (flag)
        {
        case RecordKVFormat::SHORT_VALUE_PREFIX:
        {
            size_t slen = RecordKVFormat::readUInt8(data, len);
            if (slen > len)
                throw Exception("content len not equal to short value len", ErrorCodes::LOGICAL_ERROR);
            short_value = RecordKVFormat::readRawString<std::string_view>(data, len, slen);
            break;
        }
        case RecordKVFormat::FLAG_OVERLAPPED_ROLLBACK:
            // ignore
            break;
        case RecordKVFormat::GC_FENCE_PREFIX:
            /**
                 * according to https://github.com/tikv/tikv/pull/9207, when meet `GC fence` flag, it is definitely a
                 * rewriting record and there must be a complete row written to tikv, just ignore it in tiflash.
                 */
            return std::nullopt;
        case RecordKVFormat::LAST_CHANGE_PREFIX:
        {
            // Used to accelerate TiKV MVCC scan, useless for TiFlash.
            UInt64 last_change_ts = readUInt64(data, len);
            UInt64 versions_to_last_change = readVarUInt(data, len);
            UNUSED(last_change_ts);
            UNUSED(versions_to_last_change);
            break;
        }
        case RecordKVFormat::TXN_SOURCE_PREFIX_FOR_WRITE:
        {
            // Used for CDC, useless for TiFlash.
            UInt64 txn_source_prefic = readVarUInt(data, len);
            UNUSED(txn_source_prefic);
            break;
        }
        default:
            throw Exception("invalid flag " + std::to_string(flag) + " in write cf", ErrorCodes::LOGICAL_ERROR);
        }
    }

    return InnerDecodedWriteCFValue{
        write_type,
        prewrite_ts,
        short_value.empty() ? nullptr : std::make_shared<const TiKVValue>(short_value.data(), short_value.length())};
}

inline TiKVValue encodeWriteCfValue(
    UInt8 write_type,
    Timestamp ts,
    std::string_view short_value = {},
    bool gc_fence = false)
{
    WriteBufferFromOwnString res;
    res.write(write_type);
    TiKV::writeVarUInt(ts, res);
    if (!short_value.empty())
    {
        res.write(SHORT_VALUE_PREFIX);
        res.write(static_cast<char>(short_value.size()));
        res.write(short_value.data(), short_value.size());
    }
    // just for test
    res.write(FLAG_OVERLAPPED_ROLLBACK);
    if (gc_fence)
    {
        res.write(GC_FENCE_PREFIX);
        encodeUInt64(8888, res);
    }
    return TiKVValue(res.releaseStr());
}

template <bool start>
inline std::string DecodedTiKVKeyToDebugString(const DecodedTiKVKey & decoded_key)
{
    if (decoded_key.size() <= RAW_KEY_NO_HANDLE_SIZE)
    {
        if constexpr (start)
        {
            return "-INF";
        }
        else
        {
            return "+INF";
        }
    }
    return Redact::keyToDebugString(
        decoded_key.data() + RAW_KEY_NO_HANDLE_SIZE,
        decoded_key.size() - RAW_KEY_NO_HANDLE_SIZE);
}

inline std::string DecodedTiKVKeyRangeToDebugString(const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & key_range)
{
    if (unlikely(*key_range.first >= *key_range.second))
        return "[none]";

    return std::string("[") //
        + RecordKVFormat::DecodedTiKVKeyToDebugString<true>(*key_range.first) + ", "
        + RecordKVFormat::DecodedTiKVKeyToDebugString<false>(*key_range.second) //
        + ")";
}

} // namespace RecordKVFormat

} // namespace DB
