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

#include <Common/config.h> // for ENABLE_NEXT_GEN
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <TiDB/Schema/TiDB.h>

namespace DB::RecordKVFormat
{

TiKVKey genKey(const TiDB::TableInfo & table_info, std::vector<Field> keys)
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

TiKVValue encodeLockCfValue(
    UInt8 lock_type,
    const String & primary,
    Timestamp ts,
    UInt64 ttl,
    const String * short_value,
    Timestamp min_commit_ts)
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
#if ENABLE_NEXT_GEN
        TiKV::writeVarUInt(short_value->size(), res);
#else
        res.write(static_cast<char>(short_value->size()));
#endif
        res.write(short_value->data(), short_value->size());
    }
    if (min_commit_ts)
    {
        res.write(MIN_COMMIT_TS_PREFIX);
        encodeUInt64(min_commit_ts, res);
    }
    return TiKVValue(res.releaseStr());
}

DecodedWriteCFValue decodeWriteCfValue(const TiKVValue & value)
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
#if ENABLE_NEXT_GEN
            size_t slen = RecordKVFormat::readVarUInt(data, len);
#else
            size_t slen = RecordKVFormat::readUInt8(data, len);
#endif
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
        short_value.empty() ? nullptr : std::make_shared<const TiKVValue>(short_value.data(), short_value.length()),
    };
}

TiKVValue encodeWriteCfValue(UInt8 write_type, Timestamp ts, std::string_view short_value, bool gc_fence)
{
    WriteBufferFromOwnString res;
    res.write(write_type);
    TiKV::writeVarUInt(ts, res);
    if (!short_value.empty())
    {
        res.write(SHORT_VALUE_PREFIX);
#if ENABLE_NEXT_GEN
        TiKV::writeVarUInt(short_value.size(), res);
#else
        res.write(static_cast<char>(short_value.size()));
#endif
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

} // namespace DB::RecordKVFormat
