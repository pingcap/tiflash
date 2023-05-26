// Copyright 2023 PingCAP, Ltd.
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

#include <Storages/Transaction/RegionCFDataBase.h>
#include <Storages/Transaction/RegionCFDataTrait.h>
#include <Storages/Transaction/TiKVRecordFormat.h>

namespace DB
{
namespace RecordKVFormat
{
// https://github.com/tikv/tikv/blob/master/components/txn_types/src/lock.rs
inline void decodeLockCfValue(DecodedLockCFValue & res)
{
    const TiKVValue & value = *res.val;
    const char * data = value.data();
    size_t len = value.dataSize();

    kvrpcpb::Op lock_type = kvrpcpb::Op_MIN;
    switch (readUInt8(data, len))
    {
    case LockType::Put:
        lock_type = kvrpcpb::Op::Put;
        break;
    case LockType::Delete:
        lock_type = kvrpcpb::Op::Del;
        break;
    case LockType::Lock:
        lock_type = kvrpcpb::Op::Lock;
        break;
    case LockType::Pessimistic:
        lock_type = kvrpcpb::Op::PessimisticLock;
        break;
    }
    res.lock_type = lock_type;
    res.primary_lock = readVarString<std::string_view>(data, len);
    res.lock_version = readVarUInt(data, len);

    if (len > 0)
    {
        res.lock_ttl = readVarUInt(data, len);
        while (len > 0)
        {
            char flag = readUInt8(data, len);
            switch (flag)
            {
            case SHORT_VALUE_PREFIX:
            {
                size_t str_len = readUInt8(data, len);
                if (len < str_len)
                    throw Exception("content len shorter than short value len", ErrorCodes::LOGICAL_ERROR);
                // no need short value
                readRawString<std::nullptr_t>(data, len, str_len);
                break;
            };
            case MIN_COMMIT_TS_PREFIX:
            {
                res.min_commit_ts = readUInt64(data, len);
                break;
            }
            case FOR_UPDATE_TS_PREFIX:
            {
                res.lock_for_update_ts = readUInt64(data, len);
                break;
            }
            case TXN_SIZE_PREFIX:
            {
                res.txn_size = readUInt64(data, len);
                break;
            }
            case ASYNC_COMMIT_PREFIX:
            {
                res.use_async_commit = true;
                const auto * start = data;
                UInt64 cnt = readVarUInt(data, len);
                for (UInt64 i = 0; i < cnt; ++i)
                {
                    readVarString<std::nullptr_t>(data, len);
                }
                const auto * end = data;
                res.secondaries = {start, static_cast<size_t>(end - start)};
                break;
            }
            case ROLLBACK_TS_PREFIX:
            {
                UInt64 cnt = readVarUInt(data, len);
                for (UInt64 i = 0; i < cnt; ++i)
                {
                    readUInt64(data, len);
                }
                break;
            }
            case LAST_CHANGE_PREFIX:
            {
                // Used to accelerate TiKV MVCC scan, useless for TiFlash.
                UInt64 last_change_ts = readUInt64(data, len);
                UInt64 versions_to_last_change = readVarUInt(data, len);
                UNUSED(last_change_ts);
                UNUSED(versions_to_last_change);
                break;
            }
            case TXN_SOURCE_PREFIX_FOR_LOCK:
            {
                // Used for CDC, useless for TiFlash.
                UInt64 txn_source_prefic = readVarUInt(data, len);
                UNUSED(txn_source_prefic);
                break;
            }
            default:
            {
                std::string msg = std::string("invalid flag ") + flag + " in lock value " + value.toDebugString();
                throw Exception(msg, ErrorCodes::LOGICAL_ERROR);
            }
            }
        }
    }
    if (len != 0)
        throw Exception("invalid lock value " + value.toDebugString(), ErrorCodes::LOGICAL_ERROR);
}

DecodedLockCFValue::DecodedLockCFValue(std::shared_ptr<const TiKVKey> key_, std::shared_ptr<const TiKVValue> val_)
    : key(std::move(key_))
    , val(std::move(val_))
{
    decodeLockCfValue(*this);
}

void DecodedLockCFValue::intoLockInfo(kvrpcpb::LockInfo & res) const
{
    res.set_lock_type(lock_type);
    res.set_primary_lock(primary_lock.data(), primary_lock.size());
    res.set_lock_version(lock_version);
    res.set_lock_ttl(lock_ttl);
    res.set_min_commit_ts(min_commit_ts);
    res.set_lock_for_update_ts(lock_for_update_ts);
    res.set_txn_size(txn_size);
    res.set_use_async_commit(use_async_commit);
    res.set_key(decodeTiKVKey(*key));

    if (use_async_commit)
    {
        const auto * data = secondaries.data();
        auto len = secondaries.size();
        UInt64 cnt = readVarUInt(data, len);
        for (UInt64 i = 0; i < cnt; ++i)
        {
            res.add_secondaries(readVarString<std::string>(data, len));
        }
    }
}

std::unique_ptr<kvrpcpb::LockInfo> DecodedLockCFValue::intoLockInfo() const
{
    auto res = std::make_unique<kvrpcpb::LockInfo>();
    intoLockInfo(*res);
    return res;
}

} // namespace RecordKVFormat
} // namespace DB