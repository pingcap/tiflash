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

#include <Storages/KVStore/MultiRaft/RegionCFDataBase.h>
#include <Storages/KVStore/MultiRaft/RegionCFDataTrait.h>
#include <Storages/KVStore/TiKVHelpers/DecodedLockCFValue.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB
{
namespace RecordKVFormat
{

// https://github.com/tikv/tikv/blob/master/components/txn_types/src/lock.rs
[[nodiscard]] DecodedLockCFValue::Inner * decodeLockCfValue(const DecodedLockCFValue & decoded)
{
    auto * inner = new DecodedLockCFValue::Inner{};
    auto & res = *inner;
    const TiKVValue & value = *decoded.val;
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
#if SERVERLESS_PROXY != 0
            case IS_TXN_FILE_PREFIX:
            {
                res.is_txn_file = true;
                break;
            }
#endif
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
            case PESSIMISTIC_LOCK_WITH_CONFLICT_PREFIX:
            {
                // https://github.com/pingcap/tidb/issues/43540
                break;
            }
            case GENERATION_PREFIX:
            {
                res.generation = readUInt64(data, len);
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
    return inner;
}

DecodedLockCFValue::DecodedLockCFValue(std::shared_ptr<const TiKVKey> key_, std::shared_ptr<const TiKVValue> val_)
    : key(std::move(key_))
    , val(std::move(val_))
{
    // TODO do not cache when meets large txn, otherwise, still cache
    // decodeLockCfValue(*this);
}

void DecodedLockCFValue::withInner(std::function<void(const DecodedLockCFValue::Inner &)> f) const
{
    if likely (inner != nullptr)
    {
        f(*inner);
        return;
    }
    // We own a raw pointer here, remember to release it!
    DecodedLockCFValue::Inner * in = decodeLockCfValue(*this);
    f(*in);
    delete in;
}

void DecodedLockCFValue::intoLockInfo(kvrpcpb::LockInfo & res) const
{
    withInner([&](const Inner & in) {
        res.set_lock_type(in.lock_type);
        res.set_primary_lock(in.primary_lock.data(), in.primary_lock.size());
        res.set_lock_version(in.lock_version);
        res.set_lock_ttl(in.lock_ttl);
        res.set_min_commit_ts(in.min_commit_ts);
        res.set_lock_for_update_ts(in.lock_for_update_ts);
        res.set_txn_size(in.txn_size);
        res.set_use_async_commit(in.use_async_commit);
        res.set_key(decodeTiKVKey(*key));
        res.set_is_txn_file(in.is_txn_file);
        if (in.use_async_commit)
        {
            const auto * data = in.secondaries.data();
            auto len = in.secondaries.size();
            UInt64 cnt = readVarUInt(data, len);
            for (UInt64 i = 0; i < cnt; ++i)
            {
                res.add_secondaries(readVarString<std::string>(data, len));
            }
        }
    });
}

std::unique_ptr<kvrpcpb::LockInfo> DecodedLockCFValue::intoLockInfo() const
{
    auto res = std::make_unique<kvrpcpb::LockInfo>();
    intoLockInfo(*res);
    return res;
}

bool DecodedLockCFValue::isLargeTxn() const
{
    auto g = 0;
    withInner([&](const Inner & in) { g = in.generation; });
    return g > 0;
}

UInt64 DecodedLockCFValue::getLockVersion() const
{
    auto x = 0;
    withInner([&](const Inner & in) { x = in.lock_version; });
    return x;
}
UInt64 DecodedLockCFValue::getLockTtl() const
{
    auto x = 0;
    withInner([&](const Inner & in) { x = in.lock_ttl; });
    return x;
}
UInt64 DecodedLockCFValue::getTxnSize() const
{
    auto x = 0;
    withInner([&](const Inner & in) { x = in.txn_size; });
    return x;
}
UInt64 DecodedLockCFValue::getLockForUpdateTs() const
{
    auto x = 0;
    withInner([&](const Inner & in) { x = in.lock_for_update_ts; });
    return x;
}
kvrpcpb::Op DecodedLockCFValue::getLockType() const
{
    kvrpcpb::Op x;
    withInner([&](const Inner & in) { x = in.lock_type; });
    return x;
}

bool DecodedLockCFValue::getUseAsyncCommit() const
{
    auto x = false;
    withInner([&](const Inner & in) { x = in.use_async_commit; });
    return x;
}

UInt64 DecodedLockCFValue::getMinCommitTs() const
{
    auto x = 0;
    withInner([&](const Inner & in) { x = in.min_commit_ts; });
    return x;
}

bool DecodedLockCFValue::getIsTxnFile() const
{
    auto x = false;
    withInner([&](const Inner & in) { x = in.is_txn_file; });
    return x;
}

UInt64 DecodedLockCFValue::getGeneration() const
{
    auto x = 0;
    withInner([&](const Inner & in) { x = in.generation; });
    return x;
}

} // namespace RecordKVFormat
} // namespace DB
