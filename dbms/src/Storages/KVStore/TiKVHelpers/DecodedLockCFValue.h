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

#include <Common/Exception.h>
#include <Core/Types.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Read/RegionLockInfo.h>

namespace DB::RecordKVFormat
{

struct DecodedLockCFValue : boost::noncopyable
{
    struct Inner
    {
        std::string_view secondaries;
        std::string_view primary_lock;
        UInt64 lock_version{0};
        UInt64 lock_ttl{0};
        UInt64 txn_size{0};
        UInt64 lock_for_update_ts{0};
        UInt64 min_commit_ts{0};
        UInt64 generation{0}; // For large txn, generation is not zero.
        kvrpcpb::Op lock_type{kvrpcpb::Op_MIN};
        bool use_async_commit{false};
        bool is_txn_file{false};
        /// Set `res` if the `query` could be blocked by this lock. Otherwise `set` res to nullptr.
        void getLockInfoPtr(
            const RegionLockReadQuery & query,
            const std::shared_ptr<const TiKVKey> & key,
            LockInfoPtr & res) const;
        void intoLockInfo(const std::shared_ptr<const TiKVKey> & key, kvrpcpb::LockInfo &) const;
    };
    DecodedLockCFValue(std::shared_ptr<const TiKVKey> key_, std::shared_ptr<const TiKVValue> val_);
    ~DecodedLockCFValue();
    std::unique_ptr<kvrpcpb::LockInfo> intoLockInfo() const;
    void intoLockInfo(kvrpcpb::LockInfo &) const;
    bool isLargeTxn() const;
    void withInner(std::function<void(const Inner &)> && f) const;
    /// Return LockInfoPtr if the `query` could be blocked by this lock. Otherwise return nullptr.
    LockInfoPtr getLockInfoPtr(const RegionLockReadQuery & query) const;
    size_t getSize() const;

    std::shared_ptr<const TiKVKey> key;
    std::shared_ptr<const TiKVValue> val;

private:
    static std::unique_ptr<DecodedLockCFValue::Inner> decodeLockCfValue(const DecodedLockCFValue & decoded);
    // Avoid using shared_ptr to reduce space.
    std::unique_ptr<Inner> inner{nullptr};
};

} // namespace DB::RecordKVFormat
