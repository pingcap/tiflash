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

namespace DB::RecordKVFormat
{

struct DecodedLockCFValue : boost::noncopyable
{
    DecodedLockCFValue(std::shared_ptr<const TiKVKey> key_, std::shared_ptr<const TiKVValue> val_);
    std::unique_ptr<kvrpcpb::LockInfo> intoLockInfo() const;
    void intoLockInfo(kvrpcpb::LockInfo &) const;
    bool isLargeTxn() const;

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
    bool is_txn_file{false};
    // For large txn, generation is not zero.
    UInt64 generation{0};
};

} // namespace DB::RecordKVFormat
