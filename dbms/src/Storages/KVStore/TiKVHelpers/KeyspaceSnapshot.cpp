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

#include <Common/Exception.h>
#include <IO/Endian.h>
#include <Storages/KVStore/TiKVHelpers/KeyspaceSnapshot.h>
#include <Storages/KVStore/TiKVHelpers/TiKVKeyspaceIDImpl.h>
#include <pingcap/Exception.h>

#include <magic_enum.hpp>

namespace DB
{
KeyspaceSnapshot::KeyspaceSnapshot(KeyspaceID keyspace_id_, pingcap::kv::Cluster * cluster_, UInt64 version_)
    : snap(cluster_, version_)
{
    if (keyspace_id_ == NullspaceID)
        return;
    prefix = std::string(KEYSPACE_PREFIX_LEN, 0);
    auto id = toBigEndian(keyspace_id_);
    memcpy(prefix.data(), reinterpret_cast<const char *>(&id), sizeof(KeyspaceID));
    prefix[0] = DB::TXN_MODE_PREFIX;
}

std::string KeyspaceSnapshot::Get(const std::string & key)
{
    try
    {
        auto encoded_key = encodeKey(key);
        return snap.Get(encoded_key);
    }
    catch (pingcap::Exception & e)
    {
        // turn into DB::Exception with stack trace
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR,
            "pingcap::Exception code={} msg={}",
            magic_enum::enum_name(static_cast<pingcap::ErrorCodes>(e.code())),
            e.message());
    }
}

kvrpcpb::MvccInfo KeyspaceSnapshot::mvccGet(const std::string & key)
{
    try
    {
        auto encoded_key = encodeKey(key);
        return snap.mvccGet(encoded_key);
    }
    catch (pingcap::Exception & e)
    {
        // turn into DB::Exception with stack trace
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR,
            "pingcap::Exception code={} msg={}",
            magic_enum::enum_name(static_cast<pingcap::ErrorCodes>(e.code())),
            e.message());
    }
}

std::string KeyspaceSnapshot::Get(pingcap::kv::Backoffer & bo, const std::string & key)
{
    try
    {
        auto encoded_key = encodeKey(key);
        return snap.Get(bo, encoded_key);
    }
    catch (pingcap::Exception & e)
    {
        // turn into DB::Exception with stack trace
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR,
            "pingcap::Exception code={} msg={}",
            magic_enum::enum_name(static_cast<pingcap::ErrorCodes>(e.code())),
            e.message());
    }
}

KeyspaceScanner KeyspaceSnapshot::Scan(const std::string & begin, const std::string & end)
{
    try
    {
        auto inner = snap.Scan(encodeKey(begin), encodeKey(end));
        return KeyspaceScanner(inner, /* need_cut_ */ !prefix.empty());
    }
    catch (pingcap::Exception & e)
    {
        // turn into DB::Exception with stack trace
        throw DB::Exception(
            ErrorCodes::LOGICAL_ERROR,
            "pingcap::Exception code={} msg={}",
            magic_enum::enum_name(static_cast<pingcap::ErrorCodes>(e.code())),
            e.message());
    }
}

std::string KeyspaceSnapshot::encodeKey(const std::string & key)
{
    return prefix.empty() ? key : prefix + key;
}

std::string KeyspaceScanner::key()
{
    auto k = Base::key();
    if (need_cut)
        k = k.substr(DB::KEYSPACE_PREFIX_LEN);
    return k;
}
} // namespace DB
