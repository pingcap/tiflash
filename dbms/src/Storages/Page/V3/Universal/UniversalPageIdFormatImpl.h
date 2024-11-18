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

#include <IO/Buffer/WriteBuffer.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Endian.h>
#include <IO/WriteHelpers.h>
#include <Storages/KVStore/TiKVHelpers/TiKVKeyspaceIDImpl.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <fmt/format.h>

#include <magic_enum.hpp>

namespace DB
{
// General UniversalPageId Format: Prefix + PageIdU64.
// So normally the size of page id should be larger than 8 bytes(size of PageIdU64).
// If the size of page id is smaller than 8 bytes, it will be regraded as a whole.(Its Prefix is itself, while PageIdU64 is INVALID_PAGE_U64_ID)
//
// Currently, if the PageIdU64 is 0(which is INVALID_PAGE_U64_ID), it may have some special meaning in some cases,
// so please avoid it in the following case:
//  1. if there are ref operations in your workload
//
//
// The main key format types:
// Raft related key format
//  Format: https://github.com/tikv/tikv/blob/9c0df6d68c72d30021b36d24275fdceca9864235/components/keys/src/lib.rs#L24
//  And because some key will be migrated from kv engine to raft engine,
//  kv engine and raft engine may write and delete the same key.
//  So to distinguish data written by kv engine and raft engine,
//  we prepend an `0x01` to the key written by raft engine, and prepend an `0x02` to the key written by kv engine.
//  For example, suppose a key in tikv to be {0x01, 0x02, 0x03}.
//  If it is written by raft engine, then actual key uni ps see will be {0x01, 0x01, 0x02, 0x03}.
//  But if it is written by kv engine, the actual key uni ps see will be {0x02, 0x01, 0x02, 0x03}.
//
// KVStore related key: "kvs" + RegionID
//
// Storage key
//  Meta
//      Prefix = [optional KeyspaceID] + "tm" + NamespaceID
//  Log
//      Prefix = [optional KeyspaceID] + "tl" + NamespaceID
//  Data
//      Prefix = [optional KeyspaceID] + "td" + NamespaceID
//
// KeyspaceID format is the same as in https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md
//  'x'(TXN_MODE_PREFIX) + keyspace_id(3 bytes, big endian)
//  Note 'x' will be a reserved keyword, and should not be used in other prefix.
//  If the first byte of a UniversalPageId is 'x', the next 3 bytes will be considered a KeyspaceID.
//  If not, NullspaceID will be returned.

struct UniversalPageIdFormat
{
public:
    static inline UniversalPageId toFullPageId(const String & prefix, PageIdU64 page_id)
    {
        WriteBufferFromOwnString buff;
        writeString(prefix, buff);
        UniversalPageIdFormat::encodeUInt64(page_id, buff);
        return buff.releaseStr();
    }

    static inline String toFullPrefix(KeyspaceID keyspace_id, StorageType type, NamespaceID ns_id)
    {
        WriteBufferFromOwnString buff;
        if (type != StorageType::KVStore)
        {
            writeString(TiKVKeyspaceID::makeKeyspacePrefix(keyspace_id), buff);
        }
        writeString(getSubPrefix(type), buff);
        if (type != StorageType::KVStore)
        {
            UniversalPageIdFormat::encodeUInt64(ns_id, buff);
        }

        return buff.releaseStr();
    }

    static UniversalPageId toKVStoreKey(UInt64 region_id)
    {
        return toFullPageId(getSubPrefix(StorageType::KVStore), region_id);
    }

    static constexpr char RAFT_PREFIX = 0x01;
    static constexpr char KV_PREFIX = 0x02;
    static constexpr char LOCAL_KV_PREFIX = 0x03;

    // data is in kv engine, so it is prepended by KV_PREFIX
    // KV_PREFIX LOCAL_PREFIX REGION_RAFT_PREFIX region_id APPLY_STATE_SUFFIX
    static UniversalPageId toRaftApplyStateKeyInKVEngine(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(KV_PREFIX, buff);
        writeChar(0x01, buff);
        writeChar(0x02, buff);
        encodeUInt64(region_id, buff);
        writeChar(0x03, buff);
        return buff.releaseStr();
    }

    // RAFT_PREFIX LOCAL_PREFIX REGION_RAFT_PREFIX region_id APPLY_STATE_SUFFIX
    static UniversalPageId toRaftApplyStateKeyInRaftEngine(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(RAFT_PREFIX, buff);
        writeChar(0x01, buff);
        writeChar(0x02, buff);
        encodeUInt64(region_id, buff);
        writeChar(0x03, buff);
        return buff.releaseStr();
    }

    // data is in kv engine, so it is prepended by KV_PREFIX
    // KV_PREFIX LOCAL_PREFIX REGION_META_PREFIX region_id REGION_STATE_SUFFIX
    static UniversalPageId toRegionLocalStateKeyInKVEngine(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(KV_PREFIX, buff);
        writeChar(0x01, buff);
        writeChar(0x03, buff);
        encodeUInt64(region_id, buff);
        writeChar(0x01, buff);
        return buff.releaseStr();
    }

    // RAFT_PREFIX LOCAL_PREFIX REGION_RAFT_PREFIX region_id RAFT_LOG_SUFFIX
    static String toFullRaftLogPrefix(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(RAFT_PREFIX, buff);
        writeChar(0x01, buff);
        writeChar(0x02, buff);
        encodeUInt64(region_id, buff);
        writeChar(0x01, buff);
        return buff.releaseStr();
    }

    enum class LocalKVKeyType : UInt64
    {
        FAPIngestInfo = 1,
        EncryptionKey = 2,
    };

    // LOCAL_PREFIX LocalKVKeyType::FAPIngestInfo region_id
    static String toFAPIngestInfoPageID(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(LOCAL_KV_PREFIX, buff);
        encodeUInt64(static_cast<UInt64>(LocalKVKeyType::FAPIngestInfo), buff);
        encodeUInt64(region_id, buff);
        return buff.releaseStr();
    }

    // LOCAL_PREFIX LocalKVKeyType::EncryptionKey keyspace_id(as 64bits)
    static String toEncryptionKeyPageID(KeyspaceID keyspace_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(LOCAL_KV_PREFIX, buff);
        encodeUInt64(static_cast<UInt64>(LocalKVKeyType::EncryptionKey), buff);
        encodeUInt64(keyspace_id, buff);
        return buff.releaseStr();
    }

    // RAFT_PREFIX LOCAL_PREFIX REGION_RAFT_PREFIX region_id (RAFT_LOG_SUFFIX + 1)
    static String toFullRaftLogScanEnd(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(RAFT_PREFIX, buff);
        writeChar(0x01, buff);
        writeChar(0x02, buff);
        encodeUInt64(region_id, buff);
        writeChar(0x02, buff);
        return buff.releaseStr();
    }

    // RAFT_PREFIX LOCAL_PREFIX REGION_RAFT_PREFIX region_id RAFT_LOG_SUFFIX log_index
    static String toRaftLogKey(RegionID region_id, UInt64 log_index)
    {
        WriteBufferFromOwnString buff;
        writeChar(RAFT_PREFIX, buff);
        writeChar(0x01, buff);
        writeChar(0x02, buff);
        encodeUInt64(region_id, buff);
        writeChar(0x01, buff);
        encodeUInt64(log_index, buff);
        return buff.releaseStr();
    }

    // Store ident //

    // KV_PREFIX LOCAL_PREFIX STORE_IDENT_KEY
    static String getStoreIdentIdInKVEngine()
    {
        WriteBufferFromOwnString buff;
        writeChar(KV_PREFIX, buff);
        writeChar(0x01, buff);
        writeChar(0x01, buff);
        return buff.releaseStr();
    }

    // RAFT_PREFIX LOCAL_PREFIX STORE_IDENT_KEY
    static String getStoreIdentId()
    {
        WriteBufferFromOwnString buff;
        writeChar(RAFT_PREFIX, buff);
        writeChar(0x01, buff);
        writeChar(0x01, buff);
        return buff.releaseStr();
    }

    static inline PageIdU64 getU64ID(const UniversalPageId & page_id)
    {
        if (page_id.size() >= sizeof(UInt64))
            return decodeUInt64(page_id.data() + page_id.size() - sizeof(UInt64));
        else
            return INVALID_PAGE_U64_ID;
    }

    static inline KeyspaceID getKeyspaceID(const UniversalPageId & page_id)
    {
        return TiKVKeyspaceID::getKeyspaceID(std::string_view(page_id.data(), page_id.size()));
    }

    static inline String getFullPrefix(const UniversalPageId & page_id)
    {
        size_t prefix_length = (page_id.size() >= sizeof(UInt64)) ? (page_id.size() - sizeof(UInt64)) : page_id.size();
        return page_id.substr(0, prefix_length).toStr();
    }

    static inline bool isType(const UniversalPageId & page_id, StorageType type)
    {
        const auto & page_id_str = page_id.asStr();
        auto page_id_without_keyspace
            = TiKVKeyspaceID::removeKeyspaceID(std::string_view(page_id_str.data(), page_id_str.size()));
        return page_id_without_keyspace.starts_with(getSubPrefix(type));
    }

    static inline StorageType getUniversalPageIdType(const UniversalPageId & page_id)
    {
        if (page_id.empty())
            return StorageType::Unknown;

        const auto & page_id_str = page_id.asStr();
        if (page_id_str[0] == RAFT_PREFIX)
        {
            return StorageType::RaftEngine;
        }
        else if (page_id_str[0] == KV_PREFIX)
        {
            return StorageType::KVEngine;
        }
        else if (page_id_str[0] == LOCAL_KV_PREFIX)
        {
            return StorageType::LocalKV;
        }
        else
        {
            auto page_id_without_keyspace
                = TiKVKeyspaceID::removeKeyspaceID(std::string_view(page_id_str.data(), page_id_str.size()));
            if (page_id_without_keyspace.starts_with(getSubPrefix(StorageType::Log)))
            {
                return StorageType::Log;
            }
            if (page_id_without_keyspace.starts_with(getSubPrefix(StorageType::Data)))
            {
                return StorageType::Data;
            }
            if (page_id_without_keyspace.starts_with(getSubPrefix(StorageType::Meta)))
            {
                return StorageType::Meta;
            }
            if (page_id_without_keyspace.starts_with(getSubPrefix(StorageType::KVStore)))
            {
                return StorageType::KVStore;
            }
        }
        return StorageType::Unknown;
    }

public:
    static inline void encodeUInt32(const UInt32 x, WriteBuffer & ss)
    {
        auto u = toBigEndian(x);
        ss.write(reinterpret_cast<const char *>(&u), sizeof(u));
    }

    static inline void encodeUInt64(const UInt64 x, WriteBuffer & ss)
    {
        auto u = toBigEndian(x);
        ss.write(reinterpret_cast<const char *>(&u), sizeof(u));
    }

    static inline UInt64 decodeUInt64(const char * s)
    {
        auto v = *(reinterpret_cast<const UInt64 *>(s));
        return toBigEndian(v);
    }

private:
    static String getSubPrefix(StorageType type)
    {
        switch (type)
        {
        case StorageType::Log:
            return "tl";
        case StorageType::Data:
            return "td";
        case StorageType::Meta:
            return "tm";
        case StorageType::KVStore:
            return "kvs";
        default:
            throw Exception(
                fmt::format("Unknown storage type {}", static_cast<UInt8>(type)),
                ErrorCodes::LOGICAL_ERROR);
        }
    }
};
} // namespace DB
