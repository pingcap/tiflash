// Copyright 2022 PingCAP, Ltd.
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

#include <IO/Endian.h>
#include <IO/WriteBuffer.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <fmt/format.h>

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
// Raft related key
//  Format: https://github.com/tikv/tikv/blob/9c0df6d68c72d30021b36d24275fdceca9864235/components/keys/src/lib.rs#L24
//
// KVStore related key
//  Prefix = [optional prefix] + "kvs"
//
// Storage key
//  Meta
//      Prefix = [optional prefix] + "tm" + NamespaceId
//  Log
//      Prefix = [optional prefix] + "tl" + NamespaceId
//  Data
//      Prefix = [optional prefix] + "td" + NamespaceId

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

    static inline String toSubPrefix(StorageType type)
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
            throw Exception(fmt::format("Unknown storage type {}", static_cast<UInt8>(type)), ErrorCodes::LOGICAL_ERROR);
        }
    }

    static inline String toFullPrefix(StorageType type, NamespaceId ns_id)
    {
        WriteBufferFromOwnString buff;
        writeString(toSubPrefix(type), buff);
        if (type != StorageType::KVStore)
        {
            UniversalPageIdFormat::encodeUInt64(ns_id, buff);
        }
        return buff.releaseStr();
    }

    static inline PageIdU64 getU64ID(const UniversalPageId & page_id)
    {
        if (page_id.size() >= sizeof(UInt64))
            return decodeUInt64(page_id.data() + page_id.size() - sizeof(UInt64));
        else
            return INVALID_PAGE_U64_ID;
    }

    static inline String getFullPrefix(const UniversalPageId & page_id)
    {
        size_t prefix_length = (page_id.size() >= sizeof(UInt64)) ? (page_id.size() - sizeof(UInt64)) : page_id.size();
        return page_id.substr(0, prefix_length).toStr();
    }

private:
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
};
} // namespace DB
