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
// Raft related key format
//  Format: https://github.com/tikv/tikv/blob/9c0df6d68c72d30021b36d24275fdceca9864235/components/keys/src/lib.rs#L24
//  And to distinguish data written by kv engine and raft engine, we prepend an `0x01` to the key written by kv engine.
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

enum class StorageType
{
    Log = 1,
    Data = 2,
    Meta = 3,
    KVStore = 4,
};

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

    // data is in kv engine, so it has another `0x01` prefix
    // 0x01 0x01 0x02 region_id 0x03
    static UniversalPageId toRaftApplyStateKeyInKVEngine(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(0x01, buff);
        writeChar(0x01, buff);
        writeChar(0x02, buff);
        encodeUInt64(region_id, buff);
        writeChar(0x03, buff);
        return buff.releaseStr();
    }

    // data is in kv engine, so it has another `0x01` prefix
    // 0x01 0x01 0x03 region_id 0x01
    static UniversalPageId toRegionLocalStateKeyInKVEngine(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(0x01, buff);
        writeChar(0x01, buff);
        writeChar(0x03, buff);
        encodeUInt64(region_id, buff);
        writeChar(0x01, buff);
        return buff.releaseStr();
    }

    // 0x01 0x02 region_id 0x01
    static String toFullRaftLogPrefix(UInt64 region_id)
    {
        WriteBufferFromOwnString buff;
        writeChar(0x01, buff);
        writeChar(0x02, buff);
        encodeUInt64(region_id, buff);
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

template <>
struct fmt::formatter<DB::UniversalPageId>
{
    static constexpr auto parse(format_parse_context & ctx) -> decltype(ctx.begin())
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();
        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("invalid format");
        return it;
    }

    template <typename FormatContext>
    auto format(const DB::UniversalPageId & value, FormatContext & ctx) const -> decltype(ctx.out())
    {
        auto prefix = DB::UniversalPageIdFormat::getFullPrefix(value);
        return format_to(ctx.out(), "0x{}.{}", Redact::keyToHexString(prefix.data(), prefix.size()), DB::UniversalPageIdFormat::getU64ID(value));
    }
};
