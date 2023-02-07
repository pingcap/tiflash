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
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{
// General UniversalPageId Format: Prefix + PageIdU64.
// So normally the size of page id should be larger than 8 bytes(size of PageIdU64).
// If the size of page id is smaller than 8 bytes, it will be regraded as a whole.(Its Prefix is empty, while PageIdU64 is INVALID_PAGE_U64_ID)
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
