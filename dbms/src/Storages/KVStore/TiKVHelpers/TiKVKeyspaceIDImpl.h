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

#include <IO/Endian.h>
#include <Storages/KVStore/Types.h>
#include <common/defines.h>

namespace DB
{
static const size_t KEYSPACE_PREFIX_LEN = 4;
static const char TXN_MODE_PREFIX = 'x';

struct TiKVKeyspaceID
{
    ALWAYS_INLINE static inline KeyspaceID getKeyspaceID(const std::string_view & data)
    {
        if (data.size() < KEYSPACE_PREFIX_LEN || data[0] != TXN_MODE_PREFIX)
            return NullspaceID;

        char buf[KEYSPACE_PREFIX_LEN];
        memcpy(buf, data.data(), KEYSPACE_PREFIX_LEN);
        buf[0] = 0;
        return toBigEndian(*reinterpret_cast<const KeyspaceID *>(buf));
    }

    ALWAYS_INLINE static inline std::string_view removeKeyspaceID(const std::string_view & data)
    {
        if (data.size() < KEYSPACE_PREFIX_LEN || data[0] != TXN_MODE_PREFIX)
            return data;

        return std::string_view(data.data() + KEYSPACE_PREFIX_LEN, data.size() - KEYSPACE_PREFIX_LEN);
    }

    ALWAYS_INLINE static inline String makeKeyspacePrefix(KeyspaceID keyspace_id)
    {
        if (keyspace_id == NullspaceID)
            return std::string();
        std::string prefix(KEYSPACE_PREFIX_LEN, 0);
        keyspace_id = toBigEndian(keyspace_id);
        memcpy(prefix.data(), reinterpret_cast<const char *>(&keyspace_id), KEYSPACE_PREFIX_LEN);
        prefix[0] = TXN_MODE_PREFIX;
        return prefix;
    }
};
} // namespace DB
