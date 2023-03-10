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

#include <IO/Endian.h>
#include <Storages/Transaction/TiKVKeyValue.h>

namespace DB
{
KeyspaceID DecodedTiKVKey::getKeyspaceID() const
{
    if (size() < KEYSPACE_PREFIX_LEN || *begin() != TXN_MODE_PREFIX)
        return NullspaceID;

    char buf[KEYSPACE_PREFIX_LEN];
    memcpy(buf, data(), KEYSPACE_PREFIX_LEN);
    buf[0] = 0;
    return toBigEndian(*reinterpret_cast<const KeyspaceID *>(buf));
}

std::string_view DecodedTiKVKey::getUserKey() const
{
    if (size() < KEYSPACE_PREFIX_LEN || *begin() != TXN_MODE_PREFIX)
        return std::string_view(c_str(), size());

    return std::string_view(c_str() + KEYSPACE_PREFIX_LEN, size() - KEYSPACE_PREFIX_LEN);
}

std::string DecodedTiKVKey::makeKeyspacePrefix(KeyspaceID keyspace_id)
{
    if (keyspace_id == NullspaceID)
        return std::string();
    std::string prefix(KEYSPACE_PREFIX_LEN, 0);
    keyspace_id = toBigEndian(keyspace_id);
    memcpy(prefix.data(), reinterpret_cast<const char *>(&keyspace_id), KEYSPACE_PREFIX_LEN);
    prefix[0] = TXN_MODE_PREFIX;
    return prefix;
}
} // namespace DB