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

#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/TiKVHelpers/TiKVKeyspaceIDImpl.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB
{
KeyspaceID DecodedTiKVKey::getKeyspaceID() const
{
    return TiKVKeyspaceID::getKeyspaceID(std::string_view(data(), size()));
}

std::string_view DecodedTiKVKey::getUserKey() const
{
    return TiKVKeyspaceID::removeKeyspaceID(std::string_view(data(), size()));
}

std::string DecodedTiKVKey::toDebugString() const
{
    const auto & user_key = getUserKey();
    return Redact::keyToDebugString(user_key.data(), user_key.size());
}

std::string DecodedTiKVKey::makeKeyspacePrefix(KeyspaceID keyspace_id)
{
    return TiKVKeyspaceID::makeKeyspacePrefix(keyspace_id);
}

HandleID RawTiDBPK::getHandleID() const
{
    const auto & pk = *this;
    return RecordKVFormat::decodeInt64(RecordKVFormat::read<UInt64>(pk->data()));
}
} // namespace DB
