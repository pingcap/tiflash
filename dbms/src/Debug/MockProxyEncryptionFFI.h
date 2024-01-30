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
#include <pingcap/pd/Types.h>

#include <string>
#include <unordered_set>


namespace DB
{

using String = std::string;
using KeyspaceID = pingcap::pd::KeyspaceID;

struct MockProxyEncryptionFFI
{
    MockProxyEncryptionFFI(bool encryption_enabled_, String master_key_)
        : encryption_enabled(encryption_enabled_)
        , master_key(master_key_)
    {}

    void addKeyspace(KeyspaceID keyspace_id) { keyspace_ids.insert(keyspace_id); }

    bool checkEncryptionEnabled() const { return encryption_enabled; }
    String getMasterKey() const { return master_key; }
    bool getKeyspaceEncryption(uint32_t keyspace_id) const { return keyspace_ids.contains(keyspace_id); }

    const bool encryption_enabled;
    const String master_key;
    std::unordered_set<KeyspaceID> keyspace_ids;
};

} // namespace DB
