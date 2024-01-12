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

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <Encryption/KeyManager.h>
#include <Encryption/MasterKey.h>
#include <Poco/Path.h>
#include <Storages/Page/PageStorage_fwd.h>
#include <common/likely.h>

namespace DB
{

struct EngineStoreServerWrap;

/// CSEDataKeyManager is a KeyManager implementation that designed for Cloud Storage Engine.
/// It will store all encryption keys which have been encrypted using MasterKey in PageStorage.
class CSEDataKeyManager : public KeyManager
{
public:
    static constexpr UInt64 ENCRYPTION_KEY_RESERVED_NAMESPACE_ID = std::numeric_limits<UInt64>::max();
    static constexpr UInt64 ENCRYPTION_KEY_RESERVED_PAGEU64_ID = std::numeric_limits<UInt64>::max();

    explicit CSEDataKeyManager(
        EngineStoreServerWrap * tiflash_instance_wrap_,
        std::weak_ptr<UniversalPageStorage> & ps_write_);

    ~CSEDataKeyManager() override = default;

    FileEncryptionInfo getInfo(const EncryptionPath & ep) override;

    FileEncryptionInfo newInfo(const EncryptionPath & ep) override;

    void deleteInfo(const EncryptionPath & ep, bool throw_on_error) override;

    // Note: This function will not be used.
    void linkInfo(const EncryptionPath & /*src_cp*/, const EncryptionPath & /*dst_cp*/) override;

    // delete the keyspace encryption key
    void deleteKey(KeyspaceID keyspace_id);

private:
    EngineStoreServerWrap * tiflash_instance_wrap;
    // Note: it is a reference of a weak_ptr point to UniversalPageStorage
    std::weak_ptr<UniversalPageStorage> & ps_write;
    // cache, keyspace_id -> encryption_key
    std::unordered_map<KeyspaceID, EncryptionKey> keyspace_id_to_key;
    const MasterKeyPtr master_key;
};

} // namespace DB
