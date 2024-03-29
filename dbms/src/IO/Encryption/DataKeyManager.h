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
#include <IO/FileProvider/KeyManager.h>

namespace DB
{
struct EngineStoreServerWrap;
class DataKeyManager : public KeyManager
{
public:
    explicit DataKeyManager(EngineStoreServerWrap * tiflash_instance_wrap_);

    ~DataKeyManager() override = default;

    FileEncryptionInfo getInfo(const EncryptionPath & ep) override;

    FileEncryptionInfo newInfo(const EncryptionPath & ep) override;

    void deleteInfo(const EncryptionPath & ep, bool throw_on_error) override;

    void linkInfo(const EncryptionPath & src_ep, const EncryptionPath & dst_ep) override;

    bool isEncryptionEnabled(KeyspaceID keyspace_id) override;

private:
    EngineStoreServerWrap * tiflash_instance_wrap;
};
} // namespace DB
