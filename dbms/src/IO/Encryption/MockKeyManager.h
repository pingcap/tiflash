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

#include <IO/FileProvider/KeyManager.h>

#include <vector>

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

class MockKeyManager final : public KeyManager
{
public:
    ~MockKeyManager() override = default;

    explicit MockKeyManager(bool encryption_enabled_ = true);

    MockKeyManager(EncryptionMethod method_, const String & key_, const String & iv, bool encryption_enabled_ = true);

    FileEncryptionInfo getInfo(const EncryptionPath & ep) override;

    FileEncryptionInfo newInfo(const EncryptionPath & ep) override;

    void deleteInfo(const EncryptionPath & ep, bool /*throw_on_error*/) override;

    void linkInfo(const EncryptionPath & src_ep, const EncryptionPath & dst_ep) override;

    bool isEncryptionEnabled(KeyspaceID /*keyspace_id*/) override;

private:
    bool fileExist(const String & fname) const;

private:
    const static EncryptionMethod default_method;
    const static unsigned char default_key[33];
    const static unsigned char default_iv[17];
    std::vector<String> files;

    EncryptionMethod method;
    String key;
    String iv;
    bool encryption_enabled;

    LoggerPtr logger;
};
} // namespace DB
