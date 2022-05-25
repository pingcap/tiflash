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

#include <Encryption/KeyManager.h>

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

    FileEncryptionInfo getFile(const String & fname) override;

    FileEncryptionInfo newFile(const String & fname) override;

    void deleteFile(const String & fname, bool /*throw_on_error*/) override;

    void linkFile(const String & src_fname, const String & dst_fname) override;

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
