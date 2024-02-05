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

#include <Encryption/EncryptionPath.h>


namespace DB
{
enum class EncryptionMethod : uint8_t;
struct FileEncryptionInfo;

class KeyManager
{
public:
    virtual ~KeyManager() = default;

    virtual FileEncryptionInfo getInfo(const EncryptionPath & ep) = 0;

    virtual FileEncryptionInfo newInfo(const EncryptionPath & ep) = 0;

    virtual void deleteInfo(const EncryptionPath & ep, bool throw_on_error) = 0;

    virtual void linkInfo(const EncryptionPath & src_ep, const EncryptionPath & dst_ep) = 0;

    virtual bool isEncryptionEnabled(KeyspaceID keyspace_id) = 0;
};

using KeyManagerPtr = std::shared_ptr<KeyManager>;
} // namespace DB
