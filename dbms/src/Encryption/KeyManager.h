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

size_t keySize(EncryptionMethod method);

class KeyManager
{
public:
    virtual ~KeyManager() = default;

    virtual FileEncryptionInfo getFile(const String & fname) = 0;

    virtual FileEncryptionInfo newFile(const String & fname) = 0;

    virtual void deleteFile(const String & fname, bool throw_on_error) = 0;

    virtual void linkFile(const String & src_fname, const String & dst_fname) = 0;
};

using KeyManagerPtr = std::shared_ptr<KeyManager>;
} // namespace DB
