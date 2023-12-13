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

#include <Common/TiFlashException.h>
#include <Common/nocopyable.h>
#include <Encryption/BlockAccessCipherStream.h>
#include <Encryption/EncryptionPath.h>
#include <RaftStoreProxyFFI/EncryptionFFI.h>
#include <Storages/KVStore/FFI/ProxyFFICommon.h>


namespace DB
{

const char * IntoEncryptionMethodName(EncryptionMethod);
struct EngineStoreServerWrap;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#ifndef __clang__
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif

/// FileEncryptionInfo hold the encryption info of a file, the key is plain text.
/// Warning: Never expose the key.
struct FileEncryptionInfo : private FileEncryptionInfoRaw
{
    ~FileEncryptionInfo()
    {
        if (key)
        {
            delete key;
            key = nullptr;
        }
        if (iv)
        {
            delete iv;
            iv = nullptr;
        }
        if (error_msg)
        {
            delete error_msg;
            error_msg = nullptr;
        }
    }

    explicit FileEncryptionInfo(const FileEncryptionInfoRaw & src)
        : FileEncryptionInfoRaw(src)
    {}
    FileEncryptionInfo(
        const FileEncryptionRes & res_,
        const EncryptionMethod & method_,
        RawCppStringPtr key_,
        RawCppStringPtr iv_,
        RawCppStringPtr error_msg_)
        : FileEncryptionInfoRaw{res_, method_, key_, iv_, error_msg_}
    {}
    DISALLOW_COPY(FileEncryptionInfo);
    FileEncryptionInfo(FileEncryptionInfo && src)
        : FileEncryptionInfoRaw()
    {
        std::memcpy(this, &src, sizeof(src)); // NOLINT
        std::memset(&src, 0, sizeof(src)); // NOLINT
    }
    FileEncryptionInfo & operator=(FileEncryptionInfo && src)
    {
        if (this == &src)
            return *this;
        this->~FileEncryptionInfo();
        std::memcpy(this, &src, sizeof(src)); // NOLINT
        std::memset(&src, 0, sizeof(src)); // NOLINT
        return *this;
    }

    BlockAccessCipherStreamPtr createCipherStream(const EncryptionPath & encryption_path) const;

    bool isValid() const { return (res == FileEncryptionRes::Ok || res == FileEncryptionRes::Disabled); }
    // FileEncryptionRes::Disabled means encryption feature has never been enabled, so no file will be encrypted.
    bool isEncrypted() const { return (res != FileEncryptionRes::Disabled && method != EncryptionMethod::Plaintext); }

    std::string getErrorMsg() const { return error_msg ? std::string(*error_msg) : ""; }
};
#pragma GCC diagnostic pop

} // namespace DB
