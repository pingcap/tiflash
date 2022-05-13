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

#include <Common/nocopyable.h>
#include <RaftStoreProxyFFI/EncryptionFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>

namespace DB
{

const char * IntoEncryptionMethodName(EncryptionMethod);
struct EngineStoreServerWrap;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#ifndef __clang__
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
struct FileEncryptionInfo : FileEncryptionInfoRaw
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

    FileEncryptionInfo(const FileEncryptionInfoRaw & src)
        : FileEncryptionInfoRaw(src)
    {}
    FileEncryptionInfo(const FileEncryptionRes & res_,
                       const EncryptionMethod & method_,
                       RawCppStringPtr key_,
                       RawCppStringPtr iv_,
                       RawCppStringPtr error_msg_)
        : FileEncryptionInfoRaw{res_, method_, key_, iv_, error_msg_}
    {}
    DISALLOW_COPY(FileEncryptionInfo);
    FileEncryptionInfo(FileEncryptionInfo && src)
    {
        std::memcpy(this, &src, sizeof(src));
        std::memset(&src, 0, sizeof(src));
    }
    FileEncryptionInfo & operator=(FileEncryptionInfo && src)
    {
        if (this == &src)
            return *this;
        this->~FileEncryptionInfo();
        std::memcpy(this, &src, sizeof(src));
        std::memset(&src, 0, sizeof(src));
        return *this;
    }
};
#pragma GCC diagnostic pop

} // namespace DB
