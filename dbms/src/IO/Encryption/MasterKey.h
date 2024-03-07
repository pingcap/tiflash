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

#include <Common/RandomData.h>
#include <Common/TiFlashException.h>
#include <IO/Encryption/AESCTRCipher.h>
#include <IO/Encryption/EncryptionKey.h>
#include <common/types.h>

namespace DB
{

class MasterKey
{
public:
    explicit MasterKey(String master_key_)
        : master_key(std::move(master_key_))
    {}

    EncryptionKeyPtr generateEncryptionKey()
    {
        String plain_text = DB::random::randomString(EncryptionKey::KEY_LENGTH);
        String cipher_text = plain_text;
        DB::Encryption::Cipher(
            0,
            cipher_text.data(),
            EncryptionKey::KEY_LENGTH,
            master_key,
            EncryptionMethod::Aes256Ctr,
            nullptr,
            true);
        return std::make_shared<EncryptionKey>(cipher_text, plain_text, 0);
    }

    EncryptionKeyPtr decryptEncryptionKey(const String & exported)
    {
        RUNTIME_CHECK_MSG(
            exported.size() == EncryptionKey::EXPORT_KEY_LENGTH,
            "Invalid exported key length: {}, expected: {}",
            exported.size(),
            EncryptionKey::EXPORT_KEY_LENGTH);
        char method = exported[0];
        RUNTIME_CHECK_MSG(
            method == static_cast<char>(EncryptionMethod::Aes256Ctr),
            "Invalid encryption method: {}, expected: {}",
            static_cast<int>(method),
            static_cast<int>(EncryptionMethod::Aes256Ctr));
        auto current_ver = readBigEndian<UInt32>(exported.data() + 1);
        String cipher_text = exported.substr(1 + 4, EncryptionKey::KEY_LENGTH);
        String plain_text = cipher_text;
        DB::Encryption::Cipher(
            0,
            plain_text.data(),
            EncryptionKey::KEY_LENGTH,
            master_key,
            EncryptionMethod::Aes256Ctr,
            nullptr,
            false);
        return std::make_shared<EncryptionKey>(cipher_text, plain_text, current_ver);
    }

private:
    String master_key; // never expose this key
};

using MasterKeyPtr = std::unique_ptr<MasterKey>;

} // namespace DB
