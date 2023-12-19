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

#include <Common/config.h>
#include <Encryption/AESCTRCipher.h>
#include <Encryption/BlockAccessCipherStream.h>
#include <IO/Endian.h>
#include <common/types.h>
#include <openssl/aes.h>

namespace DB
{
struct EncryptionPath;
struct FileEncryptionInfo;

class AESCTRCipherStream : public BlockAccessCipherStream
{
public:
    AESCTRCipherStream(const EncryptionMethod method_, std::string key_, uint64_t iv_high, uint64_t iv_low)
        : method(method_)
        , key(std::move(key_))
        , initial_iv_high(iv_high)
        , initial_iv_low(iv_low)
    {}

    ~AESCTRCipherStream() override = default;

    void encrypt(uint64_t file_offset, char * data, size_t data_size) override;
    void decrypt(uint64_t file_offset, char * data, size_t data_size) override;

    static BlockAccessCipherStreamPtr createCipherStream(
        const FileEncryptionInfo & encryption_info_,
        const EncryptionPath & encryption_path_);

private:
    inline void initIV(uint64_t block_index, unsigned char * iv) const;

    const EncryptionMethod method;
    const std::string key;
    const uint64_t initial_iv_high;
    const uint64_t initial_iv_low;
};
} // namespace DB
