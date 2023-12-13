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

#if (USE_GM_SSL == 0) && !defined(OPENSSL_NO_SM4)
// TODO: OpenSSL Lib does not export SM4_BLOCK_SIZE by now.
// Need to remove SM4_BLOCK_SIZE once Openssl lib support the definition.
// SM4 uses 128-bit block size as AES.
// Ref:
// https://github.com/openssl/openssl/blob/OpenSSL_1_1_1-stable/include/crypto/sm4.h#L24
#define SM4_BLOCK_SIZE 16
#endif


class AESCTRCipherStream : public BlockAccessCipherStream
{
public:
    AESCTRCipherStream(const EncryptionMethod method_, std::string key, uint64_t iv_high, uint64_t iv_low)
        : method(method_)
        , key_(std::move(key))
        , initial_iv_high_(iv_high)
        , initial_iv_low_(iv_low)
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
    const std::string key_; // NOLINT
    const uint64_t initial_iv_high_; // NOLINT
    const uint64_t initial_iv_low_; // NOLINT
};
} // namespace DB
