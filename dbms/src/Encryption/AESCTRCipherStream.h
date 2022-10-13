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

#include <Common/config.h>
#include <Encryption/BlockAccessCipherStream.h>
#include <IO/Endian.h>

#if USE_GM_SSL
#include <gmssl/sm4.h>
#endif

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/md5.h>

namespace DB
{
struct EncryptionPath;

#if OPENSSL_VERSION_NUMBER < 0x01010000f

#define InitCipherContext(ctx) \
    EVP_CIPHER_CTX ctx##_var;  \
    ctx = &ctx##_var;          \
    EVP_CIPHER_CTX_init(ctx);

// do nothing
#define FreeCipherContext(ctx)

#else

#define InitCipherContext(ctx)              \
    ctx = EVP_CIPHER_CTX_new();             \
    if (ctx != nullptr)                     \
    {                                       \
        if (EVP_CIPHER_CTX_reset(ctx) != 1) \
        {                                   \
            ctx = nullptr;                  \
        }                                   \
    }

#define FreeCipherContext(ctx) EVP_CIPHER_CTX_free(ctx);

#endif


#if (USE_GM_SSL == 0) && !defined(OPENSSL_NO_SM4)
// TODO: OpenSSL Lib does not export SM4_BLOCK_SIZE by now.
// Need to remove SM4_BLOCK_SIZE once Openssl lib support the definition.
// SM4 uses 128-bit block size as AES.
// Ref:
// https://github.com/openssl/openssl/blob/OpenSSL_1_1_1-stable/include/crypto/sm4.h#L24
#define SM4_BLOCK_SIZE 16
#endif

struct FileEncryptionInfo;

class AESCTRCipherStream : public BlockAccessCipherStream
{
public:
    AESCTRCipherStream(const EVP_CIPHER * cipher, std::string key, uint64_t iv_high, uint64_t iv_low)
        : cipher_(cipher)
        , key_(std::move(key))
        , initial_iv_high_(iv_high)
        , initial_iv_low_(iv_low)
    {
#if USE_GM_SSL
        if (cipher == nullptr)
        {
            // use sm4 in GmSSL
            sm4_set_encrypt_key(&sm4_key_, reinterpret_cast<const uint8_t *>(key_.c_str()));
        }
#endif
    }

    ~AESCTRCipherStream() override = default;

    size_t blockSize() override
    {
#if defined(SM4_BLOCK_SIZE)
        static_assert(SM4_BLOCK_SIZE == AES_BLOCK_SIZE);
#endif
        return AES_BLOCK_SIZE; // 16
    }

    void encrypt(uint64_t file_offset, char * data, size_t data_size) override
    {
        cipher(file_offset, data, data_size, true /*is_encrypt*/);
    }

    void decrypt(uint64_t file_offset, char * data, size_t data_size) override
    {
        cipher(file_offset, data, data_size, false /*is_encrypt*/);
    }

    static BlockAccessCipherStreamPtr createCipherStream(
        const FileEncryptionInfo & encryption_info_,
        const EncryptionPath & encryption_path_);

private:
    void cipher(uint64_t file_offset, char * data, size_t data_size, bool is_encrypt);

    inline void initIV(uint64_t block_index, unsigned char * iv) const;

    const EVP_CIPHER * cipher_;
    const std::string key_;
    const uint64_t initial_iv_high_;
    const uint64_t initial_iv_low_;
#if USE_GM_SSL
    SM4_KEY sm4_key_;
#endif
};
} // namespace DB
