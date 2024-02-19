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

#include <Common/Exception.h>
#include <Common/TiFlashException.h>
#include <IO/Encryption/AESCTRCipher.h>
#include <openssl/err.h>
#include <openssl/evp.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>

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
    if ((ctx) != nullptr)                   \
    {                                       \
        if (EVP_CIPHER_CTX_reset(ctx) != 1) \
        {                                   \
            (ctx) = nullptr;                \
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

namespace DB::Encryption
{

size_t keySize(EncryptionMethod method)
{
    switch (method)
    {
    case EncryptionMethod::Aes128Ctr:
        return 16;
    case EncryptionMethod::Aes192Ctr:
        return 24;
    case EncryptionMethod::Aes256Ctr:
        return 32;
    case EncryptionMethod::SM4Ctr:
        return 16;
    default:
        return 0;
    }
}

size_t blockSize(EncryptionMethod method)
{
    switch (method)
    {
    case EncryptionMethod::Aes128Ctr:
    case EncryptionMethod::Aes192Ctr:
    case EncryptionMethod::Aes256Ctr:
        return AES_BLOCK_SIZE;
    case EncryptionMethod::SM4Ctr:
#if USE_GM_SSL
        // SM4 uses 128-bit block size as AES.
        // Ref: https://github.com/openssl/openssl/blob/OpenSSL_1_1_1-stable/include/crypto/sm4.h#L24
        return 16;
#elif OPENSSL_VERSION_NUMBER < 0x1010100fL || defined(OPENSSL_NO_SM4)
        throw DB::TiFlashException(
            Errors::Encryption::Internal,
            "Do not support encryption method SM4Ctr when using OpenSSL and the version is less than 1.1.1");
#else
        // Openssl support SM4 after 1.1.1 release version.
        return SM4_BLOCK_SIZE;
#endif
    default:
        return 0;
    }
}

const EVP_CIPHER * getCipher(EncryptionMethod method)
{
    switch (method)
    {
    case EncryptionMethod::Aes128Ctr:
        return EVP_aes_128_ctr();
    case EncryptionMethod::Aes192Ctr:
        return EVP_aes_192_ctr();
    case EncryptionMethod::Aes256Ctr:
        return EVP_aes_256_ctr();
    case EncryptionMethod::SM4Ctr:
#if USE_GM_SSL
        // Use sm4 in GmSSL, return nullptr
        return nullptr;
#elif OPENSSL_VERSION_NUMBER < 0x1010100fL || defined(OPENSSL_NO_SM4)
        throw DB::TiFlashException(
            Errors::Encryption::Internal,
            "Do not support encryption method SM4Ctr when using OpenSSL and the version is less than 1.1.1");
#else
        // Openssl support SM4 after 1.1.1 release version.
        return EVP_sm4_ctr();
#endif
    default:
        throw DB::TiFlashException(
            Errors::Encryption::Internal,
            "Unsupported encryption method: {}",
            static_cast<int>(method));
    }
}

void OpenSSLCipher(
    uint64_t file_offset,
    char * data,
    size_t data_size,
    String key,
    EncryptionMethod method,
    const unsigned char * iv,
    bool is_encrypt)
{
#if OPENSSL_VERSION_NUMBER < 0x01000200f
    (void)file_offset;
    (void)data;
    (void)data_size;
    (void)key;
    (void)method;
    (void)iv;
    (void)is_encrypt;
    throw Exception("OpenSSL version < 1.0.2", ErrorCodes::NOT_IMPLEMENTED);
#else
    const EVP_CIPHER * cipher = getCipher(method);
    RUNTIME_CHECK_MSG(cipher != nullptr, "Cipher is not valid, method={}", magic_enum::enum_name(method));

    const size_t block_size = blockSize(method);
    uint64_t block_offset = file_offset % block_size;

    uint64_t data_offset = 0;
    size_t remaining_data_size = data_size;
    int output_size = 0;
    unsigned char partial_block[block_size];

    int ret = 1;
    EVP_CIPHER_CTX * ctx = nullptr;
    InitCipherContext(ctx);
    RUNTIME_CHECK_MSG(ctx != nullptr, "Failed to create cipher context.");
    SCOPE_EXIT({ FreeCipherContext(ctx); });

    ret = EVP_CipherInit(ctx, cipher, reinterpret_cast<const unsigned char *>(key.data()), iv, (is_encrypt ? 1 : 0));
    RUNTIME_CHECK_MSG(ret == 1, "Failed to create cipher context.");

    // Disable padding. After disabling padding, data size should always be
    // multiply of block size.
    ret = EVP_CIPHER_CTX_set_padding(ctx, 0);
    RUNTIME_CHECK_MSG(ret == 1, "Failed to disable padding for cipher context.");

    // In the following we assume the encrypt/decrypt process allow in and out buffer are
    // the same, to save one memcpy. This is not specified in official man page.

    // Handle partial block at the beginning. The partial block is copied to
    // buffer to fake a full block.
    if (block_offset > 0)
    {
        size_t partial_block_size = std::min<size_t>(block_size - block_offset, remaining_data_size);
        memcpy(partial_block + block_offset, data, partial_block_size);

        ret = EVP_CipherUpdate(ctx, partial_block, &output_size, partial_block, block_size);
        RUNTIME_CHECK_MSG(ret == 1, "Cipher failed for first block, offset {}.", file_offset);
        RUNTIME_CHECK_MSG(
            output_size == static_cast<int>(block_size),
            "Unexpected cipher output size for first block, expected {} actual {}",
            block_size,
            output_size);
        memcpy(data, partial_block + block_offset, partial_block_size);
        data_offset += partial_block_size;
        remaining_data_size -= partial_block_size;
    }

    // Handle full blocks in the middle.
    if (remaining_data_size >= block_size)
    {
        size_t actual_data_size = remaining_data_size - remaining_data_size % block_size;
        unsigned char * full_blocks = reinterpret_cast<unsigned char *>(data) + data_offset;
        ret = EVP_CipherUpdate(ctx, full_blocks, &output_size, full_blocks, static_cast<int>(actual_data_size));
        RUNTIME_CHECK_MSG(ret == 1, "Cipher failed for offset {}.", file_offset + data_offset);
        RUNTIME_CHECK_MSG(
            output_size == static_cast<int>(actual_data_size),
            "Unexpected cipher output size for block, expected {} actual {}",
            actual_data_size,
            output_size);
        data_offset += actual_data_size;
        remaining_data_size -= actual_data_size;
    }

    // Handle partial block at the end. The partial block is copied to buffer to
    // fake a full block.
    if (remaining_data_size > 0)
    {
        assert(remaining_data_size < block_size);
        memcpy(partial_block, data + data_offset, remaining_data_size);
        ret = EVP_CipherUpdate(ctx, partial_block, &output_size, partial_block, block_size);
        RUNTIME_CHECK_MSG(ret == 1, "Cipher failed for last block, offset {}.", file_offset + data_offset);
        RUNTIME_CHECK_MSG(
            output_size == static_cast<int>(block_size),
            "Unexpected cipher output size for last block, expected {} actual {}",
            block_size,
            output_size);
        memcpy(data + data_offset, partial_block, remaining_data_size);
    }
#endif
}

#if USE_GM_SSL
void GMSSLSM4Cipher(
    uint64_t file_offset,
    char * data,
    size_t data_size,
    String key,
    unsigned char * iv,
    bool is_encrypt)
{
    SM4_KEY sm4_key;
    // set key for sm4
    sm4_set_encrypt_key(&sm4_key, reinterpret_cast<const uint8_t *>(key.c_str()));
    const size_t block_size = blockSize(EncryptionMethod::SM4Ctr);
    uint64_t block_offset = file_offset % block_size;

    uint64_t data_offset = 0;
    size_t remaining_data_size = data_size;
    unsigned char partial_block[block_size];

    // In the following we assume the encrypt/decrypt process allow in and out buffer are
    // the same, to save one memcpy. This is not specified in official man page.

    // Handle partial block at the beginning. The partial block is copied to
    // buffer to fake a full block.
    if (block_offset > 0)
    {
        size_t partial_block_size = std::min<size_t>(block_size - block_offset, remaining_data_size);
        memcpy(partial_block + block_offset, data, partial_block_size);
        if (is_encrypt)
            sm4_ctr_encrypt(&sm4_key, iv, partial_block, block_size, partial_block);
        else
            sm4_ctr_decrypt(&sm4_key, iv, partial_block, block_size, partial_block);
        memcpy(data, partial_block + block_offset, partial_block_size);
        data_offset += partial_block_size;
        remaining_data_size -= partial_block_size;
    }

    // Handle full blocks in the middle.
    if (remaining_data_size >= block_size)
    {
        size_t actual_data_size = remaining_data_size - remaining_data_size % block_size;
        unsigned char * full_blocks = reinterpret_cast<unsigned char *>(data) + data_offset;
        if (is_encrypt)
            sm4_ctr_encrypt(&sm4_key, iv, full_blocks, actual_data_size, full_blocks);
        else
            sm4_ctr_decrypt(&sm4_key, iv, full_blocks, actual_data_size, full_blocks);
        data_offset += actual_data_size;
        remaining_data_size -= actual_data_size;
    }

    // Handle partial block at the end. The partial block is copied to buffer to
    // fake a full block.
    if (remaining_data_size > 0)
    {
        assert(remaining_data_size < block_size);
        memcpy(partial_block, data + data_offset, remaining_data_size);
        if (is_encrypt)
            sm4_ctr_encrypt(&sm4_key, iv, partial_block, block_size, partial_block);
        else
            sm4_ctr_decrypt(&sm4_key, iv, partial_block, block_size, partial_block);
        memcpy(data + data_offset, partial_block, remaining_data_size);
    }
}
#endif

void Cipher(
    uint64_t file_offset,
    char * data,
    size_t data_size,
    String key,
    EncryptionMethod method,
    unsigned char * iv,
    bool is_encrypt)
{
    // Only if use GmSSL and method is SM4Ctr, use GMSSLSM4Cipher, otherwise use OpenSSLCipher
#if USE_GM_SSL
    if (method == EncryptionMethod::SM4Ctr)
        return GMSSLSM4Cipher(file_offset, data, data_size, key, iv, is_encrypt);
#endif
    OpenSSLCipher(file_offset, data, data_size, key, method, iv, is_encrypt);
}

} // namespace DB::Encryption
