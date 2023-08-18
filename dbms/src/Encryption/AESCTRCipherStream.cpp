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
#include <Encryption/AESCTRCipherStream.h>
#include <Encryption/KeyManager.h>
#include <Storages/Transaction/FileEncryption.h>

#include <cassert>
#include <cstddef>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

size_t KeySize(EncryptionMethod method)
{
    switch (method)
    {
    case EncryptionMethod::Aes128Ctr:
        return 16;
    case EncryptionMethod::Aes192Ctr:
        return 24;
    case EncryptionMethod::Aes256Ctr:
        return 32;
    default:
        return 0;
    }
}

void AESCTRCipherStream::cipher(uint64_t file_offset, char * data, size_t data_size, bool is_encrypt)
{
#if OPENSSL_VERSION_NUMBER < 0x01000200f
    (void)file_offset;
    (void)data;
    (void)data_size;
    (void)is_encrypt;
    throw Exception("OpenSSL version < 1.0.2", ErrorCodes::NOT_IMPLEMENTED);
#else
    int ret = 1;
    EVP_CIPHER_CTX * ctx = nullptr;
    InitCipherContext(ctx);
    if (ctx == nullptr)
    {
        throw DB::TiFlashException("Failed to create cipher context.", Errors::Encryption::Internal);
    }

    uint64_t block_index = file_offset / AES_BLOCK_SIZE;
    uint64_t block_offset = file_offset % AES_BLOCK_SIZE;

    // In CTR mode, OpenSSL EVP API treat the IV as a 128-bit big-endian, and
    // increase it by 1 for each block.
    uint64_t iv_high = initial_iv_high_;
    uint64_t iv_low = initial_iv_low_ + block_index;
    if (std::numeric_limits<uint64_t>::max() - block_index < initial_iv_low_)
    {
        iv_high++;
    }
    iv_high = toBigEndian(iv_high);
    iv_low = toBigEndian(iv_low);
    unsigned char iv[AES_BLOCK_SIZE];
    memcpy(iv, &iv_high, sizeof(uint64_t));
    memcpy(iv + sizeof(uint64_t), &iv_low, sizeof(uint64_t));

    ret = EVP_CipherInit(ctx, cipher_, reinterpret_cast<const unsigned char *>(key_.data()), iv, (is_encrypt ? 1 : 0));
    if (ret != 1)
    {
        throw DB::TiFlashException("Failed to create cipher context.", Errors::Encryption::Internal);
    }

    // Disable padding. After disabling padding, data size should always be
    // multiply of block size.
    ret = EVP_CIPHER_CTX_set_padding(ctx, 0);
    if (ret != 1)
    {
        throw DB::TiFlashException("Failed to disable padding for cipher context.", Errors::Encryption::Internal);
    }

    uint64_t data_offset = 0;
    size_t remaining_data_size = data_size;
    int output_size = 0;
    unsigned char partial_block[AES_BLOCK_SIZE];

    // In the following we assume EVP_CipherUpdate allow in and out buffer are
    // the same, to save one memcpy. This is not specified in official man page.

    // Handle partial block at the beginning. The partial block is copied to
    // buffer to fake a full block.
    if (block_offset > 0)
    {
        size_t partial_block_size = std::min<size_t>(AES_BLOCK_SIZE - block_offset, remaining_data_size);
        memcpy(partial_block + block_offset, data, partial_block_size);
        ret = EVP_CipherUpdate(ctx, partial_block, &output_size, partial_block, AES_BLOCK_SIZE);
        if (ret != 1)
        {
            throw DB::TiFlashException(
                "Crypter failed for first block, offset " + std::to_string(file_offset),
                Errors::Encryption::Internal);
        }
        if (output_size != AES_BLOCK_SIZE)
        {
            throw DB::TiFlashException("Unexpected crypter output size for first block, expected " + std::to_string(AES_BLOCK_SIZE)
                                           + " vs actual " + std::to_string(output_size),
                                       Errors::Encryption::Internal);
        }
        memcpy(data, partial_block + block_offset, partial_block_size);
        data_offset += partial_block_size;
        remaining_data_size -= partial_block_size;
    }

    // Handle full blocks in the middle.
    if (remaining_data_size >= AES_BLOCK_SIZE)
    {
        size_t actual_data_size = remaining_data_size - remaining_data_size % AES_BLOCK_SIZE;
        unsigned char * full_blocks = reinterpret_cast<unsigned char *>(data) + data_offset;
        ret = EVP_CipherUpdate(ctx, full_blocks, &output_size, full_blocks, static_cast<int>(actual_data_size));
        if (ret != 1)
        {
            throw DB::TiFlashException(
                "Crypter failed at offset " + std::to_string(file_offset + data_offset),
                Errors::Encryption::Internal);
        }
        if (output_size != static_cast<int>(actual_data_size))
        {
            throw DB::TiFlashException("Unexpected crypter output size, expected " + std::to_string(actual_data_size) + " vs actual "
                                           + std::to_string(output_size),
                                       Errors::Encryption::Internal);
        }
        data_offset += actual_data_size;
        remaining_data_size -= actual_data_size;
    }

    // Handle partial block at the end. The partial block is copied to buffer to
    // fake a full block.
    if (remaining_data_size > 0)
    {
        assert(remaining_data_size < AES_BLOCK_SIZE);
        memcpy(partial_block, data + data_offset, remaining_data_size);
        ret = EVP_CipherUpdate(ctx, partial_block, &output_size, partial_block, AES_BLOCK_SIZE);
        if (ret != 1)
        {
            throw DB::TiFlashException(
                "Crypter failed for last block, offset " + std::to_string(file_offset + data_offset),
                Errors::Encryption::Internal);
        }
        if (output_size != AES_BLOCK_SIZE)
        {
            throw DB::TiFlashException("Unexpected crypter output size for last block, expected " + std::to_string(AES_BLOCK_SIZE)
                                           + " vs actual " + std::to_string(output_size),
                                       Errors::Encryption::Internal);
        }
        memcpy(data + data_offset, partial_block, remaining_data_size);
    }
    FreeCipherContext(ctx);
#endif
}

BlockAccessCipherStreamPtr AESCTRCipherStream::createCipherStream(
    const FileEncryptionInfo & encryption_info_,
    const EncryptionPath & encryption_path_)
{
    const auto & key = *(encryption_info_.key);

    const EVP_CIPHER * cipher = nullptr;
    switch (encryption_info_.method)
    {
    case EncryptionMethod::Aes128Ctr:
        cipher = EVP_aes_128_ctr();
        break;
    case EncryptionMethod::Aes192Ctr:
        cipher = EVP_aes_192_ctr();
        break;
    case EncryptionMethod::Aes256Ctr:
        cipher = EVP_aes_256_ctr();
        break;
    default:
        throw DB::TiFlashException("Unsupported encryption method: " + std::to_string(static_cast<int>(encryption_info_.method)),
                                   Errors::Encryption::Internal);
    }
    if (key.size() != KeySize(encryption_info_.method))
    {
        throw DB::TiFlashException("Encryption key size mismatch. " + std::to_string(key.size()) + "(actual) vs. "
                                       + std::to_string(KeySize(encryption_info_.method)) + "(expected).",
                                   Errors::Encryption::Internal);
    }
    if (encryption_info_.iv->size() != AES_BLOCK_SIZE)
    {
        throw DB::TiFlashException("iv size not equal to block cipher block size: " + std::to_string(encryption_info_.iv->size())
                                       + "(actual) vs. " + std::to_string(AES_BLOCK_SIZE) + "(expected).",
                                   Errors::Encryption::Internal);
    }
    auto iv_high = readBigEndian<uint64_t>(reinterpret_cast<const char *>(encryption_info_.iv->data()));
    auto iv_low = readBigEndian<uint64_t>(reinterpret_cast<const char *>(encryption_info_.iv->data() + sizeof(uint64_t)));
    // Currently all encryption info are stored in one file called file.dict.
    // Every update of file.dict will sync the whole file.
    // So when the file is too large, the update cost increases.
    // To keep the file size as small as possible, we reuse the encryption info among a group of related files.(e.g. the files of a DMFile)
    // For security reason, the same `iv` is not allowed to encrypt two different files,
    // so we combine the `iv` fetched from file.dict with the hash value of the file name to calculate the real `iv` for every file.
    if (!encryption_path_.file_name.empty())
    {
        unsigned char md5_value[MD5_DIGEST_LENGTH];
        static_assert(MD5_DIGEST_LENGTH == sizeof(uint64_t) * 2);
        MD5((unsigned char *)encryption_path_.file_name.c_str(), encryption_path_.file_name.size(), md5_value);
        auto md5_high = readBigEndian<uint64_t>(reinterpret_cast<const char *>(md5_value));
        auto md5_low = readBigEndian<uint64_t>(reinterpret_cast<const char *>(md5_value + sizeof(uint64_t)));
        iv_high ^= md5_high;
        iv_low ^= md5_low;
    }
    return std::make_shared<AESCTRCipherStream>(cipher, key, iv_high, iv_low);
}

} // namespace DB
