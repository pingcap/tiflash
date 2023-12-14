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
#include <Storages/KVStore/FFI/FileEncryption.h>
#include <openssl/evp.h>
#include <openssl/md5.h>

#include <ext/scope_guard.h>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

inline void AESCTRCipherStream::initIV(uint64_t block_index, unsigned char * iv) const
{
    // In CTR mode, OpenSSL EVP API treat the IV as a 128-bit big-endian, and
    // increase it by 1 for each block.
    uint64_t iv_high = initial_iv_high;
    uint64_t iv_low = initial_iv_low + block_index;
    if (std::numeric_limits<uint64_t>::max() - block_index < initial_iv_low)
    {
        iv_high++;
    }
    iv_high = toBigEndian(iv_high);
    iv_low = toBigEndian(iv_low);
    memcpy(iv, &iv_high, sizeof(uint64_t));
    memcpy(iv + sizeof(uint64_t), &iv_low, sizeof(uint64_t));
}

void AESCTRCipherStream::encrypt(uint64_t file_offset, char * data, size_t data_size)
{
    const size_t block_size = DB::Encryption::blockSize(method);
    uint64_t block_index = file_offset / block_size;
    unsigned char iv[block_size];
    initIV(block_index, iv);
    DB::Encryption::Cipher(file_offset, data, data_size, key, method, iv, /*is_encrypt=*/true);
}

void AESCTRCipherStream::decrypt(uint64_t file_offset, char * data, size_t data_size)
{
    const size_t block_size = DB::Encryption::blockSize(method);
    uint64_t block_index = file_offset / block_size;
    unsigned char iv[block_size];
    initIV(block_index, iv);
    DB::Encryption::Cipher(file_offset, data, data_size, key, method, iv, /*is_encrypt=*/false);
}

BlockAccessCipherStreamPtr AESCTRCipherStream::createCipherStream(
    const FileEncryptionInfo & encryption_info_,
    const EncryptionPath & encryption_path_)
{
    const auto & key = *(encryption_info_.key);
    RUNTIME_CHECK_MSG(key.size() == DB::Encryption::keySize(encryption_info_.method), "Encryption key size mismatch.");
    RUNTIME_CHECK_MSG(
        encryption_info_.iv->size() == DB::Encryption::blockSize(encryption_info_.method),
        "Encryption iv size mismatch.");

    auto iv_high = readBigEndian<uint64_t>(reinterpret_cast<const char *>(encryption_info_.iv->data()));
    auto iv_low
        = readBigEndian<uint64_t>(reinterpret_cast<const char *>(encryption_info_.iv->data() + sizeof(uint64_t)));
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
        MD5(reinterpret_cast<const unsigned char *>(encryption_path_.file_name.c_str()),
            encryption_path_.file_name.size(),
            md5_value);
        auto md5_high = readBigEndian<uint64_t>(reinterpret_cast<const char *>(md5_value));
        auto md5_low = readBigEndian<uint64_t>(reinterpret_cast<const char *>(md5_value + sizeof(uint64_t)));
        iv_high ^= md5_high;
        iv_low ^= md5_low;
    }
    return std::make_shared<AESCTRCipherStream>(encryption_info_.method, key, iv_high, iv_low);
}

} // namespace DB
