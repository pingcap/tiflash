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

#include <Common/Logger.h>
#include <Common/RedactHelpers.h>
#include <Common/TiFlashException.h>
#include <IO/Encryption/AESCTRCipherStream.h>
#include <IO/Endian.h>
#include <Storages/KVStore/FFI/FileEncryption.h>
#include <common/logger_useful.h>
#include <openssl/md5.h>

#include <magic_enum.hpp>


namespace DB
{

BlockAccessCipherStreamPtr FileEncryptionInfo::createCipherStream(
    const EncryptionPath & encryption_path,
    bool is_new_created_info) const
{
    // If the encryption info is newly created, we should check the res to make sure the encryption is enabled.
    if (is_new_created_info && res == FileEncryptionRes::Disabled)
        return nullptr;

    RUNTIME_CHECK_MSG(res != FileEncryptionRes::Error, "Failed to get encryption info, error message: {}", *error_msg);
    if (method == EncryptionMethod::Plaintext || method == EncryptionMethod::Unknown)
        return nullptr;

    const String & encryption_key = *key;
    RUNTIME_CHECK_MSG(
        encryption_key.size() == DB::Encryption::keySize(method),
        "Encryption key size mismatch, method: {}, key size: {}, expected size: {}.",
        magic_enum::enum_name(method),
        encryption_key.size(),
        DB::Encryption::keySize(method));
    RUNTIME_CHECK_MSG(
        iv->size() == DB::Encryption::blockSize(method),
        "Encryption iv size mismatch, method: {}, iv size: {}, expected size: {}.",
        magic_enum::enum_name(method),
        iv->size(),
        DB::Encryption::blockSize(method));
    auto iv_high = readBigEndian<uint64_t>(reinterpret_cast<const char *>(iv->data()));
    auto iv_low = readBigEndian<uint64_t>(reinterpret_cast<const char *>(iv->data() + sizeof(uint64_t)));
    LOG_DEBUG(
        DB::Logger::get("ffff"),
        "createCipherStream, before,"
        "enc_info.full_path/file_name={}/{}"
        " key={} iv={}",
        encryption_path.full_path,
        encryption_path.file_name,
        Redact::keyToHexString(reinterpret_cast<char *>(key->data()), key->size()),
        Redact::keyToHexString(iv->data(), iv->size()));
    // Currently all encryption info are stored in one file called file.dict.
    // Every update of file.dict will sync the whole file.
    // So when the file is too large, the update cost increases.
    // To keep the file size as small as possible, we reuse the encryption info among a group of related files.(e.g. the files of a DMFile)
    // For security reason, the same `iv` is not allowed to encrypt two different files,
    // so we combine the `iv` fetched from file.dict with the hash value of the file name to calculate the real `iv` for every file.
    if (!encryption_path.file_name.empty())
    {
        unsigned char md5_value[MD5_DIGEST_LENGTH];
        static_assert(MD5_DIGEST_LENGTH == sizeof(uint64_t) * 2);
        MD5(reinterpret_cast<const unsigned char *>(encryption_path.file_name.c_str()),
            encryption_path.file_name.size(),
            md5_value);
        auto md5_high = readBigEndian<uint64_t>(reinterpret_cast<const char *>(md5_value));
        auto md5_low = readBigEndian<uint64_t>(reinterpret_cast<const char *>(md5_value + sizeof(uint64_t)));
        iv_high ^= md5_high;
        iv_low ^= md5_low;
    }
    return std::make_shared<AESCTRCipherStream>(method, encryption_key, iv_high, iv_low);
}

template <FileEncryptionInfo::Operation op>
void FileEncryptionInfo::cipherData(char * data, size_t data_size) const
{
    if (res == FileEncryptionRes::Disabled || method == EncryptionMethod::Plaintext
        || method == EncryptionMethod::Unknown)
        return;
    RUNTIME_CHECK_MSG(res != FileEncryptionRes::Error, "Failed to get encryption info, error message: {}", *error_msg);

    const String & encryption_key = *key;
    RUNTIME_CHECK_MSG(
        encryption_key.size() == DB::Encryption::keySize(method),
        "Encryption key size mismatch, method: {}, key size: {}, expected size: {}.",
        magic_enum::enum_name(method),
        encryption_key.size(),
        DB::Encryption::keySize(method));
    RUNTIME_CHECK_MSG(
        iv->size() == DB::Encryption::blockSize(method),
        "Encryption iv size mismatch, method: {}, iv size: {}, expected size: {}.",
        magic_enum::enum_name(method),
        iv->size(),
        DB::Encryption::blockSize(method));
    DB::Encryption::Cipher(
        0,
        data,
        data_size,
        encryption_key,
        method,
        reinterpret_cast<unsigned char *>(iv->data()),
        /*is_encrypt*/ op == Encrypt);
}

template void FileEncryptionInfo::cipherData<FileEncryptionInfo::Encrypt>(char * data, size_t data_size) const;
template void FileEncryptionInfo::cipherData<FileEncryptionInfo::Decrypt>(char * data, size_t data_size) const;

} // namespace DB
