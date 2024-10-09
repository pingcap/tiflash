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
#include <IO/Encryption/AESCTRCipherStream.h>
#include <IO/Endian.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

inline void AESCTRCipherStream::initIV(UInt64 block_index, unsigned char * iv) const
{
    // In CTR mode, OpenSSL EVP API treat the IV as a 128-bit big-endian, and
    // increase it by 1 for each block.
    UInt64 iv_high = initial_iv_high;
    UInt64 iv_low = initial_iv_low + block_index;
    if (std::numeric_limits<UInt64>::max() - block_index < initial_iv_low)
    {
        iv_high++;
    }
    iv_high = toBigEndian(iv_high);
    iv_low = toBigEndian(iv_low);
    memcpy(iv, &iv_high, sizeof(UInt64));
    memcpy(iv + sizeof(UInt64), &iv_low, sizeof(UInt64));
}

void AESCTRCipherStream::encrypt(UInt64 file_offset, char * data, size_t data_size)
{
    const size_t block_size = DB::Encryption::blockSize(method);
    UInt64 block_index = file_offset / block_size;
    unsigned char iv[block_size];
    initIV(block_index, iv);
    LOG_DEBUG(
        DB::Logger::get("ffff"),
        "AESCTRCipher encrypt, before,"
        " block_index={} init_iv_high/low={}/{}"
        " file_offset={} data_size={} iv={} data={}",
        block_index,
        initial_iv_high,
        initial_iv_low,
        file_offset,
        data_size,
        Redact::keyToHexString(reinterpret_cast<char *>(iv), block_size),
        Redact::keyToHexString(data, data_size));
    DB::Encryption::Cipher(file_offset, data, data_size, key, method, iv, /*is_encrypt=*/true);
    LOG_DEBUG(
        DB::Logger::get("ffff"),
        "AESCTRCipher encrypt, after,"
        " block_index={} init_iv_high/low={}/{}"
        " file_offset={} data_size={} iv={} data={}",
        block_index,
        initial_iv_high,
        initial_iv_low,
        file_offset,
        data_size,
        Redact::keyToHexString(reinterpret_cast<char *>(iv), block_size),
        Redact::keyToHexString(data, data_size));
}

void AESCTRCipherStream::decrypt(UInt64 file_offset, char * data, size_t data_size)
{
    const size_t block_size = DB::Encryption::blockSize(method);
    UInt64 block_index = file_offset / block_size;
    unsigned char iv[block_size];
    initIV(block_index, iv);
    LOG_DEBUG(
        DB::Logger::get("ffff"),
        "AESCTRCipher decrypt, before,"
        " block_index={} init_iv_high/low={}/{}"
        " file_offset={} data_size={} iv={} data={}",
        block_index,
        initial_iv_high,
        initial_iv_low,
        file_offset,
        data_size,
        Redact::keyToHexString(reinterpret_cast<char *>(iv), block_size),
        Redact::keyToHexString(data, data_size));
    DB::Encryption::Cipher(file_offset, data, data_size, key, method, iv, /*is_encrypt=*/false);
    LOG_DEBUG(
        DB::Logger::get("ffff"),
        "AESCTRCipher decrypt, after,"
        " block_index={} init_iv_high/low={}/{}"
        " file_offset={} data_size={} iv={} data={}",
        block_index,
        initial_iv_high,
        initial_iv_low,
        file_offset,
        data_size,
        Redact::keyToHexString(reinterpret_cast<char *>(iv), block_size),
        Redact::keyToHexString(data, data_size));
}

} // namespace DB
