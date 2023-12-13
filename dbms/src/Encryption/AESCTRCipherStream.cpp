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

#include <Encryption/AESCTRCipherStream.h>

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

} // namespace DB
