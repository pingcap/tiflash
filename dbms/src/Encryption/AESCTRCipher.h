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
#include <common/types.h>
#include <openssl/aes.h>
#include <openssl/conf.h>
#if USE_GM_SSL
#include <gmssl/sm4.h>
#endif
#include <RaftStoreProxyFFI/EncryptionFFI.h>


namespace DB::Encryption
{

// Get the key size of the encryption method.
size_t keySize(EncryptionMethod method);

// Get the block size of the encryption method.
size_t blockSize(EncryptionMethod method);

// Get the cipher of the encryption method.
// `cipher` is just a pointer to a static storage, so no need to free it after use.
const EVP_CIPHER * getCipher(EncryptionMethod method);

// Encrypt or decrypt data in place.
// Please ensure the key and IV size appropriate for the cipher.
// Note: the IV will be modified in place when method is SM4.
// Please ensure the IV is the same when encrypt and decrypt.
void Cipher(
    uint64_t file_offset,
    char * data,
    size_t data_size,
    String key,
    EncryptionMethod method,
    unsigned char * iv,
    bool is_encrypt);

} // namespace DB::Encryption
