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

#include <IO/Encryption/AESCTRCipher.h>
#include <IO/Encryption/BlockAccessCipherStream.h>
#include <common/types.h>

namespace DB
{
struct EncryptionPath;

class AESCTRCipherStream : public BlockAccessCipherStream
{
public:
    AESCTRCipherStream(const EncryptionMethod method_, std::string key_, UInt64 iv_high, UInt64 iv_low)
        : method(method_)
        , key(std::move(key_))
        , initial_iv_high(iv_high)
        , initial_iv_low(iv_low)
    {}

    ~AESCTRCipherStream() override = default;

    void encrypt(UInt64 file_offset, char * data, size_t data_size) override;
    void decrypt(UInt64 file_offset, char * data, size_t data_size) override;

private:
    inline void initIV(UInt64 block_index, unsigned char * iv) const;

    const EncryptionMethod method;
    const String key;
    const UInt64 initial_iv_high;
    const UInt64 initial_iv_low;
};
} // namespace DB
