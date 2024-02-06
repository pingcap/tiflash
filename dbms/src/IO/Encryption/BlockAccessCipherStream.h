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

#include <stdint.h>

#include <memory>

namespace DB
{
// BlockAccessCipherStream is the base class for any cipher stream that
// supports random access at block level (without requiring data from other
// blocks). E.g. CTR (Counter operation mode) supports this requirement.
class BlockAccessCipherStream
{
public:
    virtual ~BlockAccessCipherStream() = default;

    // Encrypt one or more (partial) blocks of data at the file offset.
    // Length of data is given in dataSize.
    virtual void encrypt(uint64_t fileOffset, char * data, size_t dataSize) = 0;

    // Decrypt one or more (partial) blocks of data at the file offset.
    // Length of data is given in dataSize.
    virtual void decrypt(uint64_t fileOffset, char * data, size_t dataSize) = 0;
};

using BlockAccessCipherStreamPtr = std::shared_ptr<BlockAccessCipherStream>;
} // namespace DB
