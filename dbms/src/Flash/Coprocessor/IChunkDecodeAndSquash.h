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

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/CodecUtils.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>

namespace DB
{
class IChunkDecodeAndSquash
{
public:
    virtual ~IChunkDecodeAndSquash() = default;
    /// The returned optional value can only have block that block.operator bool() is true
    virtual std::optional<Block> decodeAndSquash(const String &) = 0;
    /// The returned optional value can only have block that block.operator bool() is true
    virtual std::optional<Block> flush() = 0;
};

class CHBlockChunkDecodeAndSquash final : public IChunkDecodeAndSquash
{
public:
    explicit CHBlockChunkDecodeAndSquash(const Block & header, size_t rows_limit_);
    virtual ~CHBlockChunkDecodeAndSquash() = default;
    std::optional<Block> decodeAndSquash(const String &);
    std::optional<Block> flush() { return accumulated_block; }
private:
    CHBlockChunkCodec codec;
    std::optional<Block> accumulated_block;
    size_t rows_limit;
};


} // namespace DB
