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

#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/CodecUtils.h>

namespace DB
{

class CHBlockChunkDecodeAndSquash
{
public:
    CHBlockChunkDecodeAndSquash(const Block & header, size_t rows_limit_);
    ~CHBlockChunkDecodeAndSquash() = default;
    std::optional<Block> decodeAndSquash(const String &);
    std::optional<Block> decodeAndSquashV1(std::string_view);
    std::optional<Block> flush();

private:
    std::optional<Block> decodeAndSquashV1Impl(ReadBuffer & istr);

private:
    CHBlockChunkCodec codec;
    std::optional<Block> accumulated_block;
    size_t rows_limit;
};


} // namespace DB
