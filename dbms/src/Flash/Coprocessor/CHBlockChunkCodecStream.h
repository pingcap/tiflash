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

#include <Common/TiFlashException.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/CompressedCHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{
class CHBlockChunkCodecStream : public ChunkCodecStream
{
public:
    explicit CHBlockChunkCodecStream(const std::vector<tipb::FieldType> & field_types);
    String getString() override;
    void clear() override;
    void encode(const Block & block, size_t start, size_t end) override;
    ~CHBlockChunkCodecStream() override = default;

private:
    WriteBuffer * initOutputBuffer(size_t init_size);
    std::unique_ptr<WriteBufferFromOwnString> output;
    DataTypes expected_types;
};

size_t getExtraInfoSize(const Block & block);
size_t ApproxBlockBytes(const Block & block);
CompressionMethod ToInternalCompressionMethod(mpp::CompressMethod compress_method);
std::unique_ptr<CHBlockChunkCodecStream> NewCHBlockChunkCodecStream(const std::vector<tipb::FieldType> & field_types);
void EncodeCHBlockChunk(WriteBuffer * ostr_ptr, const Block & block);

} // namespace DB
