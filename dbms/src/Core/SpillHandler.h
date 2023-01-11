// Copyright 2023 PingCAP, Ltd.
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

#include <Core/Spiller.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <IO/CompressedWriteBuffer.h>

namespace DB
{

class IBlockOutputStream;

/// SpillHandler is used to spill blocks, currently hidden behind `Spiller::spillBlocks`
/// and `Spiller::spillBlocksUsingBlockInputStream`, maybe need to be exposed in push model.
/// NOTE 1. SpillHandler is not thread-safe, each thread should use its own spill handler
///      2. After all the data is spilled, SpillHandler::finish() must be called to submit the spilled data
class SpillHandler
{
public:
    SpillHandler(Spiller * spiller_, std::unique_ptr<SpilledFile> && spilled_file, size_t partition_id_);
    void spillBlocks(const Blocks & blocks);
    void finish();

private:
    struct SpillWriter
    {
        SpillWriter(const FileProviderPtr & file_provider, const String & file_name, const Block & header)
            : file_buf(file_provider, file_name, EncryptionPath(file_name, ""))
            , compressed_buf(file_buf)
            , out(std::make_unique<NativeBlockOutputStream>(compressed_buf, 0, header))
        {
        }
        WriteBufferFromFileProvider file_buf;
        CompressedWriteBuffer<> compressed_buf;
        std::unique_ptr<IBlockOutputStream> out;
    };
    Spiller * spiller;
    std::vector<std::unique_ptr<SpilledFile>> spilled_files;
    size_t partition_id;
    Int64 current_spilled_file_index;
    String current_spill_file_name;
    std::unique_ptr<SpillWriter> writer;
};

} // namespace DB
