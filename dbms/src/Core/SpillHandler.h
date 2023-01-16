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
#include <IO/VarInt.h>

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
    class SpillWriter
    {
    public:
        SpillWriter(const FileProviderPtr & file_provider, const String & file_name, const Block & header, size_t spill_version)
            : file_buf(file_provider, file_name, EncryptionPath(file_name, ""))
            , compressed_buf(file_buf)
        {
            /// note this implicitly assumes that a SpillWriter will always write to a new file,
            /// if we support append write, don't need to write the spill version again
            writeVarUInt(spill_version, compressed_buf);
            out = std::make_unique<NativeBlockOutputStream>(compressed_buf, spill_version, header);
            out->writePrefix();
        }
        SpillDetails finishWrite()
        {
            out->flush();
            compressed_buf.next();
            file_buf.next();
            out->writeSuffix();
            return {written_rows, compressed_buf.count(), file_buf.count()};
        }
        void write(const Block & block)
        {
            written_rows += block.rows();
            out->write(block);
        }

    private:
        WriteBufferFromFileProvider file_buf;
        CompressedWriteBuffer<> compressed_buf;
        std::unique_ptr<IBlockOutputStream> out;
        size_t written_rows = 0;
    };
    Spiller * spiller;
    std::vector<std::unique_ptr<SpilledFile>> spilled_files;
    size_t partition_id;
    Int64 current_spilled_file_index;
    String current_spill_file_name;
    std::unique_ptr<SpillWriter> writer;
    double time_cost = 0;
};

} // namespace DB
