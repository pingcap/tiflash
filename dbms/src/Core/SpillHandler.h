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

#include <Core/Spiller.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/Buffer/WriteBufferFromWritableFile.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <IO/VarInt.h>

namespace DB
{
class IBlockOutputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

/// SpillHandler is used to spill blocks, currently hidden behind `Spiller::spillBlocks`
/// and `Spiller::spillBlocksUsingBlockInputStream`, maybe need to be exposed in push model.
/// NOTE 1. SpillHandler is not thread-safe, each thread should use its own spill handler
///      2. After all the data is spilled, SpillHandler::finish() must be called to submit the spilled data
class SpillHandler
{
public:
    SpillHandler(Spiller * spiller_, size_t partition_id_);
    void spillBlocks(Blocks && blocks);
    void finish();

private:
    std::pair<size_t, size_t> setUpNextSpilledFile();
    bool isSpilledFileFull(UInt64 spilled_rows, UInt64 spilled_bytes);
    class SpillWriter
    {
    public:
        SpillWriter(
            const FileProviderPtr & file_provider,
            const String & file_name,
            bool append_write,
            const Block & header,
            size_t spill_version);
        void finishWrite(std::unique_ptr<SpilledFile> & spilled_file);
        void write(const Block & block, std::unique_ptr<SpilledFile> & spilled_file);

    private:
        // Have to make sure bytes record into SpillLimiter is equal to spilled_file.data_bytes_compressed,
        // because when spilled_file destory, SpillLimiter will minus spilled_file.data_bytes_compressed.
        // We have to make sure SpillLimiter::current_spill_bytes >= 0.
        void recordSpillStats(size_t delta_rows, const std::unique_ptr<SpilledFile> & spilled_file)
        {
            RUNTIME_CHECK_MSG(
                compressed_buf.count() >= last_bytes_uncompressed && file_buf.count() >= last_bytes_compressed,
                "check spill detail failed, bytes uncompressed: {}, {}, bytes compressed: {}, {}",
                compressed_buf.count(),
                last_bytes_uncompressed,
                file_buf.count(),
                last_bytes_compressed);

            auto delta = SpillDetails{
                delta_rows,
                compressed_buf.count() - last_bytes_uncompressed,
                file_buf.count() - last_bytes_compressed};

            last_bytes_uncompressed = compressed_buf.count();
            last_bytes_compressed = file_buf.count();

            spilled_file->updateSpillDetails(delta);

            // Use file_buf.count() (a.k.a. compressed data size) instead of uncompressed data size
            // because we want to record the real bytes written to files.
            SpillLimiter::instance->addSpilledBytes(delta.data_bytes_compressed);
        }

        WriteBufferFromWritableFile file_buf;
        CompressedWriteBuffer<> compressed_buf;
        std::unique_ptr<IBlockOutputStream> out;
        size_t last_bytes_uncompressed = 0;
        size_t last_bytes_compressed = 0;
    };

    static void checkOkToSpill()
    {
        auto [ok_to_spill, cur_spilled_bytes, max_bytes] = SpillLimiter::instance->okToSpill();
        if (!ok_to_spill)
            throw Exception(fmt::format(
                "Failed to spill bytes to disk because exceeds max_spilled_bytes({}), cur_spilled_bytes: "
                "{}",
                max_bytes,
                cur_spilled_bytes));
    }

    // Expect to be called in catch statement.
    void markAsInvalidAndRethrow()
    {
        writer = nullptr;
        spilled_files.clear();
        current_spilled_file_index = INVALID_CURRENT_SPILLED_FILE_INDEX;
        throw Exception(fmt::format(
            "Failed to spill blocks to disk for file {}, error: {}",
            current_spill_file_name,
            getCurrentExceptionMessage(false, false)));
    }

    Spiller * spiller;
    std::vector<std::unique_ptr<SpilledFile>> spilled_files;
    UInt64 all_constant_block_rows = 0;
    size_t partition_id;
    Int64 current_spilled_file_index;
    String current_spill_file_name;
    std::unique_ptr<SpillWriter> writer;
    double time_cost = 0;
    SpillDetails prev_spill_details;
    static const Int64 INVALID_CURRENT_SPILLED_FILE_INDEX = -10;
};
} // namespace DB
