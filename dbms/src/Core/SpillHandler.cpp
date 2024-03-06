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

#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Core/SpillHandler.h>
#include <DataStreams/IBlockInputStream.h>
#include <IO/FileProvider/EncryptionPath.h>
#include <IO/FileProvider/WriteBufferFromWritableFileBuilder.h>

namespace DB
{
namespace FailPoints
{
extern const char exception_during_spill[];
} // namespace FailPoints

SpillHandler::SpillWriter::SpillWriter(
    const FileProviderPtr & file_provider,
    const String & file_name,
    bool append_write,
    const Block & header,
    size_t spill_version)
    : file_buf(WriteBufferFromWritableFileBuilder::build(
        file_provider,
        file_name,
        EncryptionPath(file_name, ""),
        true,
        nullptr,
        DBMS_DEFAULT_BUFFER_SIZE,
        append_write ? O_APPEND | O_WRONLY : -1))
    , compressed_buf(file_buf)
{
    if (!append_write)
        writeVarUInt(spill_version, compressed_buf);
    out = std::make_unique<NativeBlockOutputStream>(compressed_buf, spill_version, header);
    out->writePrefix();
}

SpillDetails SpillHandler::SpillWriter::finishWrite()
{
    out->flush();
    compressed_buf.next();
    file_buf.next();
    out->writeSuffix();
    return {written_rows, compressed_buf.count(), file_buf.count()};
}

void SpillHandler::SpillWriter::write(const Block & block)
{
    written_rows += block.rows();
    out->write(block);
}

SpillHandler::SpillHandler(Spiller * spiller_, size_t partition_id_)
    : spiller(spiller_)
    , partition_id(partition_id_)
    , current_spilled_file_index(-1)
    , writer(nullptr)
{}

std::pair<size_t, size_t> SpillHandler::setUpNextSpilledFile()
{
    assert(writer == nullptr);
    auto [spilled_file, append_write] = spiller->getOrCreateSpilledFile(partition_id);
    if (append_write)
        prev_spill_details.merge(spilled_file->getSpillDetails());
    current_spill_file_name = spilled_file->path();
    spilled_files.push_back(std::move(spilled_file));
    current_spilled_file_index = spilled_files.size() - 1;
    writer = std::make_unique<SpillWriter>(
        spiller->config.file_provider,
        current_spill_file_name,
        append_write,
        spiller->input_schema,
        spiller->spill_version);
    return std::make_pair(
        spilled_files[current_spilled_file_index]->getSpillDetails().rows,
        spilled_files[current_spilled_file_index]->getSpillDetails().data_bytes_uncompressed);
}

bool SpillHandler::isSpilledFileFull(UInt64 spilled_rows, UInt64 spilled_bytes)
{
    return (spiller->config.max_spilled_rows_per_file > 0 && spilled_rows >= spiller->config.max_spilled_rows_per_file)
        || (spiller->config.max_spilled_bytes_per_file > 0
            && spilled_bytes >= spiller->config.max_spilled_bytes_per_file);
}

void SpillHandler::spillBlocks(Blocks && blocks)
{
    ///  todo check the disk usage
    if (unlikely(blocks.empty()))
        return;

    RUNTIME_CHECK_MSG(
        current_spilled_file_index != INVALID_CURRENT_SPILLED_FILE_INDEX,
        "{}: spill after the spill handler meeting error or finished.",
        spiller->config.spill_id);
    RUNTIME_CHECK_MSG(
        spiller->isSpillFinished() == false,
        "{}: spill after the spiller is finished.",
        spiller->config.spill_id);

    try
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_spill);

        if unlikely (spiller->isAllConstant())
        {
            LOG_WARNING(
                spiller->logger,
                "Try to spill blocks containing only constant columns, it is meaningless to spill blocks containing "
                "only constant columns");
            for (auto & block : blocks)
            {
                if (unlikely(!block || block.rows() == 0))
                    continue;
                all_constant_block_rows += block.rows();
            }
        }
        else
        {
            Stopwatch watch;
            auto block_size = blocks.size();
            LOG_DEBUG(spiller->logger, "Spilling {} blocks data", block_size);

            size_t total_rows = 0;
            size_t rows_in_file = 0;
            size_t bytes_in_file = 0;
            for (auto & block : blocks)
            {
                if (unlikely(!block || block.rows() == 0))
                    continue;
                /// erase constant column
                spiller->removeConstantColumns(block);
                RUNTIME_CHECK(block.columns() > 0);
                if (unlikely(writer == nullptr))
                {
                    std::tie(rows_in_file, bytes_in_file) = setUpNextSpilledFile();
                }
                auto rows = block.rows();
                total_rows += rows;
                rows_in_file += rows;
                bytes_in_file += block.estimateBytesForSpill();
                writer->write(block);
                block.clear();
                if (spiller->enable_append_write && isSpilledFileFull(rows_in_file, bytes_in_file))
                {
                    spilled_files[current_spilled_file_index]->updateSpillDetails(writer->finishWrite());
                    spilled_files[current_spilled_file_index]->markFull();
                    writer = nullptr;
                }
            }
            double cost = watch.elapsedSeconds();
            time_cost += cost;
            LOG_DEBUG(
                spiller->logger,
                "Spilled {} rows from {} blocks into temporary file, time cost: {:.3f} sec.",
                total_rows,
                block_size,
                cost);
            RUNTIME_CHECK_MSG(
                current_spilled_file_index != INVALID_CURRENT_SPILLED_FILE_INDEX,
                "{}: spill after the spill handler is finished.",
                spiller->config.spill_id);
        }

        RUNTIME_CHECK_MSG(
            spiller->isSpillFinished() == false,
            "{}: spill after the spiller is finished.",
            spiller->config.spill_id);
    }
    catch (...)
    {
        /// mark the spill handler invalid
        writer = nullptr;
        spilled_files.clear();
        current_spilled_file_index = INVALID_CURRENT_SPILLED_FILE_INDEX;
        throw Exception(fmt::format(
            "Failed to spill blocks to disk for file {}, error: {}",
            current_spill_file_name,
            getCurrentExceptionMessage(false, false)));
    }
}

void SpillHandler::finish()
{
    /// it is guaranteed that once current_spilled_file_index >= 0, at least one block is written to spilled_files[current_spilled_file_index]
    if (likely(current_spilled_file_index >= 0))
    {
        if (writer != nullptr)
        {
            spilled_files[current_spilled_file_index]->updateSpillDetails(writer->finishWrite());
            auto current_spill_details = spilled_files[current_spilled_file_index]->getSpillDetails();
            if (!spiller->enable_append_write
                || isSpilledFileFull(current_spill_details.rows, current_spill_details.data_bytes_uncompressed))
            {
                /// always mark full if enable_append_write is false here, since if enable_append_write is false, all the files are treated as full file
                spilled_files[current_spilled_file_index]->markFull();
            }
        }

        auto gen_spill_detail_info = [&]() {
            SpillDetails details{0, 0, 0};
            for (Int64 i = 0; i <= current_spilled_file_index; i++)
                details.merge(spilled_files[i]->getSpillDetails());
            details.subtract(prev_spill_details);
            return fmt::format(
                "Commit spilled data, details: spill {} rows in {:.3f} sec,"
                " {:.3f} MiB uncompressed, {:.3f} MiB compressed, {:.3f} uncompressed bytes per row, {:.3f} compressed "
                "bytes per row, "
                "compression rate: {:.3f} ({:.3f} rows/sec., {:.3f} MiB/sec. uncompressed, {:.3f} MiB/sec. compressed)",
                details.rows,
                time_cost,
                (details.data_bytes_uncompressed / 1048576.0),
                (details.data_bytes_compressed / 1048576.0),
                (details.data_bytes_uncompressed / static_cast<double>(details.rows)),
                (details.data_bytes_compressed / static_cast<double>(details.rows)),
                (details.data_bytes_uncompressed / static_cast<double>(details.data_bytes_compressed)),
                (details.rows / time_cost),
                (details.data_bytes_uncompressed / time_cost / 1048576.0),
                (details.data_bytes_compressed / time_cost / 1048576.0));
        };
        LOG_DEBUG(spiller->logger, gen_spill_detail_info());
        spiller->spilled_files[partition_id]->commitSpilledFiles(std::move(spilled_files));
        spiller->has_spilled_data = true;
        current_spilled_file_index = INVALID_CURRENT_SPILLED_FILE_INDEX;
        RUNTIME_CHECK_MSG(
            spiller->isSpillFinished() == false,
            "{}: spill after the spiller is finished.",
            spiller->config.spill_id);
    }
    else if unlikely (spiller->isAllConstant())
    {
        if (all_constant_block_rows > 0)
        {
            spiller->recordAllConstantBlockRows(partition_id, all_constant_block_rows);
            spiller->has_spilled_data = true;
            all_constant_block_rows = 0;
        }
    }
}
} // namespace DB
