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

#include <Common/Stopwatch.h>
#include <Core/SpillHandler.h>

namespace DB
{
SpillHandler::SpillWriter::SpillWriter(const FileProviderPtr & file_provider, const String & file_name, bool append_write, const Block & header, size_t spill_version)
    : file_buf(file_provider, file_name, EncryptionPath(file_name, ""), true, nullptr, DBMS_DEFAULT_BUFFER_SIZE, append_write ? O_APPEND | O_WRONLY : -1)
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
{
    setUpNextSpilledFile();
}

void SpillHandler::setUpNextSpilledFile()
{
    writer = nullptr;
    auto [spilled_file, append_write] = spiller->getOrCreateSpilledFile(partition_id);
    if (append_write)
        prev_spill_details.merge(spilled_file->getSpillDetails());
    current_spill_file_name = spilled_file->path();
    current_append_write = append_write;
    spilled_files.push_back(std::move(spilled_file));
    current_spilled_file_index = spilled_files.size() - 1;
}

bool SpillHandler::isSpilledFileFull(UInt64 spilled_rows, UInt64 spilled_bytes)
{
    return (spiller->config.max_spilled_rows_per_file > 0 && spilled_rows >= spiller->config.max_spilled_rows_per_file) || (spiller->config.max_spilled_bytes_per_file > 0 && spilled_bytes >= spiller->config.max_spilled_bytes_per_file);
}

void SpillHandler::spillBlocks(const Blocks & blocks)
{
    ///  todo 1. set max_file_size and spill to new file if needed
    ///   2. check the disk usage
    if (unlikely(blocks.empty()))
        return;
    RUNTIME_CHECK_MSG(current_spilled_file_index >= 0, "{}: spill after the spill handler meeting error or finished.", spiller->config.spill_id);
    try
    {
        Stopwatch watch;
        RUNTIME_CHECK_MSG(spiller->isSpillFinished() == false, "{}: spill after the spiller is finished.", spiller->config.spill_id);
        auto block_size = blocks.size();
        LOG_INFO(spiller->logger, "Spilling {} blocks data into temporary file {}", block_size, current_spill_file_name);
        size_t total_rows = 0;
        size_t rows_in_file = 0;
        size_t bytes_in_file = 0;
        for (const auto & block : blocks)
        {
            if (unlikely(writer == nullptr))
            {
                writer = std::make_unique<SpillWriter>(spiller->config.file_provider, current_spill_file_name, current_append_write, blocks[0].cloneEmpty(), spiller->spill_version);
                rows_in_file = spilled_files[current_spilled_file_index]->getSpillDetails().rows;
                bytes_in_file = spilled_files[current_spilled_file_index]->getSpillDetails().data_bytes_uncompressed;
            }
            auto rows = block.rows();
            total_rows += rows;
            rows_in_file += rows;
            bytes_in_file += block.estimateBytesForSpill();
            writer->write(block);
            if (spiller->enable_append_write && isSpilledFileFull(rows_in_file, bytes_in_file))
            {
                spilled_files[current_spilled_file_index]->updateSpillDetails(writer->finishWrite());
                spilled_files[current_spilled_file_index]->markFull();
                setUpNextSpilledFile();
            }
        }
        double cost = watch.elapsedSeconds();
        time_cost += cost;
        LOG_INFO(spiller->logger, "Spilled {} rows from {} blocks into temporary file, time cost: {:.3f} sec.", total_rows, block_size, cost);
        RUNTIME_CHECK_MSG(current_spilled_file_index >= 0, "{}: spill after the spill handler is finished.", spiller->config.spill_id);
        RUNTIME_CHECK_MSG(spiller->isSpillFinished() == false, "{}: spill after the spiller is finished.", spiller->config.spill_id);
        return;
    }
    catch (...)
    {
        /// mark the spill handler invalid
        writer = nullptr;
        spilled_files.clear();
        current_spilled_file_index = -1;
        throw Exception(fmt::format("Failed to spill blocks to disk for file {}, error: {}", current_spill_file_name, getCurrentExceptionMessage(false, false)));
    }
}

void SpillHandler::finish()
{
    if (likely(current_spilled_file_index >= 0))
    {
        if (writer != nullptr)
        {
            auto spill_details = writer->finishWrite();
            spilled_files[current_spilled_file_index]->updateSpillDetails(spill_details);
            if (spiller->enable_append_write)
            {
                if (isSpilledFileFull(spilled_files[current_spilled_file_index]->getSpillDetails().rows, spilled_files[current_spilled_file_index]->getSpillDetails().data_bytes_uncompressed))
                    spilled_files[current_spilled_file_index]->markFull();
            }
            else
            {
                /// always mark full if enable_append_write is false since if enable_append_write is false, all the file is full
                spilled_files[current_spilled_file_index]->markFull();
            }
        }
        else
        {
            /// writer == nullptr means no write to current file
            /// but if current_append_write is true, we still need to put the original file back
            if (!current_append_write)
            {
                current_spilled_file_index--;
                spilled_files.pop_back();
            }
        }

        if (current_spilled_file_index >= 0)
        {
            auto gen_spill_detail_info = [&]() {
                SpillDetails details{0, 0, 0};
                for (Int64 i = 0; i <= current_spilled_file_index; i++)
                    details.merge(spilled_files[i]->getSpillDetails());
                details.subtract(prev_spill_details);
                return fmt::format("Commit spilled data, details: spill {} rows in {:.3f} sec,"
                                   " {:.3f} MiB uncompressed, {:.3f} MiB compressed, {:.3f} uncompressed bytes per row, {:.3f} compressed bytes per row, "
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
            std::unique_lock lock(spiller->spilled_files[partition_id]->spilled_files_mutex);
            for (auto & spilled_file : spilled_files)
            {
                if (!spilled_file->isFull())
                    spiller->spilled_files[partition_id]->mutable_spilled_files.push_back(std::move(spilled_file));
                else
                    spiller->spilled_files[partition_id]->immutable_spilled_files.push_back(std::move(spilled_file));
            }
            spilled_files.clear();
            spiller->has_spilled_data = true;
            current_spilled_file_index = -1;
            RUNTIME_CHECK_MSG(spiller->isSpillFinished() == false, "{}: spill after the spiller is finished.", spiller->config.spill_id);
        }
        else
        {
            assert(spilled_files.empty());
        }
    }
}

} // namespace DB
