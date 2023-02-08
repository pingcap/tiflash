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

SpillHandler::SpillWriter::SpillWriter(const FileProviderPtr & file_provider, const String & file_name, const Block & header, size_t spill_version)
    : file_buf(file_provider, file_name, EncryptionPath(file_name, ""))
    , compressed_buf(file_buf)
{
    /// note this implicitly assumes that a SpillWriter will always write to a new file,
    /// if we support append write, don't need to write the spill version again
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

SpillHandler::SpillHandler(Spiller * spiller_, std::unique_ptr<SpilledFile> && spilled_file, size_t partition_id_)
    : spiller(spiller_)
    , partition_id(partition_id_)
{
    current_spill_file_name = spilled_file->path();
    current_spilled_file_index = 0;
    spilled_files.push_back(std::move(spilled_file));
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
        RUNTIME_CHECK_MSG(spiller->spill_finished == false, "{}: spill after the spiller is finished.", spiller->config.spill_id);
        auto block_size = blocks.size();
        LOG_INFO(spiller->logger, "Spilling {} blocks data into temporary file {}", block_size, current_spill_file_name);
        size_t total_rows = 0;
        if (unlikely(writer == nullptr))
        {
            writer = std::make_unique<SpillWriter>(spiller->config.file_provider, current_spill_file_name, blocks[0].cloneEmpty(), spiller->spill_version);
        }
        for (const auto & block : blocks)
        {
            total_rows += block.rows();
            writer->write(block);
        }
        double cost = watch.elapsedSeconds();
        time_cost += cost;
        LOG_INFO(spiller->logger, "Spilled {} rows from {} blocks into temporary file, time cost: {:.3f} sec.", total_rows, block_size, cost);
        RUNTIME_CHECK_MSG(current_spilled_file_index >= 0, "{}: spill after the spill handler is finished.", spiller->config.spill_id);
        RUNTIME_CHECK_MSG(spiller->spill_finished == false, "{}: spill after the spiller is finished.", spiller->config.spill_id);
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
    if (likely(writer != nullptr))
    {
        auto spill_details = writer->finishWrite();
        spilled_files[current_spilled_file_index]->updateSpillDetails(spill_details);

        auto gen_spill_detail_info = [&]() {
            SpillDetails details{0, 0, 0};
            for (Int64 i = 0; i <= current_spilled_file_index; i++)
                details.merge(spilled_files[i]->getSpillDetails());
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
            spiller->spilled_files[partition_id]->spilled_files.push_back(std::move(spilled_file));
        spilled_files.clear();
        spiller->has_spilled_data = true;
        current_spilled_file_index = -1;
        RUNTIME_CHECK_MSG(spiller->spill_finished == false, "{}: spill after the spiller is finished.", spiller->config.spill_id);
    }
}

} // namespace DB
