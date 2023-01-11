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

#include <Core/SpillHandler.h>

namespace DB
{

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
        RUNTIME_CHECK_MSG(spiller->spill_finished == false, "{}: spill after the spiller is finished.", spiller->config.spill_id);
        LOG_INFO(spiller->logger, "Spilling {} blocks data into temporary file {}", blocks.size(), current_spill_file_name);
        size_t spilled_data_size = 0;
        if (unlikely(writer == nullptr))
        {
            writer = std::make_unique<SpillWriter>(spiller->config.file_provider, current_spill_file_name, blocks[0].cloneEmpty());
            writer->out->writePrefix();
        }
        for (const auto & block : blocks)
        {
            auto block_bytes_size = block.bytes();
            writer->out->write(block);
            spilled_files[current_spilled_file_index]->addSpilledDataSize(block_bytes_size);
            spilled_data_size += block_bytes_size;
        }
        LOG_INFO(spiller->logger, "Finish Spilling data into temporary file {}, spilled data size: {}", current_spill_file_name, spilled_data_size);
        RUNTIME_CHECK_MSG(current_spilled_file_index >= 0, "{}: spill after the spill handler is finished.", spiller->config.spill_id);
        RUNTIME_CHECK_MSG(spiller->spill_finished == false, "{}: spill after the spiller is finished.", spiller->config.spill_id);
        return;
    }
    catch (...)
    {
        /// mark the spill handle invalid
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
        writer->out->writeSuffix();
        std::unique_lock lock(spiller->spilled_files[partition_id]->spilled_files_mutex);
        for (auto & spilled_file : spilled_files)
            spiller->spilled_files[partition_id]->spilled_files.push_back(std::move(spilled_file));
        spilled_files.clear();
        spiller->has_spilled_data = true;
        current_spilled_file_index = -1;
    }
}

} // namespace DB
