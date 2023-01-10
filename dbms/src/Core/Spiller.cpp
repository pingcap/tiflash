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
#include <Core/Spiller.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/SpilledFilesInputStream.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/Path.h>

#include "DataStreams/copyData.h"


namespace DB
{
SpilledFile::SpilledFile(const String & file_name_, const FileProviderPtr & file_provider_)
    : Poco::File(file_name_)
    , file_provider(file_provider_)
{}

static DB::LoggerPtr getSpilledFileLogger()
{
    static auto logger = DB::Logger::get("SpilledFile");
    return logger;
}

SpilledFile::~SpilledFile()
{
    try
    {
        auto file_path = path();
        file_provider->deleteRegularFile(file_path, EncryptionPath(file_path, ""));
    }
    catch (...)
    {
        LOG_WARNING(getSpilledFileLogger(), "Failed to clean spilled file {}, error message: {}", path(), getCurrentExceptionMessage(false, false));
    }
}

Spiller::Spiller(const SpillConfig & config_, bool is_input_sorted_, size_t partition_num_, const Block & input_schema_, const LoggerPtr & logger_)
    : config(config_)
    , is_input_sorted(is_input_sorted_)
    , partition_num(partition_num_)
    , input_schema(input_schema_)
    , logger(logger_)
{
    for (size_t i = 0; i < partition_num; ++i)
        spilled_files.push_back(std::make_unique<SpilledFiles>());
    Poco::File spill_dir(config.spill_dir);
    if (!spill_dir.exists())
    {
        /// just for test, usually the tmp path should be created when server starting
        spill_dir.createDirectories();
    }
    else
    {
        RUNTIME_CHECK_MSG(spill_dir.isDirectory(), "Spill dir {} is a file", spill_dir.path());
    }
}

void Spiller::spillBlocksUsingBlockInputStream(IBlockInputStream & block_in, size_t partition_id, const std::function<bool()> & is_cancelled)
{
    auto spill_handler = createSpillHandler(partition_id);
    block_in.readPrefix();
    Blocks spill_blocks;
    while (true)
    {
        spill_blocks = readData(block_in, config.max_spilled_size_per_spill, is_cancelled);
        if (spill_blocks.empty())
            break;
        spill_handler.spillBlocks(spill_blocks);
    }
    if (is_cancelled())
        return;
    block_in.readSuffix();
    /// submit the spilled data
    spill_handler.finish();
}

SpillHandler Spiller::createSpillHandler(size_t partition_id)
{
    RUNTIME_CHECK_MSG(partition_id < partition_num, "{}: partition id {} exceeds partition num {}.", config.spill_id, partition_id, partition_num);
    RUNTIME_CHECK_MSG(spill_finished == false, "{}: spill after the spiller is finished.", config.spill_id);
    auto spilled_file_name = nextSpillFileName(partition_id);
    auto spilled_file = std::make_unique<SpilledFile>(spilled_file_name, config.file_provider);
    RUNTIME_CHECK_MSG(!spilled_file->exists(), "Duplicated spilled file: {}, should not happens", spilled_file_name);
    return SpillHandler(this, std::move(spilled_file), partition_id);
}

void Spiller::spillBlocks(const Blocks & blocks, size_t partition_id)
{
    auto spiller_handler = createSpillHandler(partition_id);
    spiller_handler.spillBlocks(blocks);
    spiller_handler.finish();
}

BlockInputStreams Spiller::restoreBlocks(size_t partition_id, size_t max_stream_size)
{
    RUNTIME_CHECK_MSG(partition_id < partition_num, "{}: partition id {} exceeds partition num {}.", config.spill_id, partition_id, partition_num);
    RUNTIME_CHECK_MSG(spill_finished, "{}: restore before the spiller is finished.", config.spill_id);
    if (max_stream_size == 0)
        max_stream_size = spilled_files[partition_id]->spilled_files.size();
    if (is_input_sorted && spilled_files[partition_id]->spilled_files.size() > max_stream_size)
        LOG_WARNING(logger, "sorted spilled data restore does not take max_stream_size into account");
    BlockInputStreams ret;
    if (is_input_sorted)
    {
        for (const auto & file : spilled_files[partition_id]->spilled_files)
        {
            RUNTIME_CHECK_MSG(file->exists(), "Spill file {} does not exists", file->path());
            std::vector<String> files{file->path()};
            ret.push_back(std::make_shared<SpilledFilesInputStream>(files, input_schema, config.file_provider));
        }
    }
    else
    {
        size_t return_stream_num = std::min(max_stream_size, spilled_files[partition_id]->spilled_files.size());
        std::vector<std::vector<String>> files(return_stream_num);
        // todo balance based on SpilledDataSize
        for (size_t i = 0; i < spilled_files[partition_id]->spilled_files.size(); ++i)
        {
            const auto & file = spilled_files[partition_id]->spilled_files[i];
            RUNTIME_CHECK_MSG(file->exists(), "Spill file {} does not exists", file->path());
            files[i % return_stream_num].push_back(file->path());
        }
        for (size_t i = 0; i < return_stream_num; ++i)
        {
            if (likely(!files[i].empty()))
                ret.push_back(std::make_shared<SpilledFilesInputStream>(files[i], input_schema, config.file_provider));
        }
    }
    if (ret.empty())
        ret.push_back(std::make_shared<NullBlockInputStream>(input_schema));
    return ret;
}

size_t Spiller::spilledBlockDataSize(size_t partition_id)
{
    RUNTIME_CHECK_MSG(partition_id < partition_num, "{}: partition id {} exceeds partition num {}.", config.spill_id, partition_id, partition_num);
    RUNTIME_CHECK_MSG(spill_finished, "{}: spilledBlockDataSize must be called when the spiller is finished.", config.spill_id);
    size_t ret = 0;
    for (auto & file : spilled_files[partition_id]->spilled_files)
        ret += file->getSpilledDataSize();
    return ret;
}

String Spiller::nextSpillFileName(size_t partition_id)
{
    Int64 index = tmp_file_index.fetch_add(1);
    return fmt::format("{}tmp_{}_partition_{}_{}", config.spill_dir, config.spill_id_as_file_name_prefix, partition_id, index);
}

std::atomic<Int64> Spiller::tmp_file_index = 0;

} // namespace DB
