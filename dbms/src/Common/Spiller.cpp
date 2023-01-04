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

#include <Common/Spiller.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/SpilledFilesInputStream.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/Path.h>


namespace DB
{
SpilledFile::SpilledFile(const String & file_name_)
    : Poco::File(file_name_)
{}

SpilledFile::~SpilledFile()
{
    try
    {
        if (exists())
            remove(true);
    }
    catch (...)
    {
    }
}

Spiller::Spiller(const String & id_, bool is_input_sorted_, size_t partition_num_, const String & spill_dir_, const Block & input_schema_, LoggerPtr logger_)
    : id(id_)
    , is_input_sorted(is_input_sorted_)
    , partition_num(partition_num_)
    , spill_dir(spill_dir_)
    , input_schema(input_schema_)
    , logger(logger_)
{
    if (spill_dir.at(spill_dir.size() - 1) != Poco::Path::separator())
    {
        spill_dir += Poco::Path::separator();
    }
    spilled_files.resize(partition_num);
}

bool Spiller::spillBlocks(const Blocks & blocks, size_t partition_id)
{
    RUNTIME_CHECK_MSG(partition_id < partition_num, "{}: partition id {} exceeds partition num {}.", id, partition_id, partition_num);
    RUNTIME_CHECK_MSG(spill_finished == false, "{}: spill after the spiller is finished.", id);
    /// todo append to existing file
    auto spilled_file_name = nextSpillFileName(partition_id);
    try
    {
        auto spilled_file = std::make_unique<SpilledFile>(spilled_file_name);
        if (spilled_file->exists())
            throw Exception("Duplicated spilled files, should not happens");
        WriteBufferFromFile file_buf(spilled_file_name);
        CompressedWriteBuffer compressed_buf(file_buf);
        NativeBlockOutputStream block_out(compressed_buf, 0, blocks[0].cloneEmpty());
        block_out.writePrefix();
        for (const auto & block : blocks)
        {
            auto block_bytes_size = block.bytes();
            block_out.write(block);
            spilled_file->addSpilledDataSize(block_bytes_size);
        }
        {
            std::lock_guard lock(spilled_files_mutex);
            spilled_files[partition_id].emplace_back(std::move(spilled_file));
        }
        return true;
    }
    catch (...)
    {
        LOG_ERROR(logger, "Failed to spill block to disk for file {}, error message: {}", spilled_file_name, getCurrentExceptionMessage(false, false));
        return false;
    }
}

BlockInputStreams Spiller::restoreBlocks(size_t partition_id, size_t max_stream_size)
{
    RUNTIME_CHECK_MSG(partition_id < partition_num, "{}: partition id {} exceeds partition num {}.", id, partition_id, partition_num);
    RUNTIME_CHECK_MSG(spill_finished, "{}: restore before the spiller is finished.", id);
    if (is_input_sorted && spilled_files[partition_id].size() > max_stream_size)
        LOG_WARNING(logger, "sorted spilled data restore does not take max_stream_size into account");
    BlockInputStreams ret;
    if (is_input_sorted)
    {
        for (const auto & file : spilled_files[partition_id])
        {
            if (likely(file->exists()))
            {
                std::vector<String> files{file->path()};
                ret.push_back(std::make_shared<SpilledFilesInputStream>(files, input_schema));
            }
            else
            {
                LOG_WARNING(logger, "Spill file {} does not exists", file->path());
            }
        }
    }
    else
    {
        size_t return_stream_num = std::min(max_stream_size, spilled_files[partition_id].size());
        std::vector<std::vector<String>> files(return_stream_num);
        for (size_t i = 0; i < spilled_files[partition_id].size(); ++i)
        {
            if (likely(spilled_files[partition_id][i]->exists()))
                files[i % return_stream_num].push_back(spilled_files[partition_id][i]->path());
            else
                LOG_WARNING(logger, "Spill file {} does not exists", spilled_files[partition_id][i]->path());
        }
        for (size_t i = 0; i < return_stream_num; ++i)
        {
            if (likely(!files[i].empty()))
                ret.push_back(std::make_shared<SpilledFilesInputStream>(files[i], input_schema));
        }
    }
    if (ret.empty())
        ret.push_back(std::make_shared<NullBlockInputStream>(input_schema));
    return ret;
}

size_t Spiller::spilledBlockDataSize(size_t partition_id)
{
    RUNTIME_CHECK_MSG(partition_id < partition_num, "{}: partition id {} exceeds partition num {}.", id, partition_id, partition_num);
    RUNTIME_CHECK_MSG(spill_finished, "{}: spilledBlockDataSize must be called when the spiller is finished.", id);
    size_t ret = 0;
    for (auto & file : spilled_files[partition_id])
        ret += file->getSpilledDataSize();
    return ret;
}

String Spiller::nextSpillFileName(size_t partition_id)
{
    Int64 index = tmp_file_index.fetch_add(1);
    return fmt::format("{}tmp_{}_partition_{}_{}", spill_dir, id, partition_id, index);
}

std::atomic<Int64> Spiller::tmp_file_index = 0;

} // namespace DB
