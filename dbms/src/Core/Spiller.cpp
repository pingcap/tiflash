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
#include <Core/CachedSpillHandler.h>
#include <Core/SpillHandler.h>
#include <Core/Spiller.h>
#include <DataStreams/ConstantsBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/SpilledFilesInputStream.h>
#include <DataStreams/copyData.h>
#include <IO/FileProvider/FileProvider.h>
#include <Poco/Path.h>

namespace DB
{
namespace FailPoints
{
extern const char random_spill_to_disk_failpoint[];
} // namespace FailPoints

SpilledFile::SpilledFile(const String & file_name_, const FileProviderPtr & file_provider_)
    : Poco::File(file_name_)
    , details(0, 0, 0)
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
        LOG_WARNING(
            getSpilledFileLogger(),
            "Failed to clean spilled file {}, error message: {}",
            path(),
            getCurrentExceptionMessage(false, false));
    }
}

void SpilledFiles::commitSpilledFiles(std::vector<std::unique_ptr<SpilledFile>> && spilled_files)
{
    std::unique_lock lock(spilled_files_mutex);
    for (auto & spilled_file : spilled_files)
    {
        if (!spilled_file->isFull())
            mutable_spilled_files.push_back(std::move(spilled_file));
        else
            immutable_spilled_files.push_back(std::move(spilled_file));
    }
    spilled_files.clear();
}

void SpilledFiles::makeAllSpilledFilesImmutable()
{
    std::lock_guard lock(spilled_files_mutex);
    for (auto & mutable_file : mutable_spilled_files)
    {
        mutable_file->markFull();
        immutable_spilled_files.push_back(std::move(mutable_file));
    }
    mutable_spilled_files.clear();
}

Spiller::Spiller(
    const SpillConfig & config_,
    bool is_input_sorted_,
    UInt64 partition_num_,
    const Block & input_schema_,
    const LoggerPtr & logger_,
    Int64 spill_version_,
    bool release_spilled_file_on_restore_)
    : config(config_)
    , is_input_sorted(is_input_sorted_)
    , partition_num(partition_num_)
    , input_schema(input_schema_)
    , logger(logger_)
    , spill_version(spill_version_)
    , release_spilled_file_on_restore(release_spilled_file_on_restore_)
{
    for (UInt64 i = 0; i < partition_num; ++i)
    {
        spilled_files.push_back(std::make_unique<SpilledFiles>());
    }
    /// if is_input_sorted is true, can not append write because it will break the sort property
    enable_append_write
        = !is_input_sorted && (config.max_spilled_bytes_per_file != 0 || config.max_spilled_rows_per_file != 0);
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
    for (size_t i = 0; i < input_schema.columns(); ++i)
    {
        if (input_schema.getByPosition(i).column != nullptr && input_schema.getByPosition(i).column->isColumnConst())
            const_column_indexes.push_back(i);
    }
    header_without_constants = input_schema;
    removeConstantColumns(header_without_constants);
    if (0 == header_without_constants.columns())
    {
        LOG_WARNING(
            logger,
            "Try to spill blocks containing only constant columns, it is meaningless to spill blocks containing only "
            "constant columns");
        for (UInt64 i = 0; i < partition_num; ++i)
        {
            all_constant_block_rows.push_back(0);
        }
    }
}

void Spiller::removeConstantColumns(Block & block) const
{
    /// note must erase the constant column in reverse order because the index stored in const_column_indexes is based on
    /// the original Block, if the column before the index is removed, the index has to be updated or it becomes invalid index
    for (auto it = const_column_indexes.rbegin(); it != const_column_indexes.rend(); ++it) // NOLINT
    {
        RUNTIME_CHECK_MSG(
            block.getByPosition(*it).column->isColumnConst(),
            "The {}-th column in block must be constant column",
            *it);
        block.erase(*it);
    }
}

CachedSpillHandlerPtr Spiller::createCachedSpillHandler(
    const BlockInputStreamPtr & from,
    UInt64 partition_id,
    const std::function<bool()> & is_cancelled)
{
    return std::make_shared<CachedSpillHandler>(
        this,
        partition_id,
        from,
        config.max_cached_data_bytes_in_spiller,
        is_cancelled);
}

void Spiller::spillBlocksUsingBlockInputStream(
    const BlockInputStreamPtr & block_in,
    UInt64 partition_id,
    const std::function<bool()> & is_cancelled)
{
    assert(block_in);
    auto cached_handler = createCachedSpillHandler(block_in, partition_id, is_cancelled);
    while (cached_handler->batchRead())
        cached_handler->spill();
}

std::pair<std::unique_ptr<SpilledFile>, bool> Spiller::getOrCreateSpilledFile(UInt64 partition_id)
{
    RUNTIME_CHECK_MSG(isSpillFinished() == false, "{}: spill after the spiller is finished.", config.spill_id);
    std::unique_ptr<SpilledFile> spilled_file = nullptr;
    if (enable_append_write)
    {
        auto & partition_spilled_files = spilled_files[partition_id];
        std::lock_guard partition_lock(partition_spilled_files->spilled_files_mutex);
        if (!partition_spilled_files->mutable_spilled_files.empty())
        {
            spilled_file = std::move(partition_spilled_files->mutable_spilled_files.back());
            partition_spilled_files->mutable_spilled_files.pop_back();
        }
    }
    if (spilled_file == nullptr)
    {
        auto spilled_file_name = nextSpillFileName(partition_id);
        spilled_file = std::make_unique<SpilledFile>(spilled_file_name, config.file_provider);
        RUNTIME_CHECK_MSG(
            !spilled_file->exists(),
            "Duplicated spilled file: {}, should not happens",
            spilled_file_name);
        return std::make_pair(std::move(spilled_file), false);
    }
    else
    {
        RUNTIME_CHECK_MSG(spilled_file->exists(), "Missed spilled file: {}, should not happens", spilled_file->path());
        return std::make_pair(std::move(spilled_file), true);
    }
}

SpillHandler Spiller::createSpillHandler(UInt64 partition_id)
{
    RUNTIME_CHECK_MSG(
        partition_id < partition_num,
        "{}: partition id {} exceeds partition num {}.",
        config.spill_id,
        partition_id,
        partition_num);
    RUNTIME_CHECK_MSG(isSpillFinished() == false, "{}: spill after the spiller is finished.", config.spill_id);
    return SpillHandler(this, partition_id);
}

void Spiller::spillBlocks(Blocks && blocks, UInt64 partition_id)
{
    if (blocks.empty())
        return;
    auto spiller_handler = createSpillHandler(partition_id);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_spill_to_disk_failpoint);
    spiller_handler.spillBlocks(std::move(blocks));
    spiller_handler.finish();
}

BlockInputStreams Spiller::restoreBlocks(UInt64 partition_id, UInt64 max_stream_size, bool append_dummy_read_stream)
{
    RUNTIME_CHECK_MSG(
        partition_id < partition_num,
        "{}: partition id {} exceeds partition num {}.",
        config.spill_id,
        partition_id,
        partition_num);
    RUNTIME_CHECK_MSG(isSpillFinished(), "{}: restore before the spiller is finished.", config.spill_id);

    BlockInputStreams ret;
    if unlikely (isAllConstant())
    {
        UInt64 total_rows = 0;
        {
            std::lock_guard lock(all_constant_mutex);
            total_rows = all_constant_block_rows[partition_id];
            if (release_spilled_file_on_restore)
                all_constant_block_rows[partition_id] = 0;
        }
        if (total_rows > 0)
        {
            if (max_stream_size == 0)
                max_stream_size = config.for_all_constant_max_streams;
            std::vector<UInt64> stream_rows;
            stream_rows.resize(max_stream_size, 0);
            size_t index = 0;
            while (total_rows > 0)
            {
                auto cur_rows = std::min(total_rows, config.for_all_constant_block_size);
                total_rows -= cur_rows;
                stream_rows[index++] += cur_rows;
                if (index == stream_rows.size())
                    index = 0;
            }
            for (auto stream_row : stream_rows)
            {
                if (stream_row > 0)
                    ret.push_back(std::make_shared<ConstantsBlockInputStream>(
                        input_schema,
                        stream_row,
                        config.for_all_constant_block_size));
            }
        }
    }
    else
    {
        std::lock_guard partition_lock(spilled_files[partition_id]->spilled_files_mutex);
        RUNTIME_CHECK_MSG(
            spilled_files[partition_id]->mutable_spilled_files.empty(),
            "{}: the mutable spilled files must be empty when restore.",
            config.spill_id);
        auto & partition_spilled_files = spilled_files[partition_id]->immutable_spilled_files;

        if (max_stream_size == 0)
            max_stream_size = partition_spilled_files.size();
        if (is_input_sorted && partition_spilled_files.size() > max_stream_size)
        {
            LOG_WARNING(logger, "Sorted spilled data restore does not take max_stream_size into account");
        }

        SpillDetails details{0, 0, 0};
        UInt64 spill_file_read_stream_num = is_input_sorted ? partition_spilled_files.size()
                                                            : std::min(max_stream_size, partition_spilled_files.size());
        std::vector<UInt64> restore_stream_read_rows;

        if (is_input_sorted)
        {
            for (auto & file : partition_spilled_files)
            {
                RUNTIME_CHECK_MSG(file->exists(), "Spill file {} does not exists", file->path());
                details.merge(file->getSpillDetails());
                std::vector<SpilledFileInfo> file_infos;
                file_infos.emplace_back(file->path());
                restore_stream_read_rows.push_back(file->getSpillDetails().rows);
                if (release_spilled_file_on_restore)
                    file_infos.back().file = std::move(file);
                ret.push_back(std::make_shared<SpilledFilesInputStream>(
                    std::move(file_infos),
                    input_schema,
                    header_without_constants,
                    const_column_indexes,
                    config.file_provider,
                    spill_version));
            }
        }
        else
        {
            std::vector<std::vector<SpilledFileInfo>> file_infos(spill_file_read_stream_num);
            restore_stream_read_rows.resize(spill_file_read_stream_num, 0);
            // todo balance based on SpilledRows
            for (size_t i = 0; i < partition_spilled_files.size(); ++i)
            {
                auto & file = partition_spilled_files[i];
                RUNTIME_CHECK_MSG(file->exists(), "Spill file {} does not exists", file->path());
                details.merge(file->getSpillDetails());
                file_infos[i % spill_file_read_stream_num].emplace_back(file->path());
                restore_stream_read_rows[i % spill_file_read_stream_num] += file->getSpillDetails().rows;
                if (release_spilled_file_on_restore)
                    file_infos[i % spill_file_read_stream_num].back().file = std::move(file);
            }
            for (UInt64 i = 0; i < spill_file_read_stream_num; ++i)
            {
                if (likely(!file_infos[i].empty()))
                    ret.push_back(std::make_shared<SpilledFilesInputStream>(
                        std::move(file_infos[i]),
                        input_schema,
                        header_without_constants,
                        const_column_indexes,
                        config.file_provider,
                        spill_version));
            }
        }
        for (size_t i = 0; i < spill_file_read_stream_num; ++i)
            LOG_TRACE(logger, "Restore {} rows from {}-th stream", restore_stream_read_rows[i], i);
        LOG_INFO(
            logger,
            "Will restore {} rows from {} files of size {:.3f} MiB compressed, {:.3f} MiB uncompressed using {} "
            "streams.",
            details.rows,
            spilled_files[partition_id]->immutable_spilled_files.size(),
            (details.data_bytes_compressed / 1048576.0),
            (details.data_bytes_uncompressed / 1048576.0),
            ret.size());
        if (release_spilled_file_on_restore)
        {
            /// clear the spilled_files so we can safely assume that the element in spilled_files is always not nullptr
            partition_spilled_files.clear();
        }
    }

    if (ret.empty())
    {
        ret.push_back(std::make_shared<NullBlockInputStream>(input_schema));
    }
    if (append_dummy_read_stream)
    {
        /// if append_dummy_read_stream = true, make sure at least `max_stream_size`'s streams are returned, will be used in join
        for (UInt64 i = ret.size(); i < max_stream_size; ++i)
            ret.push_back(std::make_shared<NullBlockInputStream>(input_schema));
    }
    return ret;
}

void Spiller::finishSpill()
{
    std::lock_guard lock(spill_finished_mutex);
    spill_finished = true;
    for (auto & partition_spilled_files : spilled_files)
    {
        partition_spilled_files->makeAllSpilledFilesImmutable();
    }
}

UInt64 Spiller::spilledRows(UInt64 partition_id)
{
    RUNTIME_CHECK_MSG(
        partition_id < partition_num,
        "{}: partition id {} exceeds partition num {}.",
        config.spill_id,
        partition_id,
        partition_num);
    RUNTIME_CHECK_MSG(
        isSpillFinished(),
        "{}: spilledBlockDataSize must be called when the spiller is finished.",
        config.spill_id);
    UInt64 ret = 0;

    std::lock_guard partition_lock(spilled_files[partition_id]->spilled_files_mutex);
    for (auto & file : spilled_files[partition_id]->immutable_spilled_files)
        ret += file->getSpilledRows();
    return ret;
}

String Spiller::nextSpillFileName(UInt64 partition_id)
{
    Int64 index = tmp_file_index.fetch_add(1);
    return fmt::format(
        "{}tmp_{}_partition_{}_{}",
        config.spill_dir,
        config.spill_id_as_file_name_prefix,
        partition_id,
        index);
}

std::atomic<Int64> Spiller::tmp_file_index = 0;

} // namespace DB
