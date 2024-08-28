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

#include <Common/Logger.h>
#include <Common/RedactHelpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <IO/WriteHelpers.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

namespace DB::PS::V3
{
struct LogFile
{
    String filename;
    size_t bytes;
};
using LogFiles = std::vector<LogFile>;

LogFilenameSet WALStoreReader::listAllFiles(const PSDiskDelegatorPtr & delegator, LoggerPtr logger)
{
    // [<parent_path_0, [file0, file1, ...]>, <parent_path_1, [...]>, ...]
    std::vector<std::pair<String, LogFiles>> all_filenames;
    LogFiles filenames;
    for (const auto & parent_path : delegator->listPaths())
    {
        String wal_parent_path = parent_path + WALStore::wal_folder_prefix;
        Poco::File directory(wal_parent_path);
        if (!directory.exists())
        {
            directory.createDirectories();
            continue;
        }

        filenames.clear();
        Poco::DirectoryIterator end;
        for (Poco::DirectoryIterator it(directory); it != end; ++it)
        {
            filenames.emplace_back(LogFile{.filename = it.name(), .bytes = it->getSize()});
        }
        all_filenames.emplace_back(std::make_pair(wal_parent_path, std::move(filenames)));
    }

    LogFilenameSet log_files;
    for (const auto & [parent_path, filenames] : all_filenames)
    {
        for (const auto & file : filenames)
        {
            auto name = LogFilename::parseFrom(parent_path, file.filename, logger, file.bytes);
            switch (name.stage)
            {
            case LogFileStage::Normal:
            {
                log_files.insert(name);
                break;
            }
            case LogFileStage::Temporary:
            {
                break;
            }
            case LogFileStage::Invalid:
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unknown logfile name, parent_path={} filename={}",
                    parent_path,
                    file.filename);
            }
            }
        }
    }
    return log_files;
}

std::tuple<std::optional<LogFilename>, LogFilenameSet> WALStoreReader::findCheckpoint(LogFilenameSet && all_files)
{
    auto latest_checkpoint_iter = all_files.cend();
    for (auto iter = all_files.cbegin(); iter != all_files.cend(); ++iter)
    {
        if (iter->level_num > 0)
        {
            latest_checkpoint_iter = iter;
        }
    }
    if (latest_checkpoint_iter == all_files.cend())
    {
        return {std::nullopt, std::move(all_files)};
    }

    LogFilename latest_checkpoint = *latest_checkpoint_iter;
    for (auto iter = all_files.cbegin(); iter != all_files.cend(); /*empty*/)
    {
        // We use <largest_num, 1> as the checkpoint, so all files less than or equal
        // to latest_checkpoint.log_num can be erase
        if (iter->log_num <= latest_checkpoint.log_num)
        {
            if (iter->log_num == latest_checkpoint.log_num && iter->level_num != 0)
            {
                // the checkpoint file, not remove
            }
            else
            {
                // TODO: clean useless file that is older than `checkpoint`
            }
            iter = all_files.erase(iter);
        }
        else
        {
            ++iter;
        }
    }
    return {latest_checkpoint, std::move(all_files)};
}

WALStoreReaderPtr WALStoreReader::create(
    String storage_name,
    FileProviderPtr & provider,
    LogFilenameSet files,
    WALRecoveryMode recovery_mode_,
    const ReadLimiterPtr & read_limiter)
{
    auto [checkpoint, files_to_read] = findCheckpoint(std::move(files));
    auto reader = std::make_shared<WALStoreReader>(
        storage_name,
        provider,
        checkpoint,
        std::move(files_to_read),
        recovery_mode_,
        read_limiter);
    reader->openNextFile();
    return reader;
}

WALStoreReaderPtr WALStoreReader::create(
    String storage_name,
    FileProviderPtr & provider,
    PSDiskDelegatorPtr & delegator,
    WALRecoveryMode recovery_mode_,
    const ReadLimiterPtr & read_limiter)
{
    LogFilenameSet log_files = listAllFiles(delegator, Logger::get(storage_name));
    return create(std::move(storage_name), provider, std::move(log_files), recovery_mode_, read_limiter);
}

LogReaderPtr WALStoreReader::createLogReader(
    const LogFilename & filename,
    FileProviderPtr & provider,
    ReportCollector * reporter,
    WALRecoveryMode recovery_mode,
    const ReadLimiterPtr & read_limiter,
    LoggerPtr logger)
{
    const auto log_num = filename.log_num;
    const auto fullname = filename.fullname(filename.stage);
    Poco::File f(fullname);
    const auto file_size = f.getSize();
    LOG_INFO(logger, "Open log file for reading, file={} size={}", fullname, file_size);
    auto read_buf = ReadBufferFromRandomAccessFileBuilder::buildPtr(
        provider,
        fullname,
        EncryptionPath{fullname, ""},
        Format::BLOCK_SIZE, // Must be `Format::BLOCK_SIZE`
        read_limiter);
    return std::make_unique<LogReader>(
        std::move(read_buf),
        reporter,
        /*verify_checksum*/ true,
        log_num,
        recovery_mode);
}

String WALStoreReader::getLastRecordInLogFile(
    const LogFilename & filename,
    FileProviderPtr & provider,
    WALRecoveryMode recovery_mode,
    const ReadLimiterPtr & read_limiter,
    LoggerPtr logger)
{
    ReportCollector reporter;
    auto log_reader = createLogReader(filename, provider, &reporter, recovery_mode, read_limiter, logger);
    String last_record;
    while (true)
    {
        auto [ok, record] = log_reader->readRecord();
        if (!ok)
            break;

        last_record = std::move(record);
    }
    return last_record;
}

WALStoreReader::WALStoreReader(
    String storage_name,
    FileProviderPtr & provider_,
    std::optional<LogFilename> checkpoint,
    LogFilenameSet && files_,
    WALRecoveryMode recovery_mode_,
    const ReadLimiterPtr & read_limiter_)
    : provider(provider_)
    , read_limiter(read_limiter_)
    , checkpoint_reader_created(!checkpoint.has_value())
    , reading_checkpoint_file(false)
    , checkpoint_file(checkpoint)
    , files_to_read(std::move(files_))
    , next_reading_file(files_to_read.begin())
    , recovery_mode(recovery_mode_)
    , logger(Logger::get(storage_name))
{}

bool WALStoreReader::remained() const
{
    if (reader == nullptr)
        return false;

    if (!reader->isEOF())
        return true;
    if (checkpoint_reader_created && next_reading_file != files_to_read.end())
        return true;
    return false;
}

UInt64 WALStoreReader::getSnapSeqForCheckpoint() const
{
    return checkpoint_file.has_value() ? checkpoint_file->snap_seq : 0;
}

std::pair<bool, std::optional<String>> WALStoreReader::next()
{
    bool ok = false;
    String record;
    do
    {
        std::tie(ok, record) = reader->readRecord();
        if (ok)
        {
            return std::make_pair(reading_checkpoint_file, record);
        }

        // Roll to read the next file
        if (bool next_file = openNextFile(); !next_file)
        {
            // No more file to be read.
            return std::make_pair(false, std::nullopt);
        }
    } while (true);
}

bool WALStoreReader::openNextFile()
{
    if (checkpoint_reader_created && next_reading_file == files_to_read.end())
    {
        return false;
    }

    if (!checkpoint_reader_created)
    {
        reader = createLogReader(*checkpoint_file, provider, &reporter, recovery_mode, read_limiter, logger);
        checkpoint_reader_created = true;
        reading_checkpoint_file = true;
    }
    else
    {
        reader = createLogReader(*next_reading_file, provider, &reporter, recovery_mode, read_limiter, logger);
        ++next_reading_file;
        reading_checkpoint_file = false;
    }
    return true;
}

} // namespace DB::PS::V3
