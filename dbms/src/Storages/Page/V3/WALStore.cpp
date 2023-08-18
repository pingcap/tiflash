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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Encryption/FileProvider.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogReader.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

#include <cassert>
#include <memory>
#include <mutex>

namespace DB::PS::V3
{
std::pair<WALStorePtr, WALStoreReaderPtr> WALStore::create(
    String storage_name,
    FileProviderPtr & provider,
    PSDiskDelegatorPtr & delegator,
    WALStore::Config config)
{
    auto reader = WALStoreReader::create(storage_name,
                                         provider,
                                         delegator,
                                         config.getRecoverMode());
    // Create a new LogFile for writing new logs
    auto last_log_num = reader->lastLogNum() + 1; // TODO reuse old file
    return {
        std::unique_ptr<WALStore>(new WALStore(std::move(storage_name), delegator, provider, last_log_num, std::move(config))),
        reader};
}

WALStoreReaderPtr WALStore::createReaderForFiles(const String & identifier, const LogFilenameSet & log_filenames, const ReadLimiterPtr & read_limiter)
{
    return WALStoreReader::create(identifier, provider, log_filenames, config.getRecoverMode(), read_limiter);
}

WALStore::WALStore(
    String storage_name_,
    const PSDiskDelegatorPtr & delegator_,
    const FileProviderPtr & provider_,
    Format::LogNumberType last_log_num_,
    WALStore::Config config_)
    : storage_name(std::move(storage_name_))
    , delegator(delegator_)
    , provider(provider_)
    , last_log_num(last_log_num_)
    , wal_paths_index(0)
    , logger(Logger::get("WALStore", storage_name))
    , config(config_)
{
}

void WALStore::apply(PageEntriesEdit & edit, const PageVersion & version, const WriteLimiterPtr & write_limiter)
{
    for (auto & r : edit.getMutRecords())
    {
        r.version = version;
    }
    apply(edit, write_limiter);
}

void WALStore::apply(const PageEntriesEdit & edit, const WriteLimiterPtr & write_limiter)
{
    const String serialized = ser::serializeTo(edit);
    ReadBufferFromString payload(serialized);

    {
        std::lock_guard lock(log_file_mutex);
        // Roll to a new log file
        // TODO: Make it configurable
        if (log_file == nullptr || log_file->writtenBytes() > config.roll_size)
        {
            auto log_num = last_log_num++;
            auto [new_log_file, filename] = createLogWriter({log_num, 0}, false);
            (void)filename;
            log_file.swap(new_log_file);
        }

        log_file->addRecord(payload, serialized.size(), write_limiter);
    }
}

std::tuple<std::unique_ptr<LogWriter>, LogFilename> WALStore::createLogWriter(
    const std::pair<Format::LogNumberType, Format::LogNumberType> & new_log_lvl,
    bool manual_flush)
{
    String path;

    if (delegator->numPaths() == 1)
    {
        path = delegator->defaultPath();
    }
    else
    {
        const auto & paths = delegator->listPaths();

        if (wal_paths_index >= paths.size())
        {
            wal_paths_index = 0;
        }
        path = paths[wal_paths_index];
        wal_paths_index++;
    }

    path += wal_folder_prefix;

    LogFilename log_filename = LogFilename{
        (manual_flush ? LogFileStage::Temporary : LogFileStage::Normal),
        new_log_lvl.first,
        new_log_lvl.second,
        path};
    auto filename = log_filename.filename(log_filename.stage);
    auto fullname = log_filename.fullname(log_filename.stage);
    // TODO check whether the file already existed
    LOG_FMT_INFO(logger, "Creating log file for writing [fullname={}]", fullname);
    auto log_writer = std::make_unique<LogWriter>(
        fullname,
        provider,
        new_log_lvl.first,
        /*recycle*/ true,
        /*manual_flush*/ manual_flush);
    return {
        std::move(log_writer),
        log_filename};
}

WALStore::FilesSnapshot WALStore::getFilesSnapshot() const
{
    const auto [ok, current_writting_log_num] = [this]() -> std::tuple<bool, Format::LogNumberType> {
        std::lock_guard lock(log_file_mutex);
        if (!log_file)
        {
            return {false, 0};
        }
        return {true, log_file->logNumber()};
    }();
    // Return empty set if `log_file` is not ready
    if (!ok)
    {
        return WALStore::FilesSnapshot{
            .current_writting_log_num = 0,
            .persisted_log_files = {},
        };
    }

    // Only those files are totally persisted
    LogFilenameSet persisted_log_files = WALStoreReader::listAllFiles(delegator, logger);
    for (auto iter = persisted_log_files.begin(); iter != persisted_log_files.end(); /*empty*/)
    {
        if (iter->log_num >= current_writting_log_num)
            iter = persisted_log_files.erase(iter);
        else
            ++iter;
    }
    return WALStore::FilesSnapshot{
        .current_writting_log_num = current_writting_log_num,
        .persisted_log_files = std::move(persisted_log_files),
    };
}

// In order to make `restore` in a reasonable time, we need to compact
// log files.
bool WALStore::saveSnapshot(FilesSnapshot && files_snap, PageEntriesEdit && directory_snap, const WriteLimiterPtr & write_limiter)
{
    if (files_snap.persisted_log_files.empty())
        return false;

    LOG_FMT_INFO(logger, "Saving directory snapshot");

    // Use {largest_log_num, 1} to save the `edit`
    const auto log_num = files_snap.persisted_log_files.rbegin()->log_num;
    // Create a temporary file for saving directory snapshot
    auto [compact_log, log_filename] = createLogWriter({log_num, 1}, /*manual_flush*/ true);

    const String serialized = ser::serializeTo(directory_snap);
    ReadBufferFromString payload(serialized);

    compact_log->addRecord(payload, serialized.size());
    compact_log->flush(write_limiter, /*background*/ true);
    compact_log.reset(); // close fd explicitly before renaming file.

    // Rename it to be a normal log file.
    const auto temp_fullname = log_filename.fullname(LogFileStage::Temporary);
    const auto normal_fullname = log_filename.fullname(LogFileStage::Normal);

    LOG_FMT_INFO(logger, "Renaming log file to be normal [fullname={}]", temp_fullname);
    // Use `renameFile` from FileProvider that take good care of encryption path
    provider->renameFile(
        temp_fullname,
        EncryptionPath(temp_fullname, ""),
        normal_fullname,
        EncryptionPath(normal_fullname, ""),
        true);
    LOG_FMT_INFO(logger, "Rename log file to normal done [fullname={}]", normal_fullname);

    // Remove compacted log files.
    for (const auto & filename : files_snap.persisted_log_files)
    {
        const auto log_fullname = filename.fullname(LogFileStage::Normal);
        provider->deleteRegularFile(log_fullname, EncryptionPath(log_fullname, ""));
    }

    FmtBuffer fmt_buf;
    fmt_buf.append("Dumped directory snapshot to log file done. [files_snapshot=");

    fmt_buf.joinStr(
        files_snap.persisted_log_files.begin(),
        files_snap.persisted_log_files.end(),
        [](const auto & arg, FmtBuffer & fb) {
            fb.fmtAppend("{}", arg.filename(arg.stage));
        },
        ", ");
    fmt_buf.fmtAppend("] [num of records={}] [file={}] [size={}].",
                      directory_snap.size(),
                      normal_fullname,
                      serialized.size());

    LOG_INFO(logger, fmt_buf.toString());

    return true;
}

} // namespace DB::PS::V3
