// Copyright 2022 PingCAP, Ltd.
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
#include <Common/SyncPoint/SyncPoint.h>
#include <Encryption/FileProvider.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Path.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogReader.h>
#include <Storages/Page/V3/PageDefines.h>
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
    WALConfig config)
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
    WALConfig config_)
    : storage_name(std::move(storage_name_))
    , delegator(delegator_)
    , provider(provider_)
    , last_log_num(last_log_num_)
    , wal_paths_index(0)
    , num_log_files(0)
    , bytes_on_disk(0)
    , logger(Logger::get(storage_name))
    , config(config_)
{
}

void WALStore::apply(String && serialized_edit, const WriteLimiterPtr & write_limiter)
{
    ReadBufferFromString payload(serialized_edit);

    {
        std::lock_guard lock(log_file_mutex);
        if (log_file == nullptr || log_file->writtenBytes() > config.roll_size)
        {
            // Roll to a new log file
            rollToNewLogWriter(lock);
        }

        log_file->addRecord(payload, serialized_edit.size(), write_limiter);
    }
}

Format::LogNumberType WALStore::rollToNewLogWriter(const std::lock_guard<std::mutex> &)
{
    // Roll to a new log file
    auto log_num = last_log_num++;
    auto [new_log_file, filename] = createLogWriter({log_num, 0}, false);
    UNUSED(filename);
    log_file.swap(new_log_file);
    return log_num;
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
        0,
        path};
    auto filename = log_filename.filename(log_filename.stage);
    auto fullname = log_filename.fullname(log_filename.stage);
    // TODO check whether the file already existed
    LOG_INFO(logger, "Creating log file for writing [fullname={}]", fullname);
    auto log_writer = std::make_unique<LogWriter>(
        fullname,
        provider,
        new_log_lvl.first,
        /*recycle*/ true,
        /*manual_flush*/ manual_flush);
    return {std::move(log_writer), log_filename};
}

void WALStore::updateDiskUsage(const LogFilenameSet & log_filenames)
{
    size_t n_bytes_on_disk = 0;
    for (const auto & f : log_filenames)
    {
        n_bytes_on_disk += f.bytes_on_disk;
    }
    {
        std::lock_guard guard(mtx_disk_usage);
        num_log_files = log_filenames.size();
        bytes_on_disk = n_bytes_on_disk;
    }
}

WALStore::FilesSnapshot WALStore::tryGetFilesSnapshot(size_t max_persisted_log_files, bool force)
{
    // First we simply check whether the number of files is enough for compaction
    LogFilenameSet persisted_log_files = WALStoreReader::listAllFiles(delegator, logger);
    updateDiskUsage(persisted_log_files);
    if (!force && persisted_log_files.size() <= max_persisted_log_files)
    {
        return WALStore::FilesSnapshot{};
    }

    // There could be some new-log-files generated before we acquire the lock.
    // But full GC will not run concurrently when dumping snapshot. So ignoring
    // the new files is safe.
    Format::LogNumberType current_writing_log_num = 0;
    {
        std::lock_guard lock(log_file_mutex); // block other writes
        if (log_file == nullptr && !force)
        {
            // `log_file` is empty means there is no new writes
            // after WALStore created. Just return an invalid snapshot
            // if `force == false`.
            return WALStore::FilesSnapshot{};
        }
        // Reset the `log_file` so that next edit will be written to a
        // new file and update the `last_log_num`
        current_writing_log_num = last_log_num;
        log_file.reset();
    }

    for (auto iter = persisted_log_files.begin(); iter != persisted_log_files.end(); /*empty*/)
    {
        if (iter->log_num >= current_writing_log_num)
            iter = persisted_log_files.erase(iter);
        else
            ++iter;
    }
    return WALStore::FilesSnapshot{
        .persisted_log_files = std::move(persisted_log_files),
    };
}

// In order to make `restore` in a reasonable time, we need to compact
// log files.
bool WALStore::saveSnapshot(
    FilesSnapshot && files_snap,
    String && serialized_snap,
    size_t num_records,
    const WriteLimiterPtr & write_limiter)
{
    if (files_snap.persisted_log_files.empty())
        return false;

    LOG_INFO(logger, "Saving directory snapshot [num_records={}]", num_records);

    // Use {largest_log_num, 1} to save the `edit`
    const auto log_num = files_snap.persisted_log_files.rbegin()->log_num;
    // Create a temporary file for saving directory snapshot
    auto [compact_log, log_filename] = createLogWriter({log_num, 1}, /*manual_flush*/ true);

    ReadBufferFromString payload(serialized_snap);

    compact_log->addRecord(payload, serialized_snap.size(), write_limiter, /*background*/ true);
    compact_log->flush(write_limiter, /*background*/ true);
    compact_log.reset(); // close fd explicitly before renaming file.

    // Rename it to be a normal log file.
    const auto temp_fullname = log_filename.fullname(LogFileStage::Temporary);
    const auto normal_fullname = log_filename.fullname(LogFileStage::Normal);

    LOG_INFO(logger, "Renaming log file to be normal [fullname={}]", temp_fullname);
    // Use `renameFile` from FileProvider that take good care of encryption path
    provider->renameFile(
        temp_fullname,
        EncryptionPath(temp_fullname, ""),
        normal_fullname,
        EncryptionPath(normal_fullname, ""),
        true);
    LOG_INFO(logger, "Rename log file to normal done [fullname={}]", normal_fullname);

    // Remove compacted log files.
    for (const auto & filename : files_snap.persisted_log_files)
    {
        const auto log_fullname = filename.fullname(LogFileStage::Normal);
        provider->deleteRegularFile(log_fullname, EncryptionPath(log_fullname, ""));
    }

    auto get_logging_str = [&]() {
        FmtBuffer fmt_buf;
        fmt_buf.append("Dumped directory snapshot to log file done. [files_snapshot=");
        fmt_buf.joinStr(
            files_snap.persisted_log_files.begin(),
            files_snap.persisted_log_files.end(),
            [](const auto & arg, FmtBuffer & fb) {
                fb.append(arg.filename(arg.stage));
            },
            ", ");
        fmt_buf.fmtAppend("] [num_records={}] [file={}] [size={}].",
                          num_records,
                          normal_fullname,
                          serialized_snap.size());
        return fmt_buf.toString();
    };
    LOG_INFO(logger, get_logging_str());

    return true;
}

} // namespace DB::PS::V3
