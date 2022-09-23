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

#pragma once

#include <Common/Checksum.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogWriter.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/WAL/WALConfig.h>
#include <common/types.h>

#include <memory>

namespace DB
{
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;
namespace PS::V3
{

class WALStore;
using WALStorePtr = std::unique_ptr<WALStore>;

class WALStoreReader;
using WALStoreReaderPtr = std::shared_ptr<WALStoreReader>;

class WALStore
{
public:
    constexpr static const char * wal_folder_prefix = "/wal";

    static std::pair<WALStorePtr, WALStoreReaderPtr>
    create(
        String storage_name_,
        FileProviderPtr & provider,
        PSDiskDelegatorPtr & delegator,
        WALConfig config);

    WALStoreReaderPtr createReaderForFiles(const String & identifier, const LogFilenameSet & log_filenames, const ReadLimiterPtr & read_limiter);

    void apply(String && serialized_edit, const WriteLimiterPtr & write_limiter = nullptr);

    struct FilesSnapshot
    {
        Format::LogNumberType current_writing_log_num;
        // The log files to generate snapshot from. Sorted by <log number, log level>.
        // If the WAL log file is not inited, it is an empty set.
        LogFilenameSet persisted_log_files;

        // Note that persisted_log_files should not be empty for needSave() == true,
        // cause we get the largest log num from persisted_log_files as the new
        // file name.
        bool needSave(const size_t max_size) const
        {
            return persisted_log_files.size() > max_size;
        }
    };

    FilesSnapshot getFilesSnapshot() const;

    bool saveSnapshot(
        FilesSnapshot && files_snap,
        String && serialized_snap,
        size_t num_records,
        const WriteLimiterPtr & write_limiter = nullptr);

    const String & name() { return storage_name; }

private:
    WALStore(
        String storage_name,
        const PSDiskDelegatorPtr & delegator_,
        const FileProviderPtr & provider_,
        Format::LogNumberType last_log_num_,
        WALConfig config);

    std::tuple<std::unique_ptr<LogWriter>, LogFilename>
    createLogWriter(
        const std::pair<Format::LogNumberType, Format::LogNumberType> & new_log_lvl,
        bool manual_flush);

private:
    const String storage_name;
    PSDiskDelegatorPtr delegator;
    FileProviderPtr provider;
    mutable std::mutex log_file_mutex;
    Format::LogNumberType last_log_num;
    // select next path for creating new logfile
    UInt32 wal_paths_index;
    std::unique_ptr<LogWriter> log_file;

    LoggerPtr logger;

    WALConfig config;
};

} // namespace PS::V3
} // namespace DB
