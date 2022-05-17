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

#include <Common/nocopyable.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogReader.h>
#include <Storages/Page/V3/WALStore.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
}

class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;

namespace PS::V3
{
class ReportCollector : public LogReader::Reporter
{
public:
    void corruption(size_t /*bytes*/, const String & msg) override
    {
        error_happened = true;
        // FIXME: store the reason of corruption
        throw Exception(msg, ErrorCodes::CORRUPTED_DATA);
    }

    bool hasError() const
    {
        return error_happened;
    }

private:
    bool error_happened = false;
};

class WALStoreReader
{
public:
    static LogFilenameSet listAllFiles(const PSDiskDelegatorPtr & delegator, LoggerPtr logger);
    static std::tuple<std::optional<LogFilename>, LogFilenameSet>
    findCheckpoint(LogFilenameSet && all_files);

    static WALStoreReaderPtr create(String storage_name,
                                    FileProviderPtr & provider,
                                    LogFilenameSet files,
                                    WALRecoveryMode recovery_mode_ = WALRecoveryMode::TolerateCorruptedTailRecords,
                                    const ReadLimiterPtr & read_limiter = nullptr);

    static WALStoreReaderPtr create(String storage_name,
                                    FileProviderPtr & provider,
                                    PSDiskDelegatorPtr & delegator,
                                    WALRecoveryMode recovery_mode_ = WALRecoveryMode::TolerateCorruptedTailRecords,
                                    const ReadLimiterPtr & read_limiter = nullptr);

    bool remained() const;

    std::tuple<bool, PageEntriesEdit> next();

    void throwIfError() const
    {
        if (reporter.hasError())
        {
            throw Exception("Something wrong while reading log file");
        }
    }

    Format::LogNumberType lastLogNum() const
    {
        if (!files_to_read.empty())
            return files_to_read.rbegin()->log_num;
        if (checkpoint_file)
            return checkpoint_file->log_num + 1;
        return 0;
    }

    WALStoreReader(
        String storage_name,
        FileProviderPtr & provider_,
        std::optional<LogFilename> checkpoint,
        LogFilenameSet && files_,
        WALRecoveryMode recovery_mode_,
        const ReadLimiterPtr & read_limiter_);

    DISALLOW_COPY(WALStoreReader);

private:
    bool openNextFile();

    FileProviderPtr provider;
    ReportCollector reporter;
    const ReadLimiterPtr read_limiter;

    bool checkpoint_read_done;
    const std::optional<LogFilename> checkpoint_file;
    const LogFilenameSet files_to_read;
    LogFilenameSet::const_iterator next_reading_file;
    std::unique_ptr<LogReader> reader;

    WALRecoveryMode recovery_mode;
    LoggerPtr logger;
};

} // namespace PS::V3
} // namespace DB
