#pragma once

#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogReader.h>

namespace DB
{
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;

namespace PS::V3
{
class ReportCollector : public LogReader::Reporter
{
public:
    void corruption(size_t /*bytes*/, const String & /*msg*/) override
    {
        error_happened = true;
        // FIXME: store the reason of corruption
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
    static LogFilenameSet listAllFiles(const PSDiskDelegatorPtr & delegator, Poco::Logger * logger);
    static std::tuple<std::optional<LogFilename>, LogFilenameSet>
    findCheckpoint(LogFilenameSet && all_files);

    static WALStoreReaderPtr create(FileProviderPtr & provider, LogFilenameSet files, const ReadLimiterPtr & read_limiter = nullptr);

    static WALStoreReaderPtr create(FileProviderPtr & provider, PSDiskDelegatorPtr & delegator, const ReadLimiterPtr & read_limiter = nullptr);

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
        FileProviderPtr & provider_,
        std::optional<LogFilename> checkpoint,
        LogFilenameSet && files_,
        const ReadLimiterPtr & read_limiter_);

    WALStoreReader(const WALStoreReader &) = delete;
    WALStoreReader & operator=(const WALStoreReader &) = delete;

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

    Poco::Logger * logger;
};

} // namespace PS::V3
} // namespace DB
