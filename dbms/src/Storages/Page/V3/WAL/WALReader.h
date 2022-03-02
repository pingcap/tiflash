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
    static LogFilenameSet listAllFiles(PSDiskDelegatorPtr & delegator, Poco::Logger * logger);

    static WALStoreReaderPtr create(FileProviderPtr & provider, LogFilenameSet files, const ReadLimiterPtr & read_limiter = nullptr);

    static WALStoreReaderPtr create(FileProviderPtr & provider, PSDiskDelegatorPtr & delegator, const ReadLimiterPtr & read_limiter = nullptr);

    bool remained() const;

    std::tuple<bool, PageEntriesEdit> next();

    void throwIfError() const
    {
        if (reporter.hasError())
        {
            throw Exception("Something worong while reading log file");
        }
    }

    Format::LogNumberType logNum() const
    {
        if (reader == nullptr)
            return 0;
        return reader->getLogNumber();
    }

    WALStoreReader(FileProviderPtr & provider_, LogFilenameSet && files_, const ReadLimiterPtr & read_limiter_ = nullptr);

    WALStoreReader(const WALStoreReader &) = delete;
    WALStoreReader & operator=(const WALStoreReader &) = delete;

private:
    bool openNextFile();

    FileProviderPtr provider;
    ReportCollector reporter;

    const LogFilenameSet files;
    LogFilenameSet::const_iterator next_reading_file;
    const ReadLimiterPtr read_limiter;
    std::unique_ptr<LogReader> reader;
    Poco::Logger * logger;
};

} // namespace PS::V3
} // namespace DB
