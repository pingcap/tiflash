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
        // FIXME: handle corruption
    }
};

class WALStoreReader
{
public:
    static LogFilenameSet listAllFiles(PSDiskDelegatorPtr & delegator, Poco::Logger * logger);

    static WALStoreReaderPtr create(FileProviderPtr & provider, LogFilenameSet files);

    static WALStoreReaderPtr create(FileProviderPtr & provider, PSDiskDelegatorPtr & delegator);

    bool remained() const;

    std::tuple<bool, PageEntriesEdit> next();

    Format::LogNumberType logNum()
    {
        if (reader == nullptr)
            return 0;
        return reader->getLogNumber();
    }

    WALStoreReader(FileProviderPtr & provider_, LogFilenameSet && files_);

    WALStoreReader(const WALStoreReader &) = delete;
    WALStoreReader & operator=(const WALStoreReader &) = delete;

private:
    bool openNextFile();

    FileProviderPtr provider;
    ReportCollector reporter;

    const LogFilenameSet files;
    LogFilenameSet::const_iterator next_reading_file;
    std::unique_ptr<LogReader> reader;
    Poco::Logger * logger;
};

} // namespace PS::V3
} // namespace DB
