#pragma once

#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogReader.h>

namespace DB::PS::V3
{
namespace ser
{
String serializeTo(const PageEntriesEdit & edit);
PageEntriesEdit deserializeFrom(std::string_view record);
} // namespace ser

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
    static WALStoreReaderPtr create(FileProviderPtr & provider, PSDiskDelegatorPtr & delegator);

    bool remained() const;

    std::tuple<bool, PageEntriesEdit> next();

    Format::LogNumberType logNum()
    {
        if (reader == nullptr)
            return 0;
        return reader->getLogNumber();
    }

    WALStoreReader(FileProviderPtr & provider_, LogFilenameSet && all_filenames_);

    WALStoreReader(const WALStoreReader &) = delete;
    WALStoreReader & operator=(const WALStoreReader &) = delete;

private:
    bool openNextFile();

    FileProviderPtr provider;
    ReportCollector reporter;

    const LogFilenameSet all_filenames;
    LogFilenameSet::const_iterator next_reading_file;
    std::unique_ptr<LogReader> reader;
    Poco::Logger * logger;
};

} // namespace DB::PS::V3
