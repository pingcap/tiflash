#pragma once

#include <Encryption/FileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <Storages/Page/V3/LogFile/LogReader.h>

namespace DB::PS::V3
{
namespace ser
{
String serializeTo(const PageEntriesEdit & edit);
}

class ReportCollector : public LogReader::Reporter
{
public:
    void corruption(size_t /*bytes*/, const String & /*msg*/) override
    {
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

    WALStoreReader(FileProviderPtr & provider_, std::vector<std::pair<String, Strings>> && all_filenames_);

    WALStoreReader(const WALStoreReader &) = delete;
    WALStoreReader & operator=(const WALStoreReader &) = delete;

private:
    bool openNextFile();

    FileProviderPtr provider;
    ReportCollector reporter;

    size_t all_files_read_index = 0;
    std::vector<std::pair<String, Strings>> all_filenames;
    std::unique_ptr<LogReader> reader;
    Poco::Logger * logger;
};

} // namespace DB::PS::V3
