#include <Common/Exception.h>
#include <Encryption/EncryptionPath.h>
#include <Encryption/FileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogReader.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

#include <cassert>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes
} // namespace DB
namespace DB::PS::V3
{
/**
 * A class for iterating all LogFiles and replay the `PageEntriesEdit`
 * from those files.
 */
class WALStoreReader
{
public:
    static WALStoreReaderPtr create(FileProviderPtr & /*provider*/, std::vector<std::pair<String, Strings>> && /*all_filenames*/)
    {
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool remained() const;
    std::tuple<bool, PageEntriesEdit> next();

    Format::LogNumberType logNum() const
    {
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    WALStoreReader(FileProviderPtr & /*provider_*/, std::vector<std::pair<String, Strings>> && /*all_filenames_*/) {}

    WALStoreReader(const WALStoreReader &) = delete;
    WALStoreReader & operator=(const WALStoreReader &) = delete;
};

bool WALStoreReader::remained() const
{
    return false;
}

std::tuple<bool, PageEntriesEdit> WALStoreReader::next()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

WALStoreReaderPtr WALStore::createReader(FileProviderPtr & provider, PSDiskDelegatorPtr & delegator)
{
    std::vector<std::pair<String, Strings>> all_filenames;
    {
        Strings filenames;
        for (const auto & p : delegator->listPaths())
        {
            Poco::File directory(p);
            if (!directory.exists())
                directory.createDirectories();
            filenames.clear();
            directory.list(filenames);
            all_filenames.emplace_back(std::make_pair(p, std::move(filenames)));
            filenames.clear();
        }
        ASSERT(all_filenames.size() == 1); // TODO: multi-path
    }

    return std::make_shared<WALStoreReader>(provider, std::move(all_filenames));
}

WALStorePtr WALStore::create(
    FileProviderPtr & provider,
    PSDiskDelegatorPtr & delegator,
    const WriteLimiterPtr & write_limiter)
{
    auto reader = createReader(provider, delegator);
    while (reader->remained())
    {
        auto [ok, edit] = reader->next();
        (void)ok;
        (void)edit;
        // callback(edit); apply to PageDirectory
        reader->next();
    }

    // Create a new LogFile for writing new logs
    const auto path = delegator->defaultPath(); // TODO: multi-path
    auto log_num = reader->logNum() + 1; // TODO: Reuse old log file
    auto filename = fmt::format("log_{}", log_num);
    auto fullname = fmt::format("{}/{}", path, filename);
    LOG_FMT_INFO(&Poco::Logger::get("WALStore"), "Creating log file for writing, [log_num={}] [path={}] [filename={}]", log_num, path, filename);
    WriteBufferByFileProviderBuilder builder(
        /*has_checksum=*/false,
        provider,
        fullname,
        EncryptionPath{path, filename},
        true,
        write_limiter);
    auto log_file = std::make_unique<LogWriter>(builder.with_buffer_size(Format::BLOCK_SIZE).build(), log_num, /*recycle*/ true);
    return std::unique_ptr<WALStore>(new WALStore(path, provider, write_limiter, std::move(log_file)));
}

WALStore::WALStore(
    const String & path_,
    const FileProviderPtr & provider_,
    const WriteLimiterPtr & write_limiter_,
    std::unique_ptr<LogWriter> && cur_log)
    : path(path_)
    , provider(provider_)
    , write_limiter(write_limiter_)
    , log_file(std::move(cur_log))
    , logger(&Poco::Logger::get("WALStore"))
{
}

void WALStore::apply(PageEntriesEdit & edit, const PageVersionType & version)
{
    for (auto & r : edit.getMutRecords())
    {
        r.version = version;
    }
    apply(edit);
}

void WALStore::apply(const PageEntriesEdit & /*edit*/)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void WALStore::gc()
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

} // namespace DB::PS::V3
