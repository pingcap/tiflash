#include <Common/Exception.h>
#include <Common/RedactHelpers.h>
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
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

#include <cassert>
#include <memory>

namespace DB::PS::V3
{
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
    std::function<void(PageEntriesEdit &&)> && restore_callback,
    FileProviderPtr & provider,
    PSDiskDelegatorPtr & delegator,
    const WriteLimiterPtr & write_limiter)
{
    auto reader = WALStoreReader::create(provider, delegator);
    while (reader->remained())
    {
        auto [ok, edit] = reader->next();
        (void)ok;
        (void)edit;
        // callback(edit); apply to PageDirectory
        restore_callback(std::move(edit));
    }

    // Create a new LogFile for writing new logs
    const auto path = delegator->defaultPath(); // TODO: multi-path
    auto log_num = reader->logNum() + 1; // TODO: Reuse old log file
    auto filename = fmt::format("log_{}_0", log_num);
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

void WALStore::apply(const PageEntriesEdit & edit)
{
    const String serialized = ser::serializeTo(edit);
    LOG_FMT_TRACE(logger, "apply [size={}] [ser={}]", serialized.size(), Redact::keyToHexString(serialized.data(), serialized.size()));
    ReadBufferFromString payload(serialized);
    log_file->addRecord(payload, serialized.size());
}

void WALStore::gc()
{
}

} // namespace DB::PS::V3
