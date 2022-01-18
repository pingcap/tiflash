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
#include <Storages/Page/PageDefines.h>
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
        if (!ok)
        {
            // TODO: Handle error
            break;
        }
        // callback(edit); apply to PageDirectory
        restore_callback(std::move(edit));
    }

    // Create a new LogFile for writing new logs
    auto log_num = reader->logNum() + 1; // TODO: Reuse old log file
    auto * logger = &Poco::Logger::get("WALStore");
    auto log_file = WALStore::rollLogWriter(delegator, provider, write_limiter, log_num, logger);
    return std::unique_ptr<WALStore>(new WALStore(delegator, provider, write_limiter, std::move(log_file)));
}

WALStore::WALStore(
    const PSDiskDelegatorPtr & delegator_,
    const FileProviderPtr & provider_,
    const WriteLimiterPtr & write_limiter_,
    std::unique_ptr<LogWriter> && cur_log)
    : delegator(delegator_)
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
    // LOG_FMT_TRACE(logger, "apply [size={}] [ser={}]", serialized.size(), Redact::keyToHexString(serialized.data(), serialized.size()));
    ReadBufferFromString payload(serialized);
    log_file->addRecord(payload, serialized.size());

    // Roll to a new log file
    // TODO: Make it configurable
    if (log_file->writtenBytes() > PAGE_META_ROLL_SIZE)
    {
        auto log_num = log_file->logNumber() + 1;
        auto new_log_file = rollLogWriter(delegator, provider, write_limiter, log_num, logger);
        log_file.swap(new_log_file);
    }
}

std::unique_ptr<LogWriter> WALStore::rollLogWriter(
    PSDiskDelegatorPtr delegator,
    const FileProviderPtr & provider,
    const WriteLimiterPtr & write_limiter,
    Format::LogNumberType new_log_num,
    Poco::Logger * logger)
{
    const auto path = delegator->defaultPath(); // TODO: multi-path
    auto filename = fmt::format("log_{}_0", new_log_num);
    auto fullname = fmt::format("{}/{}", path, filename);
    LOG_FMT_INFO(logger, "Creating log file for writing, [log_num={}] [path={}] [filename={}]", new_log_num, path, filename);
    return std::make_unique<LogWriter>(
        WriteBufferByFileProviderBuilder(
            /*has_checksum=*/false,
            provider,
            fullname,
            EncryptionPath{path, filename},
            true,
            write_limiter)
            .with_buffer_size(Format::BLOCK_SIZE) // Must be `BLOCK_SIZE`
            .build(),
        new_log_num,
        /*recycle*/ true);
}

void WALStore::gc()
{
}

} // namespace DB::PS::V3
