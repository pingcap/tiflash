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
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogReader.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/Page/V3/WALStore.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

#include <cassert>
#include <memory>
#include <mutex>

namespace DB::PS::V3
{
WALStorePtr WALStore::create(
    std::function<void(PageEntriesEdit &&)> && restore_callback,
    FileProviderPtr & provider,
    PSDiskDelegatorPtr & delegator)
{
    auto reader = WALStoreReader::create(provider, delegator);
    while (reader->remained())
    {
        auto [ok, edit] = reader->next();
        if (!ok)
        {
            // TODO: Handle error, some error could be ignored.
            // If the file happened to some error,
            // should truncate it to throw away incomplete data.
            reader->throwIfError();
            // else it just run to the end of file.
            break;
        }
        // apply the edit read
        restore_callback(std::move(edit));
    }

    // Create a new LogFile for writing new logs
    auto log_num = reader->logNum() + 1; // TODO: Reuse old log file
    auto * logger = &Poco::Logger::get("WALStore");
    auto [log_file, filename] = WALStore::createLogWriter(delegator, provider, {log_num, 0}, logger, false);
    (void)filename;
    return std::unique_ptr<WALStore>(new WALStore(delegator, provider, std::move(log_file)));
}

WALStore::WALStore(
    const PSDiskDelegatorPtr & delegator_,
    const FileProviderPtr & provider_,
    std::unique_ptr<LogWriter> && cur_log)
    : delegator(delegator_)
    , provider(provider_)
    , log_file(std::move(cur_log))
    , logger(&Poco::Logger::get("WALStore"))
{
}

void WALStore::apply(PageEntriesEdit & edit, const PageVersionType & version, const WriteLimiterPtr & write_limiter)
{
    for (auto & r : edit.getMutRecords())
    {
        r.version = version;
    }
    apply(edit, write_limiter);
}

void WALStore::apply(const PageEntriesEdit & edit, const WriteLimiterPtr & write_limiter)
{
    const String serialized = ser::serializeTo(edit);
    ReadBufferFromString payload(serialized);

    {
        std::lock_guard lock(log_file_mutex);
        log_file->addRecord(payload, serialized.size(), write_limiter);

        // Roll to a new log file
        // TODO: Make it configurable
        if (log_file->writtenBytes() > PAGE_META_ROLL_SIZE)
        {
            auto log_num = log_file->logNumber() + 1;
            auto [new_log_file, filename] = createLogWriter(delegator, provider, {log_num, 0}, logger, false);
            (void)filename;
            log_file.swap(new_log_file);
        }
    }
}

std::tuple<std::unique_ptr<LogWriter>, LogFilename> WALStore::createLogWriter(
    PSDiskDelegatorPtr delegator,
    const FileProviderPtr & provider,
    const std::pair<Format::LogNumberType, Format::LogNumberType> & new_log_lvl,
    Poco::Logger * logger,
    bool manual_flush)
{
    const auto path = delegator->defaultPath(); // TODO: multi-path
    LogFilename log_filename = LogFilename{
        (manual_flush ? LogFileStage::Temporary : LogFileStage::Normal),
        new_log_lvl.first,
        new_log_lvl.second,
        path};
    auto filename = log_filename.filename(log_filename.stage);
    auto fullname = log_filename.fullname(log_filename.stage);
    LOG_FMT_INFO(logger, "Creating log file for writing [fullname={}]", fullname);
    auto log_writer = std::make_unique<LogWriter>(
        fullname,
        provider,
        new_log_lvl.first,
        /*recycle*/ true,
        /*manual_flush*/ manual_flush);
    return {
        std::move(log_writer),
        log_filename};
}

// In order to make `restore` in a reasonable time, we need to compact
// log files.
bool WALStore::compactLogs(const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter)
{
    const auto current_writting_log_num = [this]() {
        std::lock_guard lock(log_file_mutex);
        return log_file->logNumber();
    }();

    LogFilenameSet compact_log_files = WALStoreReader::listAllFiles(delegator, logger);
    for (auto iter = compact_log_files.begin(); iter != compact_log_files.end(); /*empty*/)
    {
        if (iter->log_num >= current_writting_log_num)
            iter = compact_log_files.erase(iter);
        else
            ++iter;
    }
    // In order not to make read amplification too high, only apply compact logs when ...
    if (compact_log_files.size() < 4) // TODO: Make it configurable and check the reasonable of this number
        return false;

    CollapsingPageDirectory collapsing_directory;
    auto reader = WALStoreReader::create(provider, compact_log_files, read_limiter);
    while (reader->remained())
    {
        auto [ok, edit] = reader->next();
        if (!ok)
        {
            // TODO: Handle error, some error could be ignored.
            reader->throwIfError();
            // else it just run to the end of file.
            break;
        }
        collapsing_directory.apply(std::move(edit));
    }

    {
        const auto log_num = reader->logNum();
        // Create a temporary file for compacting log files.
        auto [compact_log, log_filename] = createLogWriter(delegator, provider, {log_num, 1}, logger, /*manual_flush*/ true);
        collapsing_directory.dumpTo(compact_log);
        compact_log->flush(write_limiter);
        compact_log.reset(); // close fd explictly before renaming file.

        // Rename it to be a normal log file.
        const auto temp_fullname = log_filename.fullname(LogFileStage::Temporary);
        const auto normal_fullname = log_filename.fullname(LogFileStage::Normal);
        LOG_FMT_INFO(logger, "Renaming log file to be normal [fullname={}]", temp_fullname);
        auto f = Poco::File{temp_fullname};
        f.renameTo(normal_fullname);
        LOG_FMT_INFO(logger, "Rename log file to normal done [fullname={}]", normal_fullname);
    }

    // Remove compacted log files.
    for (const auto & filename : compact_log_files)
    {
        if (auto f = Poco::File(filename.fullname(LogFileStage::Normal)); f.exists())
        {
            f.remove();
        }
    }
    // TODO: Log more information. duration, num entries, size of compact log file...
    LOG_FMT_INFO(logger, "Compact logs done [num_compacts={}]", compact_log_files.size());
    return true;
}

} // namespace DB::PS::V3
