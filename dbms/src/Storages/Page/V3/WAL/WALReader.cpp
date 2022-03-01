#include <Common/RedactHelpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Encryption/FileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/Page/V3/LogFile/LogFilename.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WAL/serialize.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

namespace DB::PS::V3
{
LogFilenameSet WALStoreReader::listAllFiles(
    const PSDiskDelegatorPtr & delegator,
    Poco::Logger * logger)
{
    // [<parent_path_0, [file0, file1, ...]>, <parent_path_1, [...]>, ...]
    std::vector<std::pair<String, Strings>> all_filenames;
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
    assert(all_filenames.size() == 1); // TODO: multi-path

    LogFilenameSet log_files;
    for (const auto & [parent_path, filenames] : all_filenames)
    {
        for (const auto & filename : filenames)
        {
            auto name = LogFilename::parseFrom(parent_path, filename, logger);
            switch (name.stage)
            {
            case LogFileStage::Normal:
            {
                log_files.insert(name);
                break;
            }
            case LogFileStage::Temporary:
                [[fallthrough]];
            case LogFileStage::Invalid:
            {
                // TODO: clean
                break;
            }
            }
        }
    }
    return log_files;
}

WALStoreReaderPtr WALStoreReader::create(FileProviderPtr & provider, LogFilenameSet files, const ReadLimiterPtr & read_limiter)
{
    auto reader = std::make_shared<WALStoreReader>(provider, std::move(files), read_limiter);
    reader->openNextFile();
    return reader;
}

WALStoreReaderPtr WALStoreReader::create(FileProviderPtr & provider, PSDiskDelegatorPtr & delegator, const ReadLimiterPtr & read_limiter)
{
    Poco::Logger * logger = &Poco::Logger::get("WALStore");
    LogFilenameSet log_files = listAllFiles(delegator, logger);
    return create(provider, std::move(log_files), read_limiter);
}

WALStoreReader::WALStoreReader(FileProviderPtr & provider_, LogFilenameSet && files_, const ReadLimiterPtr & read_limiter_)
    : provider(provider_)
    , files(std::move(files_))
    , next_reading_file(files.begin())
    , read_limiter(read_limiter_)
    , logger(&Poco::Logger::get("LogReader"))
{}

bool WALStoreReader::remained() const
{
    if (reader == nullptr)
        return false;

    if (!reader->isEOF())
        return true;
    if (next_reading_file != files.end())
        return true;
    return false;
}

std::tuple<bool, PageEntriesEdit> WALStoreReader::next()
{
    bool ok = false;
    String record;
    do
    {
        std::tie(ok, record) = reader->readRecord();
        if (ok)
        {
            return {true, ser::deserializeFrom(record)};
        }

        // Roll to read the next file
        if (bool next_file = openNextFile(); !next_file)
        {
            // No more file to be read.
            return {false, PageEntriesEdit{}};
        }
    } while (true);
}

bool WALStoreReader::openNextFile()
{
    if (next_reading_file == files.end())
    {
        return false;
    }

    {
        const auto & parent_path = next_reading_file->parent_path;
        const auto log_num = next_reading_file->log_num;
        const auto level_num = next_reading_file->level_num;
        const auto filename = fmt::format("log_{}_{}", log_num, level_num);
        const auto fullname = fmt::format("{}/{}", parent_path, filename);
        LOG_FMT_DEBUG(logger, "Open log file for reading [file={}]", fullname);

        auto read_buf = createReadBufferFromFileBaseByFileProvider(
            provider,
            fullname,
            EncryptionPath{parent_path, filename},
            /*estimated_size*/ Format::BLOCK_SIZE,
            /*aio_threshold*/ 0,
            /*read_limiter*/ read_limiter,
            /*buffer_size*/ Format::BLOCK_SIZE // Must be `Format::BLOCK_SIZE`
        );
        reader = std::make_unique<LogReader>(
            std::move(read_buf),
            &reporter,
            /*verify_checksum*/ true,
            log_num,
            WALRecoveryMode::TolerateCorruptedTailRecords,
            logger);
    }
    ++next_reading_file; // Note this will invalid `parent_path`
    return true;
}

} // namespace DB::PS::V3
