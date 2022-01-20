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
WALStorePtr WALStore::create(
    FileProviderPtr & /*provider*/,
    PSDiskDelegatorPtr & /*delegator*/,
    const WriteLimiterPtr & /*write_limiter*/)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
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
