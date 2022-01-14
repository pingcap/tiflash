#include <Encryption/WriteReadableFile.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/V3/BlobFile.h>

namespace DB::PS::V3
{
BlobFile::BlobFile(String path_,
                   FileProviderPtr file_provider_,
                   bool truncate_if_exists)
    : file_provider{file_provider_}
    , path(path_)
{
    wrfile = file_provider->newWriteReadableFile(
        getPath(),
        getEncryptionPath(),
        truncate_if_exists,
        /*create_new_encryption_info_*/ truncate_if_exists);
}

void BlobFile::read(char * buffer, size_t offset, size_t size, const ReadLimiterPtr & read_limiter)
{
    if (unlikely(wrfile->isClosed()))
    {
        throw Exception("Write failed, FD is closed which [path=" + path + "], BlobFile should also be closed",
                        ErrorCodes::LOGICAL_ERROR);
    }

    PageUtil::readFile(wrfile, offset, buffer, size, read_limiter);
}

void BlobFile::write(char * buffer, size_t offset, size_t size, const WriteLimiterPtr & write_limiter)
{
    /**
     * Precautions:
     *  - In the BlobFile `write` method, we won't increase the `PSMWritePages`.
     *  - Also won't increase `PSMReadPages` in the `read` method. 
     *  - It already do in `BlobStore`.
     *  - Also `PSMWriteBytes` and `PSMReadBytes` will be increased in `PageUtils`.
     */

    if (unlikely(wrfile->isClosed()))
    {
        throw Exception("Write failed, FD is closed which [path=" + path + "], BlobFile should also be closed",
                        ErrorCodes::LOGICAL_ERROR);
    }
#ifndef NDEBUG
    PageUtil::writeFile(wrfile, offset, buffer, size, write_limiter, true);
#else
    PageUtil::writeFile(wrfile, offset, buffer, size, write_limiter, false);
#endif
    PageUtil::syncFile(wrfile);
}

void BlobFile::truncate(size_t size)
{
    PageUtil::ftruncateFile(wrfile, size);
}

BlobFile::~BlobFile()
{
    wrfile->close();
}


} // namespace DB::PS::V3
