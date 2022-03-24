// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Encryption/WriteReadableFile.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/V3/BlobFile.h>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_page_file_write_sync[];
} // namespace FailPoints

namespace PS::V3
{
BlobFile::BlobFile(String path_,
                   BlobFileId blob_id_,
                   FileProviderPtr file_provider_,
                   PSDiskDelegatorPtr delegator_)
    : blob_id(blob_id_)
    , file_provider{std::move(file_provider_)}
    , delegator(std::move(delegator_))
    , path(path_)
{
    // TODO: support encryption file
    wrfile = file_provider->newWriteReadableFile(
        getPath(),
        getEncryptionPath(),
        false,
        /*create_new_encryption_info_*/ false);

    Poco::File file_in_disk(getPath());
    file_size = file_in_disk.getSize();
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
        throw Exception(fmt::format("Write failed, FD is closed which [path={}], BlobFile should also be closed", path),
                        ErrorCodes::LOGICAL_ERROR);
    }

    fiu_do_on(FailPoints::exception_before_page_file_write_sync,
              { // Mock that exception happend before write and sync
                  throw Exception(fmt::format("Fail point {} is triggered.", FailPoints::exception_before_page_file_write_sync),
                                  ErrorCodes::FAIL_POINT_ERROR);
              });

#ifndef NDEBUG
    PageUtil::writeFile(wrfile, offset, buffer, size, write_limiter, true);
#else
    PageUtil::writeFile(wrfile, offset, buffer, size, write_limiter, false);
#endif
    PageUtil::syncFile(wrfile);


    BlobFileOffset cur_file_size = file_size.load();
    size_t expand_size = (offset + size) - cur_file_size;
    if (expand_size > 0)
    {
        delegator->addPageFileUsedSize(std::make_pair(blob_id, 0),
                                       expand_size,
                                       path,
                                       true);

        // CAS if `file_size` have not changed, ignore if changed.
        file_size.compare_exchange_strong(cur_file_size, offset + size);
    }
}

void BlobFile::truncate(size_t size)
{
    PageUtil::ftruncateFile(wrfile, size);
    assert(size <= file_size);

    delegator->freePageFileUsedSize(std::make_pair(blob_id, 0), file_size - size, path);
    file_size.store(size);
}

void BlobFile::remove()
{
    if (!wrfile->isClosed())
    {
        wrfile->close();
    }

    if (auto data_file = Poco::File(getPath()); data_file.exists())
    {
        file_provider->deleteRegularFile(getPath(), getEncryptionPath());
    }

    delegator->removePageFile(std::make_pair(blob_id, 0), file_size, false, false);
}

BlobFile::~BlobFile()
{
    wrfile->close();
}

} // namespace PS::V3
} // namespace DB
