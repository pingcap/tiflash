// Copyright 2023 PingCAP, Inc.
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

#include <IO/BaseFile/WriteReadableFile.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/V3/Blob/BlobFile.h>
#include <Storages/PathPool.h>

namespace DB
{
namespace FailPoints
{
extern const char exception_before_page_file_write_sync[];
} // namespace FailPoints

namespace PS::V3
{
BlobFile::BlobFile(
    String parent_path_,
    BlobFileId blob_id_,
    FileProviderPtr file_provider_,
    PSDiskDelegatorPtr delegator_)
    : blob_id(blob_id_)
    , file_provider{std::move(file_provider_)}
    , delegator(std::move(delegator_))
    , parent_path(std::move(parent_path_))
{
    Poco::File file_in_disk(getPath());
    wrfile = file_provider->newWriteReadableFile(
        getPath(),
        getEncryptionPath(),
        false,
        /*create_new_encryption_info_*/ !file_in_disk.exists(),
        /*skip_encryption*/ file_provider->isKeyspaceEncryptionEnabled()
        // When keyspace encryption is enabled, we encrypt the page data instead of the whole BlobFile
    );

    file_size = file_in_disk.getSize();
    {
        std::lock_guard lock(file_size_lock);

        // If file_size is 0, we still need insert it.
        PageFileIdAndLevel id_lvl{blob_id, 0};
        if (!delegator->fileExist(id_lvl))
        {
            delegator->addPageFileUsedSize(
                id_lvl,
                file_size,
                parent_path,
                /*need_insert_location*/ true);
        }
    }
}

void BlobFile::read(char * buffer, size_t offset, size_t size, const ReadLimiterPtr & read_limiter, bool background)
{
    if (unlikely(wrfile->isClosed()))
    {
        throw Exception(
            "Write failed, FD is closed which [path=" + parent_path + "], BlobFile should also be closed",
            ErrorCodes::LOGICAL_ERROR);
    }

    PageUtil::readFile(wrfile, offset, buffer, size, read_limiter, background);
}

void BlobFile::write(char * buffer, size_t offset, size_t size, const WriteLimiterPtr & write_limiter, bool background)
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
        throw Exception(
            fmt::format("Write failed, FD is closed which [path={}], BlobFile should also be closed", parent_path),
            ErrorCodes::LOGICAL_ERROR);
    }

    fiu_do_on(
        FailPoints::exception_before_page_file_write_sync,
        { // Mock that exception happend before write and sync
            throw Exception(
                fmt::format("Fail point {} is triggered.", FailPoints::exception_before_page_file_write_sync),
                ErrorCodes::FAIL_POINT_ERROR);
        });

#ifndef NDEBUG
    PageUtil::writeFile(
        wrfile,
        offset,
        buffer,
        size,
        write_limiter,
        background,
        /*truncate_if_failed=*/false,
        /*enable_failpoint=*/true);
#else
    PageUtil::writeFile(
        wrfile,
        offset,
        buffer,
        size,
        write_limiter,
        background,
        /*truncate_if_failed=*/false,
        /*enable_failpoint=*/false);
#endif
    PageUtil::syncFile(wrfile);

    UInt64 expand_size = 0;
    {
        std::lock_guard lock(file_size_lock);
        if ((offset + size) > file_size)
        {
            expand_size = offset + size - file_size;
            file_size = offset + size;
        }
    }

    if (expand_size != 0)
    {
        delegator->addPageFileUsedSize(std::make_pair(blob_id, 0), expand_size, parent_path, false);
    }
}

void BlobFile::truncate(size_t size)
{
    PageUtil::ftruncateFile(wrfile, size);
    Int64 shrink_size = 0;
    {
        std::lock_guard lock(file_size_lock);
        assert(size <= file_size);
        shrink_size = file_size - size;
        file_size = size;
    }
    delegator->freePageFileUsedSize(std::make_pair(blob_id, 0), shrink_size, parent_path);
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
