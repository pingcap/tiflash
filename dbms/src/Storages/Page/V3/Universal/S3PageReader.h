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

#pragma once

#include <Common/Allocator.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/Universal/UniversalPageId.h>
#include <aws/s3/model/GetObjectResult.h>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{
class ReadBufferFromRandomAccessFile;
using ReadBufferFromRandomAccessFilePtr = std::shared_ptr<ReadBufferFromRandomAccessFile>;
} // namespace DB

namespace DB::PS::V3
{
using UniversalPageMap = std::map<UniversalPageId, Page>;
using UniversalPageIdAndEntry = std::pair<UniversalPageId, PS::V3::PageEntryV3>;
using UniversalPageIdAndEntries = std::vector<UniversalPageIdAndEntry>;

/**
 * Used to read checkpoint data from S3 according to the specified checkpoint info(including file_id, offset and size) in `PageEntry`.
 */
class S3PageReader : private Allocator<false>
{
public:
    S3PageReader() = default;

    Page read(const UniversalPageIdAndEntry & page_id_and_entry);
    // Give an S3RandomAccessFile, try read from current cursor of this file if possible,
    // otherwise create a new one and seek from the beginning.
    // Returns the S3RandomAccessFile we eventually read from, for later use.
    std::tuple<Page, ReadBufferFromRandomAccessFilePtr> readFromS3File(
        const UniversalPageIdAndEntry & page_id_and_entry,
        ReadBufferFromRandomAccessFilePtr file_buf,
        size_t prefetch_size);

    UniversalPageMap read(const UniversalPageIdAndEntries & page_id_and_entries);

    using FieldReadInfos = PS::V3::universal::BlobStoreType::FieldReadInfos;
    // return two page_maps, the first contains the whole page for given page id which is used to update local cache,
    // the second just contains read fields data.
    std::pair<UniversalPageMap, UniversalPageMap> read(FieldReadInfos & to_read);
};

using S3PageReaderPtr = std::unique_ptr<S3PageReader>;
} // namespace DB::PS::V3
