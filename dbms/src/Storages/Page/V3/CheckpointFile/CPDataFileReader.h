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

namespace DB::PS::V3
{
using UniversalPageMap = std::map<UniversalPageId, Page>;
using UniversalPageIdAndEntry = std::pair<UniversalPageId, PS::V3::PageEntryV3>;
using UniversalPageIdAndEntries = std::vector<UniversalPageIdAndEntry>;

/**
 * Used to read checkpoint data from S3 according to the specified checkpoint info(including file_id, offset and size) in `PageEntry`.
 */
class CPDataFileReader : private Allocator<false>
{
public:
    explicit CPDataFileReader(std::shared_ptr<Aws::S3::S3Client> s3_client_, const String & bucket_)
        : s3_client(s3_client_)
        , bucket(bucket_)
    {}

    Page read(const UniversalPageIdAndEntry & page_id_and_entry);

    UniversalPageMap read(const UniversalPageIdAndEntries & page_id_and_entries);

    using FieldReadInfos = PS::V3::universal::BlobStoreType::FieldReadInfos;
    // return two page_maps, the first contains the whole page for given page id which is used to update local cache,
    // the second just contains read fields data.
    std::pair<UniversalPageMap, UniversalPageMap> read(const FieldReadInfos & to_read);

private:
    std::shared_ptr<Aws::S3::S3Client> s3_client;
    String bucket;
};

using CPDataFileReaderPtr = std::unique_ptr<CPDataFileReader>;
} // namespace DB::PS::V3
