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

#include <Storages/Page/V3/CheckpointFile/CPWriteDataSource.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>


namespace DB::PS::V3
{

Page CPWriteDataSourceBlobStore::read(const BlobStore<universal::BlobStoreTrait>::PageIdAndEntry & page_id_and_entry)
{
    if (page_id_and_entry.second.checkpoint_info.has_value()
        && page_id_and_entry.second.checkpoint_info.is_local_data_reclaimed)
    {
        return remote_reader->read(page_id_and_entry);
    }
    else
    {
        // StorageType::Log type page in local storage may be encrypted.
        Page page = blob_store.read(page_id_and_entry);
        const auto & ups_page_id = page_id_and_entry.first;
        auto page_id_u64 = UniversalPageIdFormat::getU64ID(ups_page_id);
        auto keyspace_id = UniversalPageIdFormat::getKeyspaceID(ups_page_id);
        if (unlikely(file_provider->isEncryptionEnabled(keyspace_id)) && !page.data.empty()
            && UniversalPageIdFormat::isType(ups_page_id, StorageType::Log))
        {
            file_provider->decryptPage(keyspace_id, page.mem_holder.get(), page.data.size(), page_id_u64);
        }
        return page;
    }
}

Page CPWriteDataSourceFixture::read(const BlobStore<universal::BlobStoreTrait>::PageIdAndEntry & id_and_entry)
{
    auto it = data.find(id_and_entry.second.offset);
    if (it == data.end())
        return Page::invalidPage();

    std::string_view value = it->second;

    Page page(1);
    page.mem_holder = nullptr;
    page.data = value;
    return page;
}

} // namespace DB::PS::V3
