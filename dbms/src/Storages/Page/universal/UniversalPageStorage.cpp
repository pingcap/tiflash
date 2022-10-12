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

#include <Storages/Page/UniversalPageStorage.h>
#include <Storages/Page/V3/Blob/BlobConfig.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageDirectoryFactory.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/WAL/WALConfig.h>

namespace DB
{

UniversalPageStoragePtr UniversalPageStorage::create(
    String name,
    PSDiskDelegatorPtr delegator,
    const UniversalPageStorage::Config & config,
    const FileProviderPtr & file_provider)
{
    UniversalPageStoragePtr storage = std::make_shared<UniversalPageStorage>(name, delegator, config, file_provider);
    PS::V3::BlobConfig blob_config; // FIXME: parse from config
    storage->blob_store = std::make_shared<PS::V3::BlobStore<PS::V3::universal::BlobStoreTrait>>(
        name,
        file_provider,
        delegator,
        blob_config);
    return storage;
}

void UniversalPageStorage::restore()
{
    blob_store->registerPaths();

    PS::V3::universal::PageDirectoryFactory factory;
    PS::V3::WALConfig wal_config; // FIXME: parse from config
    page_directory = factory.setBlobStore(*blob_store).create(storage_name, file_provider, delegator, wal_config);
}

void UniversalPageStorage::write(UniversalWriteBatch && write_batch, const WriteLimiterPtr & write_limiter) const
{
    if (unlikely(write_batch.empty()))
        return;

    auto edit = blob_store->write(write_batch, write_limiter);
    page_directory->apply(std::move(edit), write_limiter);
}
} // namespace DB
