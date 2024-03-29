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

#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/Universal/S3PageReader.h>

namespace DB::PS::V3
{

/**
 * The source of data when writing checkpoint data files.
 */
class CPWriteDataSource : private boost::noncopyable
{
public:
    virtual ~CPWriteDataSource() = default;

    virtual Page read(const BlobStore<universal::BlobStoreTrait>::PageIdAndEntry &) = 0;
};

using CPWriteDataSourcePtr = std::shared_ptr<CPWriteDataSource>;

/**
 * The source of the data comes from a specified BlobStore when writing checkpoint data files.
 *
 * You need to ensure the BlobStore reference is alive during the lifetime of this data source.
 */
class CPWriteDataSourceBlobStore : public CPWriteDataSource
{
public:
    /**
     * The caller must ensure `blob_store` is valid when using with the CPFilesWriter.
     */
    explicit CPWriteDataSourceBlobStore(
        BlobStore<universal::BlobStoreTrait> & blob_store_,
        const FileProviderPtr & file_provider_)
        : blob_store(blob_store_)
        , remote_reader(std::make_unique<S3PageReader>())
        , file_provider(file_provider_)
    {}

    static CPWriteDataSourcePtr create(
        BlobStore<universal::BlobStoreTrait> & blob_store_,
        const FileProviderPtr & file_provider_)
    {
        return std::make_shared<CPWriteDataSourceBlobStore>(blob_store_, file_provider_);
    }

    Page read(const BlobStore<universal::BlobStoreTrait>::PageIdAndEntry & page_id_and_entry) override;

private:
    BlobStore<universal::BlobStoreTrait> & blob_store;
    S3PageReaderPtr remote_reader;
    FileProviderPtr file_provider;
};

/**
 * Should be only useful in tests. You need to specify the data that can be read out when passing different
 * BlobStore offset fields.
 */
class CPWriteDataSourceFixture : public CPWriteDataSource
{
public:
    explicit CPWriteDataSourceFixture(const std::unordered_map<size_t /* offset */, std::string> & data_)
        : data(data_)
    {}

    static CPWriteDataSourcePtr create(const std::unordered_map<size_t /* offset */, std::string> & data_)
    {
        return std::make_shared<CPWriteDataSourceFixture>(data_);
    }

    Page read(const BlobStore<universal::BlobStoreTrait>::PageIdAndEntry & id_and_entry) override;

private:
    std::unordered_map<size_t /* offset */, std::string> data;
};

} // namespace DB::PS::V3
