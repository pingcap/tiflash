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

#include <Storages/Page/V3/BlobStore.h>

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

class CPWriteDataSourceBlobStore : public CPWriteDataSource
{
public:
    /**
     * The caller must ensure `blob_store` is valid when using with the CPFilesWriter.
     */
    CPWriteDataSourceBlobStore(BlobStore<universal::BlobStoreTrait> & blob_store_)
        : blob_store(blob_store_)
    {}

    Page read(const BlobStore<universal::BlobStoreTrait>::PageIdAndEntry & page_id_and_entry) override
    {
        return blob_store.read(page_id_and_entry);
    }

private:
    BlobStore<universal::BlobStoreTrait> & blob_store;
};

/**
 * Should be only useful in tests.
 */
class CPWriteDataSourceFixture : public CPWriteDataSource
{
public:
    CPWriteDataSourceFixture(const std::unordered_map<size_t /* offset */, std::string> & data_)
        : data(data_)
    {
    }

    static CPWriteDataSourcePtr create(const std::unordered_map<size_t /* offset */, std::string> & data_)
    {
        return std::make_shared<CPWriteDataSourceFixture>(data_);
    }

    Page read(const BlobStore<universal::BlobStoreTrait>::PageIdAndEntry & id_and_entry) override
    {
        auto it = data.find(id_and_entry.second.offset);
        if (it == data.end())
            return Page::invalidPage();

        auto & value = it->second;

        Page page(1);
        page.mem_holder = nullptr;
        page.data = ByteBuffer(value.data(), value.data() + value.size());
        return page;
    }

private:
    std::unordered_map<size_t /* offset */, std::string> data;
};

} // namespace DB::PS::V3
