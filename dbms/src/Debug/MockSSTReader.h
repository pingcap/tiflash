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

#include <Common/nocopyable.h>
#include <Storages/Transaction/ProxyFFI.h>

#include <map>


namespace DB
{

class KVStore;
using KVStorePtr = std::shared_ptr<KVStore>;

class RegionTable;
class Region;
using RegionPtr = std::shared_ptr<Region>;

/// Some helper structure / functions for IngestSST

struct MockSSTReader
{
    using Key = std::pair<std::string, ColumnFamilyType>;
    struct Data : std::vector<std::pair<std::string, std::string>>
    {
        DISALLOW_COPY(Data);
        Data(Data &&) = default;
        Data & operator=(Data &&) = default;
        Data() = default;
    };

    MockSSTReader(const Data & data_)
        : iter(data_.begin())
        , end(data_.end())
        , remained(iter != end)
    {}

    static SSTReaderPtr ffi_get_cf_file_reader(const Data & data_) { return SSTReaderPtr{new MockSSTReader(data_)}; }

    bool ffi_remained() const { return iter != end; }

    BaseBuffView ffi_key() const { return {iter->first.data(), iter->first.length()}; }

    BaseBuffView ffi_val() const { return {iter->second.data(), iter->second.length()}; }

    void ffi_next() { ++iter; }

    static std::map<Key, MockSSTReader::Data> & getMockSSTData() { return MockSSTData; }

private:
    Data::const_iterator iter;
    Data::const_iterator end;
    bool remained;

    static std::map<Key, MockSSTReader::Data> MockSSTData;
};


class RegionMockTest
{
public:
    RegionMockTest(KVStorePtr kvstore_, RegionPtr region_);
    ~RegionMockTest();

private:
    TiFlashRaftProxyHelper mock_proxy_helper{};
    const TiFlashRaftProxyHelper * ori_proxy_helper{};
    KVStorePtr kvstore;
    RegionPtr region;
};
} // namespace DB
