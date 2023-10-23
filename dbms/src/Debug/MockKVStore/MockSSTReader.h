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
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/SSTReader.h>

#include <map>
#include <mutex>

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
        Data(Data &&) = default;
        Data & operator=(Data &&) = default;
        Data() = default;
        static Data copyFrom(const Data & other) { return other; }

    private:
        Data(const Data &) = default;
    };

    explicit MockSSTReader(const Data & data_, SSTFormatKind kind_)
        : iter(data_.begin())
        , begin(data_.begin())
        , end(data_.end())
        , remained(iter != end)
        , kind(kind_)
        , len(data_.size())
    {
        total_bytes = 0;
        for (auto it = data_.begin(); it != data_.end(); it++)
        {
            total_bytes += it->second.size();
        }
    }

    static SSTReaderPtr ffi_get_cf_file_reader(const Data & data_, SSTFormatKind kind_)
    {
        return SSTReaderPtr{new MockSSTReader(data_, kind_), kind_};
    }

    bool ffi_remained() const { return iter != end; }

    BaseBuffView ffi_key() const { return {iter->first.data(), iter->first.length()}; }

    BaseBuffView ffi_val() const { return {iter->second.data(), iter->second.length()}; }

    void ffi_next() { ++iter; }

    SSTFormatKind ffi_kind() { return kind; }

    void ffi_seek(SSTReaderPtr, ColumnFamilyType, EngineIteratorSeekType et, BaseBuffView bf)
    {
        if (et == EngineIteratorSeekType::First)
        {
            iter = begin;
            remained = iter != end;
        }
        else if (et == EngineIteratorSeekType::Last)
        {
            iter = end;
            remained = false;
        }
        else
        {
            // Seek the first key >= given key
            iter = begin;
            remained = iter != end;
            auto thres = buffToStrView(bf);
            while (ffi_remained())
            {
                auto && current_key = iter->first;
                if (current_key >= thres)
                {
                    return;
                }
                ffi_next();
            }
        }
    }

    size_t ffi_approx_size() { return total_bytes; }

    size_t length() const { return len; }

    Data::const_iterator getBegin() const { return begin; }

    Data::const_iterator getEnd() const { return end; }

    static std::map<Key, MockSSTReader::Data> & getMockSSTData() { return MockSSTData; }

public:
    static std::mutex mut;

private:
    Data::const_iterator iter;
    Data::const_iterator begin;
    Data::const_iterator end;
    bool remained;
    SSTFormatKind kind;
    size_t total_bytes;
    size_t len;

    // (region_id, cf) -> Data
    static std::map<Key, MockSSTReader::Data> MockSSTData;
};

inline std::map<MockSSTReader::Key, MockSSTReader::Data> MockSSTReader::MockSSTData;
inline std::mutex MockSSTReader::mut;

SSTReaderInterfaces make_mock_sst_reader_interface();
} // namespace DB
