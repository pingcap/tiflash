// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>

namespace DB
{
struct RegionUncommittedData
{
    RegionUncommittedData(RawTiDBPK && pk_, UInt8 write_type_, std::shared_ptr<const TiKVValue> && value_)
        : pk(std::move(pk_))
        , write_type(write_type_)
        , value(std::move(value_))
    {}
    RegionUncommittedData(
        const RawTiDBPK & pk_,
        const UInt8 write_type_,
        const std::shared_ptr<const TiKVValue> & value_)
        : pk(pk_)
        , write_type(write_type_)
        , value(value_)
    {}
    RegionUncommittedData(const RegionUncommittedData &) = default;
    RegionUncommittedData(RegionUncommittedData &&) = default;
    RegionUncommittedData & operator=(const RegionUncommittedData &) = default;
    RegionUncommittedData & operator=(RegionUncommittedData &&) = default;

    RawTiDBPK pk;
    UInt8 write_type;
    std::shared_ptr<const TiKVValue> value;
};

struct RegionUncommittedDataList
{
    using Inner = std::vector<RegionUncommittedData>;

    Inner::const_iterator cbegin() const { return data.cbegin(); }

    Inner::const_iterator cend() const { return data.cend(); }

    Inner::iterator begin() { return data.begin(); }

    Inner::const_iterator begin() const { return data.begin(); }

    Inner::iterator end() { return data.end(); }

    Inner::const_iterator end() const { return data.end(); }

    size_t size() const { return data.size(); }

    Inner & getInner() { return data; }
    const Inner & getInner() const { return data; }

private:
    Inner data;
    // Timestamp start_ts;
};

} // namespace DB