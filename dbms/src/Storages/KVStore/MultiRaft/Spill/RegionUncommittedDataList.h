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
    RegionUncommittedData(RawTiDBPK && pk_, UInt8 && write_type_, std::shared_ptr<const TiKVValue> && value_)
        : pk(std::move(pk_))
        , write_type(write_type_)
        , value(std::move(value_))
    {}
    RegionUncommittedData(
        const RawTiDBPK & pk_,
        const UInt8 & write_type_,
        const std::shared_ptr<const TiKVValue> & value_)
        : pk(pk_)
        , write_type(write_type_)
        , value(value_)
    {}
    RegionUncommittedData(const RegionDataReadInfo &) = default;
    RegionUncommittedData(RegionDataReadInfo &&) = default;
    RegionUncommittedData & operator=(const RegionDataReadInfo &) = default;
    RegionUncommittedData & operator=(RegionDataReadInfo &&) = default;

public:
    RawTiDBPK pk;
    UInt8 write_type;
    std::shared_ptr<const TiKVValue> value;
};

struct RegionUncommittedDataList
{
    std::vector<RegionUncommittedData> data;
    Timestamp start_ts;
}

} // namespace DB