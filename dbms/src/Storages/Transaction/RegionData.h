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

#include <Storages/Transaction/RegionCFDataBase.h>
#include <Storages/Transaction/RegionCFDataTrait.h>
#include <Storages/Transaction/RegionDataRead.h>

namespace DB
{
using RegionWriteCFData = RegionCFDataBase<RegionWriteCFDataTrait>;
using RegionDefaultCFData = RegionCFDataBase<RegionDefaultCFDataTrait>;
using RegionLockCFData = RegionCFDataBase<RegionLockCFDataTrait>;

using DecodedLockCFValuePtr = std::shared_ptr<const RecordKVFormat::DecodedLockCFValue>;

enum class ColumnFamilyType : uint8_t;

struct RegionLockReadQuery;
class Region;

class RegionData
{
public:
    using WriteCFIter = RegionWriteCFData::Map::iterator;
    using ConstWriteCFIter = RegionWriteCFData::Map::const_iterator;

    void insert(ColumnFamilyType cf, TiKVKey && key, TiKVValue && value);
    void remove(ColumnFamilyType cf, const TiKVKey & key);

    WriteCFIter removeDataByWriteIt(const WriteCFIter & write_it);

    RegionDataReadInfo readDataByWriteIt(const ConstWriteCFIter & write_it, bool need_value = true) const;

    DecodedLockCFValuePtr getLockInfo(const RegionLockReadQuery & query) const;

    void splitInto(const RegionRange & range, RegionData & new_region_data);
    void mergeFrom(const RegionData & ori_region_data);

    size_t dataSize() const;

    void assignRegionData(RegionData && new_region_data);

    size_t serialize(WriteBuffer & buf) const;

    static void deserialize(ReadBuffer & buf, RegionData & region_data);

    friend bool operator==(const RegionData & r1, const RegionData & r2) { return r1.isEqual(r2); }

    bool isEqual(const RegionData & r2) const;

    RegionWriteCFData & writeCF();
    RegionDefaultCFData & defaultCF();

    const RegionWriteCFData & writeCF() const;
    const RegionDefaultCFData & defaultCF() const;
    const RegionLockCFData & lockCF() const;

    RegionData() {}

    RegionData(RegionData && data);
    RegionData & operator=(RegionData &&);

private:
    friend class Region;

private:
    RegionWriteCFData write_cf;
    RegionDefaultCFData default_cf;
    RegionLockCFData lock_cf;

    // Size of data cf & write cf, without lock cf.
    std::atomic<size_t> cf_data_size = 0;
};

} // namespace DB
