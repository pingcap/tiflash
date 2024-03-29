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

#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>

#include <list>

namespace DB
{

struct RegionDataReadInfo
{
    RegionDataReadInfo(
        RawTiDBPK && pk_,
        UInt8 write_type_,
        Timestamp && commit_ts_,
        std::shared_ptr<const TiKVValue> && value_)
        : pk(std::move(pk_))
        , write_type(write_type_)
        , commit_ts(std::move(commit_ts_))
        , value(std::move(value_))
    {}
    RegionDataReadInfo(
        const RawTiDBPK & pk_,
        UInt8 write_type_,
        const Timestamp & commit_ts_,
        const std::shared_ptr<const TiKVValue> & value_)
        : pk(pk_)
        , write_type(write_type_)
        , commit_ts(commit_ts_)
        , value(value_)
    {}
    RegionDataReadInfo(const RegionDataReadInfo &) = default;
    RegionDataReadInfo(RegionDataReadInfo &&) = default;
    RegionDataReadInfo & operator=(const RegionDataReadInfo &) = default;
    RegionDataReadInfo & operator=(RegionDataReadInfo &&) = default;

public:
    RawTiDBPK pk;
    UInt8 write_type;
    Timestamp commit_ts;
    std::shared_ptr<const TiKVValue> value;
};

using RegionDataReadInfoList = std::vector<RegionDataReadInfo>;

struct PrehandleResult
{
    std::vector<DM::ExternalDTFileInfo> ingest_ids;
    struct Stats
    {
        size_t parallels = 0;
        // These are bytes we actually read from sst reader.
        // It doesn't includes rocksdb's space amplification.
        size_t raft_snapshot_bytes = 0;
        size_t approx_raft_snapshot_size = 0;
        size_t dt_disk_bytes = 0;
        size_t dt_total_bytes = 0;
        size_t total_keys = 0;
        size_t write_cf_keys = 0;
        size_t lock_cf_keys = 0;
        size_t default_cf_keys = 0;
        size_t max_split_write_cf_keys = 0;
        // Will be set in preHandleSnapshotToFiles
        size_t start_time = 0;

        void mergeFrom(const Stats & other)
        {
            parallels += other.parallels;
            raft_snapshot_bytes += other.raft_snapshot_bytes;
            approx_raft_snapshot_size += other.approx_raft_snapshot_size;
            dt_disk_bytes += other.dt_disk_bytes;
            dt_total_bytes += other.dt_total_bytes;
            total_keys += other.total_keys;
            write_cf_keys += other.write_cf_keys;
            lock_cf_keys += other.lock_cf_keys;
            default_cf_keys += other.default_cf_keys;
            max_split_write_cf_keys = std::max(max_split_write_cf_keys, other.max_split_write_cf_keys);
        }
    };
    Stats stats;
};
} // namespace DB
