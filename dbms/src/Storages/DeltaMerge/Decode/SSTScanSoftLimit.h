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

#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/FFI/SSTReader.h>
#include <Storages/KVStore/MultiRaft/RegionState.h>
#include <Storages/KVStore/TiKVHelpers/TiKVKeyValue.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB::DM
{

struct SSTScanSoftLimit
{
    constexpr static size_t HEAD_OR_ONLY_SPLIT = SIZE_MAX;
    size_t split_id;
    TiKVKey raw_start;
    TiKVKey raw_end;
    DecodedTiKVKey decoded_start;
    DecodedTiKVKey decoded_end;
    std::optional<RawTiDBPK> start_limit;
    std::optional<RawTiDBPK> end_limit;

    SSTScanSoftLimit(size_t split_id_, TiKVKey && raw_start_, TiKVKey && raw_end_)
        : split_id(split_id_)
        , raw_start(std::move(raw_start_))
        , raw_end(std::move(raw_end_))
    {
        if (!raw_start.empty())
        {
            decoded_start = RecordKVFormat::decodeTiKVKey(raw_start);
        }
        if (!raw_end.empty())
        {
            decoded_end = RecordKVFormat::decodeTiKVKey(raw_end);
        }
        if (!decoded_start.empty())
        {
            start_limit = RecordKVFormat::getRawTiDBPK(decoded_start);
        }
        if (!decoded_end.empty())
        {
            end_limit = RecordKVFormat::getRawTiDBPK(decoded_end);
        }
    }

    SSTScanSoftLimit clone() const { return SSTScanSoftLimit(split_id, raw_start.toString(), raw_end.toString()); }

    const std::optional<RawTiDBPK> & getStartLimit() const { return start_limit; }

    const std::optional<RawTiDBPK> & getEndLimit() const { return end_limit; }

    std::string toDebugString() const
    {
        return fmt::format("{}:{}", raw_start.toDebugString(), raw_end.toDebugString());
    }
};

} // namespace DB::DM
