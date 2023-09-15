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
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB
{

/// TableRowIDMinMax is used to store the min/max key for specific table.
struct TableRowIDMinMax
{
    TableRowIDMinMax(const TableID table_id)
        : handle_min(RecordKVFormat::genRawKey(table_id, std::numeric_limits<HandleID>::min()))
        , handle_max(RecordKVFormat::genRawKey(table_id, std::numeric_limits<HandleID>::max()))
    {}

    /// Make this struct can't be copied or moved.
    DISALLOW_COPY_AND_MOVE(TableRowIDMinMax);

    const DecodedTiKVKey handle_min;
    const DecodedTiKVKey handle_max;

    /// It's a long lived object, so just return const ref directly.
    static const TableRowIDMinMax & getMinMax(const TableID table_id);

private:
    static std::unordered_map<TableID, TableRowIDMinMax> data;
    static std::mutex mutex;
};

} // namespace DB
