// Copyright 2025 PingCAP, Inc.
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

#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/KVStore/MultiRaft/RegionRangeKeys.h>
#include <Storages/KVStore/Region.h>

namespace DB::DM
{

RowKeyRange RowKeyRange::fromRegionRange(
    const std::shared_ptr<const RegionRangeKeys> & region_range,
    const TableID table_id,
    bool is_common_handle,
    size_t rowkey_column_size,
    const String & tracing_msg)
{
    return fromRegionRange(
        region_range->rawKeys(),
        region_range->getMappedTableID(),
        table_id,
        is_common_handle,
        rowkey_column_size,
        tracing_msg);
}

RowKeyRange RowKeyRange::fromRegionRange(
    const std::pair<DecodedTiKVKeyPtr, DecodedTiKVKeyPtr> & raw_keys,
    const TableID table_id_in_raw_key,
    const TableID table_id,
    bool is_common_handle,
    size_t rowkey_column_size,
    const String & tracing_msg)
{
    if (unlikely(table_id_in_raw_key != table_id))
    {
        /// if table id is not the same, just return none range
        /// maybe should throw exception since it should not happen
        return newNone(is_common_handle, rowkey_column_size);
    }

    const auto & start_key = *raw_keys.first;
    const auto & end_key = *raw_keys.second;
    const auto keyspace_id = start_key.getKeyspaceID();
    const auto & table_range_min_max = getTableMinMaxData(keyspace_id, table_id, is_common_handle);
    RowKeyValue start_value, end_value;
    std::string warning_message;
    std::string_view extra_suffix_holder;
    if (start_key <= *table_range_min_max.min)
    {
        if (is_common_handle)
            start_value = RowKeyValue::COMMON_HANDLE_MIN_KEY;
        else
            start_value = RowKeyValue::INT_HANDLE_MIN_KEY;
    }
    else
    {
        // keep the ptr to handle because `extra_suffix_holder` rely on this instance lifetime
        auto start_handle = std::make_shared<std::string>(RecordKVFormat::getRawTiDBPKView(start_key));
        std::tie(start_value, extra_suffix_holder) = RowKeyValue::fromHandleWithSuffix(is_common_handle, start_handle);
        if (unlikely(!extra_suffix_holder.empty()))
        {
            warning_message = fmt::format(
                "{} start_key={} start_key_extra_suffix={}",
                warning_message,
                Redact::keyToDebugString(start_key.data(), start_key.size()),
                Redact::keyToHexString(extra_suffix_holder.data(), extra_suffix_holder.size()));
        }
    }
    if (end_key >= *table_range_min_max.max)
    {
        if (is_common_handle)
            end_value = RowKeyValue::COMMON_HANDLE_MAX_KEY;
        else
            end_value = RowKeyValue::INT_HANDLE_MAX_KEY;
    }
    else
    {
        // keep the ptr to handle because `extra_suffix_holder` rely on this instance lifetime
        auto end_handle = std::make_shared<std::string>(RecordKVFormat::getRawTiDBPKView(end_key));
        std::tie(end_value, extra_suffix_holder) = RowKeyValue::fromHandleWithSuffix(is_common_handle, end_handle);
        if (unlikely(!extra_suffix_holder.empty()))
        {
            warning_message = fmt::format(
                "{} end_key={} end_key_extra_suffix={}",
                warning_message,
                Redact::keyToDebugString(end_key.data(), end_key.size()),
                Redact::keyToHexString(extra_suffix_holder.data(), extra_suffix_holder.size()));
        }
    }
    if (unlikely(!warning_message.empty()))
    {
        LOG_WARNING(
            Logger::get(),
            "Meet rowkey which has extra suffix, keyspace={} table_id={} {} {}",
            keyspace_id,
            table_id,
            warning_message,
            tracing_msg);
    }
    return RowKeyRange(start_value, end_value, is_common_handle, rowkey_column_size);
}
} // namespace DB::DM
