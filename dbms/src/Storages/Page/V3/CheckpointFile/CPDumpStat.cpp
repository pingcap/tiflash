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

#include <Common/TiFlashMetrics.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/Page/V3/CheckpointFile/CPDumpStat.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

namespace DB::PS::V3
{

void SetMetrics(const CPDataDumpStats & stats)
{
    for (size_t i = 0; i < static_cast<size_t>(DB::StorageType::_MAX_STORAGE_TYPE_); ++i)
    {
        auto type = static_cast<DB::StorageType>(i);
        switch (type)
        {
        case DB::StorageType::Unknown:
        {
            GET_METRIC(tiflash_storage_checkpoint_keys_by_types, type_unknown).Increment(stats.num_keys[i]);
            GET_METRIC(tiflash_storage_checkpoint_flow_by_types, type_unknown).Increment(stats.num_bytes[i]);
            GET_METRIC(tiflash_storage_page_data_by_types, type_unknown).Set(stats.num_existing_bytes[i]);
            break;
        }
        case DB::StorageType::RaftEngine:
        {
            GET_METRIC(tiflash_storage_checkpoint_keys_by_types, type_raftengine).Increment(stats.num_keys[i]);
            GET_METRIC(tiflash_storage_checkpoint_flow_by_types, type_raftengine).Increment(stats.num_bytes[i]);
            GET_METRIC(tiflash_storage_page_data_by_types, type_raftengine).Set(stats.num_existing_bytes[i]);
            break;
        }
        case DB::StorageType::KVEngine:
        {
            GET_METRIC(tiflash_storage_checkpoint_keys_by_types, type_kvengine).Increment(stats.num_keys[i]);
            GET_METRIC(tiflash_storage_checkpoint_flow_by_types, type_kvengine).Increment(stats.num_bytes[i]);
            GET_METRIC(tiflash_storage_page_data_by_types, type_kvengine).Set(stats.num_existing_bytes[i]);
            break;
        }
        case DB::StorageType::KVStore:
        {
            GET_METRIC(tiflash_storage_checkpoint_keys_by_types, type_kvstore).Increment(stats.num_keys[i]);
            GET_METRIC(tiflash_storage_checkpoint_flow_by_types, type_kvstore).Increment(stats.num_bytes[i]);
            GET_METRIC(tiflash_storage_page_data_by_types, type_kvstore).Set(stats.num_existing_bytes[i]);
            break;
        }
        case DB::StorageType::Data:
        {
            GET_METRIC(tiflash_storage_checkpoint_keys_by_types, type_data).Increment(stats.num_keys[i]);
            GET_METRIC(tiflash_storage_checkpoint_flow_by_types, type_data).Increment(stats.num_bytes[i]);
            GET_METRIC(tiflash_storage_page_data_by_types, type_data).Set(stats.num_existing_bytes[i]);
            break;
        }
        case DB::StorageType::Log:
        {
            GET_METRIC(tiflash_storage_checkpoint_keys_by_types, type_log).Increment(stats.num_keys[i]);
            GET_METRIC(tiflash_storage_checkpoint_flow_by_types, type_log).Increment(stats.num_bytes[i]);
            GET_METRIC(tiflash_storage_page_data_by_types, type_log).Set(stats.num_existing_bytes[i]);
            break;
        }
        case DB::StorageType::Meta:
        {
            GET_METRIC(tiflash_storage_checkpoint_keys_by_types, type_meta).Increment(stats.num_keys[i]);
            GET_METRIC(tiflash_storage_checkpoint_flow_by_types, type_meta).Increment(stats.num_bytes[i]);
            GET_METRIC(tiflash_storage_page_data_by_types, type_meta).Set(stats.num_existing_bytes[i]);
            break;
        }
        case DB::StorageType::LocalKV:
        {
            GET_METRIC(tiflash_storage_checkpoint_keys_by_types, type_localkv).Increment(stats.num_keys[i]);
            GET_METRIC(tiflash_storage_checkpoint_flow_by_types, type_localkv).Increment(stats.num_bytes[i]);
            GET_METRIC(tiflash_storage_page_data_by_types, type_localkv).Set(stats.num_existing_bytes[i]);
            break;
        }
        default:
            LOG_FATAL(Logger::get("SetMetrics"), "unsupported storage type {}", magic_enum::enum_name(type));
            __builtin_unreachable();
        }
    }
}

} // namespace DB::PS::V3
