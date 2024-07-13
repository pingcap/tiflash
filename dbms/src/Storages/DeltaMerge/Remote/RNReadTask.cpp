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

#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/Remote/RNDataProvider.h>
#include <Storages/DeltaMerge/Remote/RNLocalPageCache.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB::DM::Remote
{

RNReadSegmentTaskPtr RNReadSegmentTask::buildFromEstablishResp(
    const LoggerPtr & table_log,
    const Context & db_context,
    const ScanContextPtr & scan_context,
    const RemotePb::RemoteSegment & proto,
    const DisaggTaskId & snapshot_id,
    StoreID store_id,
    const String & store_address,
    KeyspaceID keyspace_id,
    TableID physical_table_id,
    ColumnID pk_col_id)
{
    RowKeyRange segment_range;
    {
        ReadBufferFromString rb(proto.key_range());
        segment_range = RowKeyRange::deserialize(rb);
    }
    RowKeyRanges read_ranges(proto.read_key_ranges_size());
    for (int i = 0; i < proto.read_key_ranges_size(); ++i)
    {
        ReadBufferFromString rb(proto.read_key_ranges(i));
        read_ranges[i] = RowKeyRange::deserialize(rb);
    }

    auto tracing_id = fmt::format(
        "{} segment_id={} epoch={} delta_epoch={}",
        table_log->identifier(),
        proto.segment_id(),
        proto.segment_epoch(),
        proto.delta_index_epoch());
    auto dm_context = std::make_shared<DMContext>(
        db_context,
        /* path_pool */ nullptr,
        /* storage_pool */ nullptr,
        /* min_version */ 0,
        keyspace_id,
        physical_table_id,
        pk_col_id,
        /* is_common_handle */ segment_range.is_common_handle,
        /* rowkey_column_size */ segment_range.rowkey_column_size,
        db_context.getSettingsRef(),
        scan_context,
        tracing_id);

    auto segment = std::make_shared<Segment>(
        Logger::get(),
        /*epoch*/ 0,
        segment_range,
        proto.segment_id(),
        /*next_segment_id*/ 0,
        nullptr,
        nullptr);

    auto segment_snap
        = Serializer::deserializeSegmentSnapshotFrom(*dm_context, store_id, keyspace_id, physical_table_id, proto);

    // Note: At this moment, we still cannot read from `task->segment_snap`,
    // because they are constructed using ColumnFileDataProviderNop.

    std::vector<UInt64> delta_tinycf_ids;
    std::vector<size_t> delta_tinycf_sizes;
    {
        // The number of ColumnFileTiny of MemTableSet is unknown, but there is a very high probability that it is zero.
        // So ignoring the number of ColumnFileTiny of MemTableSet is better than always adding all the number of ColumnFile of MemTableSet when reserving.
        auto persisted_cfs = segment_snap->delta->getPersistedFileSetSnapshot();
        delta_tinycf_ids.reserve(persisted_cfs->getColumnFileCount());
        delta_tinycf_sizes.reserve(persisted_cfs->getColumnFileCount());
    }
    auto extract_remote_pages = [&delta_tinycf_ids, &delta_tinycf_sizes](const ColumnFiles & cfs) {
        UInt64 count = 0;
        for (const auto & cf : cfs)
        {
            if (auto * tiny = cf->tryToTinyFile(); tiny)
            {
                delta_tinycf_ids.emplace_back(tiny->getDataPageId());
                delta_tinycf_sizes.emplace_back(tiny->getDataPageSize());
                ++count;
            }
        }
        return count;
    };
    auto memory_page_count = extract_remote_pages(segment_snap->delta->getMemTableSetSnapshot()->getColumnFiles());
    auto persisted_page_count
        = extract_remote_pages(segment_snap->delta->getPersistedFileSetSnapshot()->getColumnFiles());

    LOG_DEBUG(
        segment_snap->log,
        "Build RNReadSegmentTask: memtable_cfs={} persisted_cfs={} delta_index={} memory_page_count={} "
        "persisted_page_count={}",
        segment_snap->delta->getMemTableSetSnapshot()->getColumnFileCount(),
        segment_snap->delta->getPersistedFileSetSnapshot()->getColumnFileCount(),
        segment_snap->delta->getSharedDeltaIndex()->toString(),
        memory_page_count,
        persisted_page_count);

    return std::shared_ptr<RNReadSegmentTask>(new RNReadSegmentTask(RNReadSegmentMeta{
        .keyspace_id = keyspace_id,
        .physical_table_id = physical_table_id,
        .segment_id = proto.segment_id(),
        .store_id = store_id,

        .delta_tinycf_page_ids = delta_tinycf_ids,
        .delta_tinycf_page_sizes = delta_tinycf_sizes,
        .segment = segment,
        .segment_snap = segment_snap,
        .store_address = store_address,

        .read_ranges = read_ranges,
        .snapshot_id = snapshot_id,
        .dm_context = dm_context,
    }));
}

void RNReadSegmentTask::initColumnFileDataProvider(const RNLocalPageCacheGuardPtr & pages_guard)
{
    auto page_cache = meta.dm_context->db_context.getSharedContextDisagg()->rn_page_cache;
    auto data_provider = std::make_shared<ColumnFileDataProviderRNLocalPageCache>(
        page_cache,
        pages_guard,
        meta.store_id,
        KeyspaceTableID{meta.keyspace_id, meta.physical_table_id});

    auto & persisted_cf_set_data_provider = meta.segment_snap->delta->getPersistedFileSetSnapshot()->data_provider;
    RUNTIME_CHECK(std::dynamic_pointer_cast<ColumnFileDataProviderNop>(persisted_cf_set_data_provider));
    persisted_cf_set_data_provider = data_provider;

    auto & memory_cf_set_data_provider = meta.segment_snap->delta->getMemTableSetSnapshot()->data_provider;
    RUNTIME_CHECK(std::dynamic_pointer_cast<ColumnFileDataProviderNop>(memory_cf_set_data_provider));
    memory_cf_set_data_provider = data_provider;
}

void RNReadSegmentTask::initInputStream(
    const ColumnDefines & columns_to_read,
    UInt64 read_tso,
    const PushDownFilterPtr & push_down_filter,
    ReadMode read_mode)
{
    RUNTIME_CHECK(input_stream == nullptr);
    input_stream = meta.segment->getInputStream(
        read_mode,
        *meta.dm_context,
        columns_to_read,
        meta.segment_snap,
        meta.read_ranges,
        push_down_filter,
        read_tso,
        DEFAULT_BLOCK_SIZE);
}

} // namespace DB::DM::Remote
