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

#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/Page/PageStorage.h>
#include <tipb/executor.pb.h>

namespace DB
{
namespace DM
{

StoreStats DeltaMergeStore::getStoreStats()
{
    StoreStats stat;

    if (shutdown_called.load(std::memory_order_relaxed))
        return stat;

    Int64 total_placed_rows = 0;
    Int64 total_delta_cache_rows = 0;
    Float64 total_delta_cache_size = 0;
    Int64 total_delta_valid_cache_rows = 0;
    {
        std::shared_lock lock(read_write_mutex);
        stat.segment_count = segments.size();

        for (const auto & [handle, segment] : segments)
        {
            UNUSED(handle);
            const auto & delta = segment->getDelta();
            const auto & stable = segment->getStable();

            total_placed_rows += delta->getPlacedDeltaRows();

            if (delta->getColumnFileCount())
            {
                stat.total_rows += delta->getRows();
                stat.total_size += delta->getBytes();

                stat.total_delete_ranges += delta->getDeletes();

                stat.delta_count += 1;
                const auto num_delta_column_file = delta->getColumnFileCount();
                stat.total_pack_count_in_delta += num_delta_column_file;
                stat.max_pack_count_in_delta = std::max(stat.max_pack_count_in_delta, num_delta_column_file);

                stat.total_delta_rows += delta->getRows();
                stat.total_delta_size += delta->getBytes();

                stat.delta_index_size += delta->getDeltaIndexBytes();

                total_delta_cache_rows += delta->getTotalCacheRows();
                total_delta_cache_size += delta->getTotalCacheBytes();
                total_delta_valid_cache_rows += delta->getValidCacheRows();
            }

            if (stable->getDMFilesPacks())
            {
                stat.total_rows += stable->getRows();
                stat.total_size += stable->getBytes();

                stat.stable_count += 1;
                stat.total_pack_count_in_stable += stable->getDMFilesPacks();

                stat.total_stable_rows += stable->getRows();
                stat.total_stable_size += stable->getBytes();
                stat.total_stable_size_on_disk += stable->getDMFilesBytesOnDisk();
            }
        }
    } // access to `segments` end

    stat.delta_rate_rows = static_cast<Float64>(stat.total_delta_rows) / stat.total_rows;
    stat.delta_rate_segments = static_cast<Float64>(stat.delta_count) / stat.segment_count;

    stat.delta_placed_rate = static_cast<Float64>(total_placed_rows) / stat.total_delta_rows;
    stat.delta_cache_size = total_delta_cache_size;
    stat.delta_cache_rate = static_cast<Float64>(total_delta_valid_cache_rows) / stat.total_delta_rows;
    stat.delta_cache_wasted_rate
        = static_cast<Float64>(total_delta_cache_rows - total_delta_valid_cache_rows) / total_delta_valid_cache_rows;

    stat.avg_segment_rows = static_cast<Float64>(stat.total_rows) / stat.segment_count;
    stat.avg_segment_size = static_cast<Float64>(stat.total_size) / stat.segment_count;

    stat.avg_delta_rows = static_cast<Float64>(stat.total_delta_rows) / stat.delta_count;
    stat.avg_delta_size = static_cast<Float64>(stat.total_delta_size) / stat.delta_count;
    stat.avg_delta_delete_ranges = static_cast<Float64>(stat.total_delete_ranges) / stat.delta_count;

    stat.avg_stable_rows = static_cast<Float64>(stat.total_stable_rows) / stat.stable_count;
    stat.avg_stable_size = static_cast<Float64>(stat.total_stable_size) / stat.stable_count;

    stat.avg_pack_count_in_delta = static_cast<Float64>(stat.total_pack_count_in_delta) / stat.delta_count;
    stat.avg_pack_rows_in_delta = static_cast<Float64>(stat.total_delta_rows) / stat.total_pack_count_in_delta;
    stat.avg_pack_size_in_delta = static_cast<Float64>(stat.total_delta_size) / stat.total_pack_count_in_delta;

    stat.avg_pack_count_in_stable = static_cast<Float64>(stat.total_pack_count_in_stable) / stat.stable_count;
    stat.avg_pack_rows_in_stable = static_cast<Float64>(stat.total_stable_rows) / stat.total_pack_count_in_stable;
    stat.avg_pack_size_in_stable = static_cast<Float64>(stat.total_stable_size) / stat.total_pack_count_in_stable;

    // Only collect the snapshot stats for each table when PageStorage V2 is enabled.
    // Collecting snapshot stats on the global PageStorage V3 for each table will cause too many
    // waste on CPU and lock contention. Which cause slow queries.
    if (storage_pool->getPageStorageRunMode() == PageStorageRunMode::ONLY_V2)
    {
        {
            auto snaps_stat = storage_pool->dataReader()->getSnapshotsStat();
            stat.storage_stable_num_snapshots = snaps_stat.num_snapshots;
            stat.storage_stable_oldest_snapshot_lifetime = snaps_stat.longest_living_seconds;
            stat.storage_stable_oldest_snapshot_thread_id = snaps_stat.longest_living_from_thread_id;
            stat.storage_stable_oldest_snapshot_tracing_id = snaps_stat.longest_living_from_tracing_id;
        }
        {
            auto snaps_stat = storage_pool->logReader()->getSnapshotsStat();
            stat.storage_delta_num_snapshots = snaps_stat.num_snapshots;
            stat.storage_delta_oldest_snapshot_lifetime = snaps_stat.longest_living_seconds;
            stat.storage_delta_oldest_snapshot_thread_id = snaps_stat.longest_living_from_thread_id;
            stat.storage_delta_oldest_snapshot_tracing_id = snaps_stat.longest_living_from_tracing_id;
        }
        {
            auto snaps_stat = storage_pool->metaReader()->getSnapshotsStat();
            stat.storage_meta_num_snapshots = snaps_stat.num_snapshots;
            stat.storage_meta_oldest_snapshot_lifetime = snaps_stat.longest_living_seconds;
            stat.storage_meta_oldest_snapshot_thread_id = snaps_stat.longest_living_from_thread_id;
            stat.storage_meta_oldest_snapshot_tracing_id = snaps_stat.longest_living_from_tracing_id;
        }
    }

    stat.background_tasks_length = background_tasks.length();

    return stat;
}

SegmentsStats DeltaMergeStore::getSegmentsStats()
{
    std::shared_lock lock(read_write_mutex);

    SegmentsStats stats;
    for (const auto & [handle, segment] : segments)
    {
        UNUSED(handle);

        SegmentStats stat;
        const auto & delta = segment->getDelta();
        const auto & delta_memtable = delta->getMemTableSet();
        const auto & delta_persisted = delta->getPersistedFileSet();
        const auto & stable = segment->getStable();

        stat.segment_id = segment->segmentId();
        stat.range = segment->getRowKeyRange();
        stat.epoch = segment->segmentEpoch();
        stat.rows = segment->getEstimatedRows();
        stat.size = segment->getEstimatedBytes();

        stat.delta_rate = static_cast<Float64>(delta->getRows()) / stat.rows;
        stat.delta_memtable_rows = delta_memtable->getRows();
        stat.delta_memtable_size = delta_memtable->getBytes();
        stat.delta_memtable_column_files = delta_memtable->getColumnFileCount();
        stat.delta_memtable_delete_ranges = delta_memtable->getDeletes();
        stat.delta_persisted_page_id = delta_persisted->getId();
        stat.delta_persisted_rows = delta_persisted->getRows();
        stat.delta_persisted_size = delta_persisted->getBytes();
        stat.delta_persisted_column_files = delta_persisted->getColumnFileCount();
        stat.delta_persisted_delete_ranges = delta_persisted->getDeletes();
        stat.delta_cache_size = delta->getTotalCacheBytes();
        stat.delta_index_size = delta->getDeltaIndexBytes();

        stat.stable_page_id = stable->getId();
        stat.stable_rows = stable->getRows();
        stat.stable_size = stable->getBytes();
        stat.stable_dmfiles = stable->getDMFiles().size();
        if (stat.stable_dmfiles > 0)
            stat.stable_dmfiles_id_0 = stable->getDMFiles().front()->fileId();
        stat.stable_dmfiles_rows = stable->getDMFilesRows();
        stat.stable_dmfiles_size = stable->getDMFilesBytes();
        stat.stable_dmfiles_size_on_disk = stable->getDMFilesBytesOnDisk();
        stat.stable_dmfiles_packs = stable->getDMFilesPacks();

        stats.emplace_back(stat);
    }
    return stats;
}

std::optional<LocalIndexesStats> DeltaMergeStore::genLocalIndexStatsByTableInfo(const TiDB::TableInfo & table_info)
{
    auto local_index_infos = DM::initLocalIndexInfos(table_info, Logger::get());
    if (!local_index_infos)
        return std::nullopt;

    DM::LocalIndexesStats stats;
    for (const auto & index_info : *local_index_infos)
    {
        DM::LocalIndexStats index_stats;
        index_stats.column_id = index_info.column_id;
        index_stats.index_id = index_info.index_id;
        index_stats.index_kind = "HNSW";
        stats.emplace_back(std::move(index_stats));
    }
    return stats;
}

LocalIndexesStats DeltaMergeStore::getLocalIndexStats()
{
    auto local_index_infos_snap = getLocalIndexInfosSnapshot();
    if (!local_index_infos_snap)
        return {};

    std::shared_lock lock(read_write_mutex);

    LocalIndexesStats stats;
    for (const auto & index_info : *local_index_infos_snap)
    {
        LocalIndexStats index_stats;
        index_stats.column_id = index_info.column_id;
        index_stats.index_id = index_info.index_id;
        index_stats.index_kind = magic_enum::enum_name(index_info.kind); // like Vector

        for (const auto & [handle, segment] : segments)
        {
            UNUSED(handle);

            // Delta
            const auto & delta = segment->getDelta();
            if (const auto lock = delta->getLock(); lock)
            {
                index_stats.rows_delta_not_indexed += delta->getRows();
                const auto & persisted = delta->getPersistedFileSet();
                for (const auto & file : persisted->getFiles())
                {
                    if (const auto * tiny_file = file->tryToTinyFile();
                        tiny_file && tiny_file->hasIndex(index_stats.index_id))
                    {
                        index_stats.rows_delta_indexed += tiny_file->getRows();
                        index_stats.rows_delta_not_indexed -= tiny_file->getRows();
                    }
                }
            }

            // Stable
            {
                const auto & stable = segment->getStable();
                bool is_stable_indexed = true;
                for (const auto & dmfile : stable->getDMFiles())
                {
                    const auto [state, bytes] = dmfile->getLocalIndexState(index_info.column_id, index_info.index_id);
                    UNUSED(bytes);
                    switch (state)
                    {
                    case DMFileMeta::LocalIndexState::NoNeed:
                        // Regard as indexed, because column does not need any index
                    case DMFileMeta::LocalIndexState::IndexBuilt:
                        break;
                    case DMFileMeta::LocalIndexState::IndexPending:
                        is_stable_indexed = false;
                        break;
                    }
                }

                if (is_stable_indexed)
                {
                    index_stats.rows_stable_indexed += stable->getRows();
                }
                else
                {
                    index_stats.rows_stable_not_indexed += stable->getRows();
                }

                const auto index_build_error = segment->getIndexBuildError();
                // Set error_message to the first error_message we meet among all segments
                if (auto err_iter = index_build_error.find(index_info.index_id);
                    err_iter != index_build_error.end() && index_stats.error_message.empty())
                {
                    index_stats.error_message = err_iter->second;
                }
            }
        }

        stats.emplace_back(index_stats);
    }

    return stats;
}

} // namespace DM
} // namespace DB
