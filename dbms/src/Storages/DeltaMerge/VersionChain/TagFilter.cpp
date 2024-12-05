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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/VersionFilter.h>

namespace DB::DM
{
UInt32 buildTagFilterBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const UInt32 start_row_id,
    std::vector<UInt8> & filter)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());
    auto cf_reader = cf.getReader(dm_context, data_provider, getTagColumnDefinesPtr(), ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(!cf_reader->readNextBlock(), "{}: read all rows in one block is required!", cf.toString());
    auto tag_col = block.begin()->column;
    const auto & tags = *toColumnVectorDataPtr<UInt8>(tag_col); // Must success.
    std::copy_n(tags.begin(), tags.size(), std::next(filter.begin(), start_row_id));
    return tags.size();
}

UInt32 buildTagFilterDMFile(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const RowKeyRanges & segment_ranges,
    const ssize_t start_row_id,
    std::vector<UInt8> & filter)
{
    auto pack_filter = DMFilePackFilter::loadFrom(
        dmfile,
        dm_context.global_context.getMinMaxIndexCache(),
        true,
        segment_ranges,
        EMPTY_RS_OPERATOR,
        {},
        dm_context.global_context.getFileProvider(),
        dm_context.getReadLimiter(),
        dm_context.scan_context,
        dm_context.tracing_id,
        ReadTag::MVCC);
    const auto & seg_range_handle_res = pack_filter.getHandleRes();
    const auto valid_start_itr
        = std::find_if(seg_range_handle_res.begin(), seg_range_handle_res.end(), [](RSResult r) { return r.isUse(); });
    RUNTIME_CHECK_MSG(
        valid_start_itr != seg_range_handle_res.end(),
        "dmfile={}, segment_ranges={}, start_row_id={}",
        dmfile->path(),
        toDebugString(segment_ranges),
        start_row_id);
    const auto valid_end_itr
        = std::find_if(valid_start_itr, seg_range_handle_res.end(), [](RSResult r) { return !r.isUse(); });
    const auto valid_start_pack_id = valid_start_itr - seg_range_handle_res.begin();
    const auto valid_pack_count = valid_end_itr - valid_start_itr;

    auto read_packs = std::make_shared<IdSet>();
    UInt32 need_read_rows = 0;
    std::unordered_map<UInt32, UInt32> read_pack_to_start_row_ids;

    const auto & pack_stats = dmfile->getPackStats();
    const auto & pack_properties = dmfile->getPackProperties();
    UInt32 rows = 0;
    for (UInt32 i = 0; i < valid_pack_count; ++i)
    {
        const UInt32 pack_id = valid_start_pack_id + i;
        const UInt32 pack_start_row_id = start_row_id + rows;
        if (pack_properties.property(pack_id).deleted_rows() > 0)
        {
            read_packs->insert(pack_id);
            read_pack_to_start_row_ids.emplace(pack_id, pack_start_row_id);
            need_read_rows += pack_stats[pack_id].rows;
        }
        rows += pack_stats[pack_id].rows;
    }

    if (need_read_rows == 0)
        return rows;

    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    builder.setRowsThreshold(need_read_rows).setReadPacks(read_packs).setReadTag(ReadTag::MVCC);
    auto stream = builder.build(dmfile, {getTagColumnDefine()}, {}, dm_context.scan_context);
    auto block = stream->read();
    RUNTIME_CHECK(block.rows() == need_read_rows, block.rows(), need_read_rows);
    auto tag_col = block.begin()->column;
    const auto & tags = *toColumnVectorDataPtr<UInt8>(tag_col); // Must success

    UInt32 offset = 0;
    for (auto pack_id : *read_packs)
    {
        const auto itr = read_pack_to_start_row_ids.find(pack_id);
        RUNTIME_CHECK(itr != read_pack_to_start_row_ids.end(), read_pack_to_start_row_ids, pack_id);
        const UInt32 pack_start_row_id = itr->second;
        std::copy_n(
            std::next(tags.begin(), offset),
            pack_stats[pack_id].rows,
            std::next(filter.begin(), pack_start_row_id));
        offset += pack_stats[pack_id].rows;
    }
    return rows;
}

UInt32 buildTagFilterColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const ssize_t start_row_id,
    std::vector<UInt8> & filter)
{
    auto dmfile = cf_big.getFile();
    auto segment_ranges = RowKeyRanges{cf_big.getRange()};
    return buildTagFilterDMFile(dm_context, dmfile, segment_ranges, start_row_id, filter);
}

UInt32 buildTagFilterStable(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    std::vector<UInt8> & filter)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    return buildTagFilterDMFile(dm_context, dmfiles[0], {}, 0, filter);
}

std::vector<UInt8> buildTagFilter(const DMContext & dm_context, const SegmentSnapshot & snapshot)
{
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    std::vector<UInt8> filter(total_rows, /*default_value*/ 1);

    auto read_rows = buildTagFilterStable(dm_context, stable, filter);
    RUNTIME_CHECK(stable_rows == read_rows, stable_rows, read_rows);

    auto cfs = delta.getPersistedFileSetSnapshot()->getColumnFiles();
    const auto & memory_cfs = delta.getMemTableSetSnapshot()->getColumnFiles();
    cfs.insert(cfs.end(), memory_cfs.begin(), memory_cfs.end());

    auto storage_snap = std::make_shared<StorageSnapshot>(
        *dm_context.storage_pool,
        dm_context.getReadLimiter(),
        dm_context.tracing_id,
        /*snapshot_read*/ true);
    auto data_from_storage_snap = ColumnFileDataProviderLocalStoragePool::create(storage_snap);

    for (auto itr = cfs.begin(); itr != cfs.end(); ++itr)
    {
        const auto & cf = *itr;
        if (cf->isDeleteRange())
            continue;

        const UInt32 cf_rows = cf->getRows();
        const UInt32 start_row_id = read_rows;
        read_rows += cf_rows;

        // TODO: add deleted_rows in tiny file
        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            const auto n = buildTagFilterBlock(dm_context, data_from_storage_snap, *cf, start_row_id, filter);
            RUNTIME_CHECK(cf_rows == n, cf_rows, n);
            continue;
        }

        if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            const auto n = buildTagFilterColumnFileBig(dm_context, *cf_big, start_row_id, filter);
            RUNTIME_CHECK(cf_rows == n, cf_rows, n);
            continue;
        }
        RUNTIME_CHECK_MSG(false, "{}: unknow ColumnFile type", cf->toString());
    }
    RUNTIME_CHECK(read_rows == total_rows, read_rows, total_rows);
    return filter;
}
} // namespace DB::DM
