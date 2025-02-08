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
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/DeletedFilter.h>

namespace DB::DM
{
UInt32 buildDeletedFilterBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const UInt32 start_row_id,
    IColumn::Filter & filter)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());
    const auto rows = cf.getRows();
    if (rows == 0)
        return 0;

    static const auto delmark_cds_ptr = std::make_shared<ColumnDefines>(1, getTagColumnDefine());
    auto cf_reader = cf.getReader(dm_context, data_provider, delmark_cds_ptr, ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(
        rows == block.rows(),
        "ColumnFile<{}> returns {} rows. Read all rows in one block is required!",
        cf.toString(),
        block.rows());
    const auto & deleteds = *toColumnVectorDataPtr<UInt8>(block.begin()->column); // Must success.
    UInt32 filered_out_rows = 0;
    for (UInt32 i = 0; i < deleteds.size(); ++i)
    {
        filter[start_row_id + i] = filter[start_row_id + i] && !deleteds[i];
        filtered_out_rows += deleteds[i];
    }
    return filtered_out_rows;
}

UInt32 buildDeletedFilterDMFile(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const std::optional<RowKeyRange> & segment_range,
    const ssize_t start_row_id,
    IColumn::Filter & filter)
{
    auto [valid_handle_res, valid_start_pack_id] = getClippedRSResultsByRanges(dm_context, dmfile, segment_range);
    if (valid_handle_res.empty())
        return 0;

    auto need_read_packs = std::make_shared<IdSet>();
    UInt32 need_read_rows = 0;
    std::unordered_map<UInt32, UInt32> need_read_pack_to_start_row_ids;

    const auto & pack_stats = dmfile->getPackStats();
    const auto & pack_properties = dmfile->getPackProperties();
    UInt32 processed_rows = 0;
    for (UInt32 i = 0; i < valid_handle_res.size(); ++i)
    {
        const UInt32 pack_id = valid_start_pack_id + i;
        const UInt32 pack_start_row_id = start_row_id + processed_rows;
        if (pack_properties.property(pack_id).deleted_rows() > 0)
        {
            need_read_packs->insert(pack_id);
            need_read_pack_to_start_row_ids.emplace(pack_id, pack_start_row_id);
            need_read_rows += pack_stats[pack_id].rows;
        }
        processed_rows += pack_stats[pack_id].rows;
    }

    if (need_read_rows == 0)
        return 0;

    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    builder.onlyReadOnePackEveryTime().setReadPacks(need_read_packs).setReadTag(ReadTag::MVCC);
    auto stream = builder.build(dmfile, {getTagColumnDefine()}, {}, dm_context.scan_context);
    UInt32 read_rows = 0;
    for (auto pack_id : *need_read_packs)
    {
        auto block = stream->read();
        RUNTIME_CHECK(block.rows() == pack_stats[pack_id].rows, block.rows(), pack_stats[pack_id].rows);
        const auto & deleteds = *toColumnVectorDataPtr<UInt8>(block.begin()->column); // Must success

        const auto itr = need_read_pack_to_start_row_ids.find(pack_id);
        RUNTIME_CHECK(itr != need_read_pack_to_start_row_ids.end(), need_read_pack_to_start_row_ids, pack_id);
        const UInt32 pack_start_row_id = itr->second;
        UInt32 filtered_out_rows = 0;
        for (UInt32 i = 0; i < deleteds.size(); ++i)
        {
            filter[pack_start_row_id + i] = filter[pack_start_row_id + i] && !deleteds[i];
            filtered_out_rows += deleteds[i];
        }
        read_rows += pack_stats[pack_id].rows;
    }
    RUNTIME_CHECK(read_rows == need_read_rows, read_rows, need_read_rows);
    return filtered_out_rows;
}

UInt32 buildDeletedFilterColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const ssize_t start_row_id,
    IColumn::Filter & filter)
{
    return buildDeletedFilterDMFile(dm_context, cf_big.getFile(), cf_big.getRange(), start_row_id, filter);
}

UInt32 buildDeletedFilterStable(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    IColumn::Filter & filter)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    return buildDeletedFilterDMFile(dm_context, dmfiles[0], std::nullopt, 0, filter);
}

void buildDeletedFilter(const DMContext & dm_context, const SegmentSnapshot & snapshot, IColumn::Filter & filter)
{
    const auto & delta = *(snapshot.delta);
    const auto & stable = *(snapshot.stable);
    const UInt32 delta_rows = delta.getRows();
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = delta_rows + stable_rows;
    const auto cfs = delta.getColumnFiles();
    const auto & data_provider = delta.getDataProvider();
    assert(filter.size() == total_rows);

    auto filtered_out_rows = buildDeletedFilterStable(dm_context, stable, filter);
    auto read_rows = stable_rows;
    for (const auto & cf : cfs)
    {
        if (cf->isDeleteRange())
            continue;

        const UInt32 cf_rows = cf->getRows();
        const UInt32 start_row_id = read_rows;
        read_rows += cf_rows;

        // TODO: add deleted_rows in tiny file
        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            filered_out_rows += buildDeletedFilterBlock(dm_context, data_provider, *cf, start_row_id, filter);
            continue;
        }

        if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            filered_out_rows += buildDeletedFilterColumnFileBig(dm_context, *cf_big, start_row_id, filter);
            continue;
        }
        RUNTIME_CHECK_MSG(false, "{}: unknow ColumnFile type", cf->toString());
    }
    RUNTIME_CHECK(read_rows == total_rows, read_rows, total_rows);
    return filered_out_rows;
}
} // namespace DB::DM
