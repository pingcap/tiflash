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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/VersionChain/Common.h>
#include <Storages/DeltaMerge/VersionChain/DeleteMarkFilter.h>

namespace DB::DM
{
UInt32 buildDeleteMarkFilterBlock(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr & data_provider,
    const ColumnFile & cf,
    const UInt32 start_row_id,
    BitmapFilter & filter)
{
    assert(cf.isInMemoryFile() || cf.isTinyFile());
    const auto rows = cf.getRows();
    if (rows == 0)
        return 0;

    // ColumnDefine of delete mark column is never changed.
    static const auto delmark_cds_ptr = std::make_shared<ColumnDefines>(1, getTagColumnDefine());
    auto cf_reader = cf.getReader(dm_context, data_provider, delmark_cds_ptr, ReadTag::MVCC);
    auto block = cf_reader->readNextBlock();
    RUNTIME_CHECK_MSG(
        rows == block.rows(),
        "ColumnFile<{}> returns {} rows. Read all rows in one block is required!",
        cf.toString(),
        block.rows());
    const auto deleteds = ColumnView<UInt8>(*(block.begin()->column));
    UInt32 filtered_out_rows = 0;
    for (UInt32 i = 0; i < deleteds.size(); ++i)
    {
        if (filter[start_row_id + i] && deleteds[i])
        {
            filter[start_row_id + i] = 0;
            ++filtered_out_rows;
        }
    }
    return filtered_out_rows;
}

UInt32 buildDeleteMarkFilterDMFile(
    const DMContext & dm_context,
    const DMFilePtr & dmfile,
    const UInt32 start_pack_id,
    const RSResults & rs_results,
    const ssize_t start_row_id,
    BitmapFilter & filter)
{
    auto need_read_packs = std::make_shared<IdSet>();
    std::unordered_map<UInt32, UInt32> start_row_id_of_need_read_packs; // pack_id -> start_row_id
    const auto & pack_stats = dmfile->getPackStats();
    const auto & pack_properties = dmfile->getPackProperties();
    UInt32 processed_rows = 0;
    for (UInt32 i = 0; i < rs_results.size(); ++i)
    {
        const UInt32 pack_id = start_pack_id + i;
        const UInt32 pack_start_row_id = start_row_id + processed_rows;
        processed_rows += pack_stats[pack_id].rows;

        // Packs that filtered out by rs_results is handle by RowKeyFilter.
        // So we just skip these packs here.
        if (!rs_results[i].isUse())
            continue;

        if (pack_properties.property(pack_id).deleted_rows() > 0)
        {
            need_read_packs->insert(pack_id);
            start_row_id_of_need_read_packs.emplace(pack_id, pack_start_row_id);
        }
    }

    if (need_read_packs->empty())
        return 0;

    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    builder.onlyReadOnePackEveryTime().setReadPacks(need_read_packs).setReadTag(ReadTag::MVCC);
    auto stream = builder.build(dmfile, {getTagColumnDefine()}, {}, dm_context.scan_context);
    UInt32 filtered_out_rows = 0;
    for (auto pack_id : *need_read_packs)
    {
        auto block = stream->read();
        RUNTIME_CHECK(block.rows() == pack_stats[pack_id].rows, block.rows(), pack_stats[pack_id].rows);
        const auto deleteds = ColumnView<UInt8>(*(block.begin()->column));
        const auto itr = start_row_id_of_need_read_packs.find(pack_id);
        RUNTIME_CHECK(itr != start_row_id_of_need_read_packs.end(), start_row_id_of_need_read_packs, pack_id);
        const UInt32 pack_start_row_id = itr->second;
        for (UInt32 i = 0; i < deleteds.size(); ++i)
        {
            if (filter[pack_start_row_id + i] && deleteds[i])
            {
                filter[pack_start_row_id + i] = 0;
                ++filtered_out_rows;
            }
        }
    }
    return filtered_out_rows;
}

UInt32 buildDeleteMarkFilterColumnFileBig(
    const DMContext & dm_context,
    const ColumnFileBig & cf_big,
    const ssize_t start_row_id,
    BitmapFilter & filter)
{
    auto [valid_handle_res, valid_start_pack_id]
        = getClippedRSResultsByRange(dm_context, cf_big.getFile(), cf_big.getRange());
    if (valid_handle_res.empty())
        return 0;
    return buildDeleteMarkFilterDMFile(
        dm_context,
        cf_big.getFile(),
        valid_start_pack_id,
        valid_handle_res,
        start_row_id,
        filter);
}

UInt32 buildDeleteMarkFilterStable(
    const DMContext & dm_context,
    const StableValueSpace::Snapshot & stable,
    const DMFilePackFilterResultPtr & stable_filter_res,
    BitmapFilter & filter)
{
    const auto & dmfiles = stable.getDMFiles();
    RUNTIME_CHECK(dmfiles.size() == 1, dmfiles.size());
    return buildDeleteMarkFilterDMFile(
        dm_context,
        dmfiles[0],
        /*start_pack_id*/ 0,
        stable_filter_res->getPackRes(),
        /*start_row_id*/ 0,
        filter);
}

UInt32 buildDeleteMarkFilter(
    const DMContext & dm_context,
    const SegmentSnapshot & snapshot,
    const DMFilePackFilterResultPtr & stable_filter_res,
    BitmapFilter & filter)
{
    const auto & stable = *(snapshot.stable);
    const UInt32 stable_rows = stable.getDMFilesRows();
    const UInt32 total_rows = snapshot.delta->getRows() + stable_rows;
    assert(filter.size() == total_rows);
    const auto cfs = snapshot.delta->getColumnFiles();
    const auto & data_provider = snapshot.delta->getDataProvider();

    auto filtered_out_rows = buildDeleteMarkFilterStable(dm_context, stable, stable_filter_res, filter);
    auto read_rows = stable_rows;
    for (const auto & cf : cfs)
    {
        if (cf->isDeleteRange())
            continue;

        const UInt32 cf_rows = cf->getRows();
        const UInt32 start_row_id = read_rows;
        read_rows += cf_rows;

        // TODO: add deleted_rows in tiny file and we can skip this column file is deleted_rows equals to 0.
        if (cf->isInMemoryFile() || cf->isTinyFile())
        {
            filtered_out_rows += buildDeleteMarkFilterBlock(dm_context, data_provider, *cf, start_row_id, filter);
            continue;
        }

        if (const auto * cf_big = cf->tryToBigFile(); cf_big)
        {
            filtered_out_rows += buildDeleteMarkFilterColumnFileBig(dm_context, *cf_big, start_row_id, filter);
            continue;
        }
        RUNTIME_CHECK_MSG(false, "{}: unknow ColumnFile type", cf->toString());
    }
    RUNTIME_CHECK(read_rows == total_rows, read_rows, total_rows);
    return filtered_out_rows;
}
} // namespace DB::DM
