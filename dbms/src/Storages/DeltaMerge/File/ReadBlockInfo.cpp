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

#include <Common/config.h> // For ENABLE_CLARA
#include <Storages/DeltaMerge/File/ReadBlockInfo.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/IProvideVectorIndex.h>

#include <span>

#if ENABLE_CLARA
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/IProvideFullTextIndex.h>
#endif

namespace DB::DM
{

ReadBlockInfos ReadBlockInfo::create(
    const RSResults & pack_res,
    const DMFileMeta::PackStats & pack_stats,
    const size_t read_pack_limit,
    const size_t rows_threshold_per_read)
{
    ReadBlockInfos read_block_infos;
    size_t start_pack_id = 0;
    size_t read_rows = 0;
    auto prev_block_pack_res = RSResult::All;
    for (size_t pack_id = 0; pack_id < pack_res.size(); ++pack_id)
    {
        bool is_use = pack_res[pack_id].isUse();
        bool reach_limit = pack_id - start_pack_id >= read_pack_limit || read_rows >= rows_threshold_per_read;
        // Get continuous packs with RSResult::All but don't split the read if it is too small.
        // Too small block may hurts performance.
        bool break_all_match = prev_block_pack_res.allMatch() && !pack_res[pack_id].allMatch()
            && read_rows >= rows_threshold_per_read / 2;

        if (!is_use)
        {
            if (read_rows > 0)
                read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, prev_block_pack_res, read_rows);
            // Current pack is not included in the next read_block_info
            start_pack_id = pack_id + 1;
            read_rows = 0;
            prev_block_pack_res = RSResult::All;
        }
        else if (reach_limit || break_all_match)
        {
            if (read_rows > 0)
                read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, prev_block_pack_res, read_rows);
            // Current pack must be included in the next read_block_info
            start_pack_id = pack_id;
            read_rows = pack_stats[pack_id].rows;
            prev_block_pack_res = pack_res[pack_id];
        }
        else
        {
            prev_block_pack_res = prev_block_pack_res && pack_res[pack_id];
            read_rows += pack_stats[pack_id].rows;
        }
    }
    if (read_rows > 0)
        read_block_infos.emplace_back(start_pack_id, pack_res.size() - start_pack_id, prev_block_pack_res, read_rows);
    return read_block_infos;
}

template <typename T>
ReadBlockInfos ReadBlockInfo::createWithRowIDs(
    std::span<T> row_ids,
    const std::vector<size_t> & pack_offset,
    const RSResults & pack_res,
    const DMFileMeta::PackStats & pack_stats,
    const size_t rows_threshold_per_read)
{
    ReadBlockInfos read_block_infos;
    size_t start_pack_id = 0;
    size_t read_rows = 0;
    auto prev_block_pack_res = RSResult::All;
    auto sorted_results_it = row_ids.begin();
    size_t pack_id = 0;
    for (; pack_id < pack_stats.size(); ++pack_id)
    {
        if (sorted_results_it == row_ids.end())
            break;
        const auto begin = std::lower_bound( //
            sorted_results_it,
            row_ids.end(),
            pack_offset[pack_id],
            [](const auto & lhs, const auto & rhs) { return lhs.rowid < rhs; });
        const auto end = std::lower_bound( //
            begin,
            row_ids.end(),
            pack_offset[pack_id] + pack_stats[pack_id].rows,
            [](const auto & lhs, const auto & rhs) { return lhs.rowid < rhs; });
        bool is_use = begin != end;
        bool reach_limit = read_rows >= rows_threshold_per_read;
        bool break_all_match = prev_block_pack_res.allMatch() && !pack_res[pack_id].allMatch()
            && read_rows >= rows_threshold_per_read / 2;

        if (!is_use)
        {
            if (read_rows > 0)
                read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, prev_block_pack_res, read_rows);
            start_pack_id = pack_id + 1;
            read_rows = 0;
            prev_block_pack_res = RSResult::All;
        }
        else if (reach_limit || break_all_match)
        {
            if (read_rows > 0)
                read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, prev_block_pack_res, read_rows);
            start_pack_id = pack_id;
            read_rows = pack_stats[pack_id].rows;
            prev_block_pack_res = pack_res[pack_id];
        }
        else
        {
            prev_block_pack_res = prev_block_pack_res && pack_res[pack_id];
            read_rows += pack_stats[pack_id].rows;
        }

        sorted_results_it = end;
    }
    if (read_rows > 0)
        read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, prev_block_pack_res, read_rows);

    RUNTIME_CHECK_MSG(sorted_results_it == row_ids.end(), "All results are not consumed");
    return read_block_infos;
}

template ReadBlockInfos ReadBlockInfo::createWithRowIDs<IProvideVectorIndex::SearchResult>(
    std::span<IProvideVectorIndex::SearchResult> row_ids,
    const std::vector<size_t> & pack_offset,
    const RSResults & pack_res,
    const DMFileMeta::PackStats & pack_stats,
    size_t rows_threshold_per_read);

#if ENABLE_CLARA
template ReadBlockInfos ReadBlockInfo::createWithRowIDs<IProvideFullTextIndex::SearchResult>(
    std::span<IProvideFullTextIndex::SearchResult> row_ids,
    const std::vector<size_t> & pack_offset,
    const RSResults & pack_res,
    const DMFileMeta::PackStats & pack_stats,
    size_t rows_threshold_per_read);
#endif

} // namespace DB::DM
