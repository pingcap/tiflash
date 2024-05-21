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

#include <Storages/DeltaMerge/File/VectorColumnFromIndexReader.h>

namespace DB::DM
{

std::vector<UInt32> VectorColumnFromIndexReader::calcPackStartRowID(const DMFileMeta::PackStats & pack_stats)
{
    std::vector<UInt32> pack_start_rowid(pack_stats.size());
    UInt32 rowid = 0;
    for (size_t i = 0, i_max = pack_stats.size(); i < i_max; i++)
    {
        pack_start_rowid[i] = rowid;
        rowid += pack_stats[i].rows;
    }
    return pack_start_rowid;
}

MutableColumnPtr VectorColumnFromIndexReader::calcResultsByPack(
    std::vector<VectorIndexViewer::Key> && results,
    const DMFileMeta::PackStats & pack_stats,
    const std::vector<UInt32> & pack_start_rowid)
{
    auto column = ColumnArray::create(ColumnUInt32::create());

    // results must be in ascending order.
    std::sort(results.begin(), results.end());
    // results must not contain duplicates. Usually there should be no duplicates.
    results.erase(std::unique(results.begin(), results.end()), results.end());

    std::vector<UInt32> offsets_in_pack;
    size_t results_it = 0;
    const size_t results_it_max = results.size();
    for (size_t pack_id = 0, pack_id_max = pack_start_rowid.size(); pack_id < pack_id_max; pack_id++)
    {
        offsets_in_pack.clear();

        UInt32 pack_start = pack_start_rowid[pack_id];
        UInt32 pack_end = pack_start + pack_stats[pack_id].rows;

        while (results_it < results_it_max //
               && results[results_it] >= pack_start //
               && results[results_it] < pack_end)
        {
            offsets_in_pack.push_back(results[results_it] - pack_start);
            results_it++;
        }

        column->insertData(
            reinterpret_cast<const char *>(offsets_in_pack.data()),
            offsets_in_pack.size() * sizeof(UInt32));
    }

    RUNTIME_CHECK_MSG(results_it == results_it_max, "All packs has been visited but not all results are consumed");

    return column;
}

void VectorColumnFromIndexReader::read(MutableColumnPtr & column, size_t start_pack_id, UInt32 read_rows)
{
    std::vector<Float32> value;
    const auto * results_by_pack = checkAndGetColumn<ColumnArray>(this->results_by_pack.get());
    checkAndGetColumn<ColumnArray>(column.get());

    size_t pack_id = start_pack_id;
    UInt32 remaining_rows_in_pack = pack_stats[pack_id].rows;

    while (read_rows > 0)
    {
        if (remaining_rows_in_pack == 0)
        {
            // If this pack is drained but we still need to read more rows, let's read from next pack.
            pack_id++;
            RUNTIME_CHECK(pack_id < pack_stats.size());
            remaining_rows_in_pack = pack_stats[pack_id].rows;
        }

        UInt32 expect_result_rows = std::min(remaining_rows_in_pack, read_rows);
        UInt32 filled_result_rows = 0;

        auto offsets_in_pack = results_by_pack->getDataAt(pack_id);
        auto offsets_in_pack_n = results_by_pack->sizeAt(pack_id);
        RUNTIME_CHECK(offsets_in_pack.size == offsets_in_pack_n * sizeof(UInt32));

        // Note: offsets_in_pack_n may be 0, means there is no results in this pack.
        for (size_t i = 0; i < offsets_in_pack_n; ++i)
        {
            UInt32 offset_in_pack = reinterpret_cast<const UInt32 *>(offsets_in_pack.data)[i];
            RUNTIME_CHECK(filled_result_rows <= offset_in_pack);
            if (offset_in_pack > filled_result_rows)
            {
                UInt32 nulls = offset_in_pack - filled_result_rows;
                // Insert [] if column is Not Null, or NULL if column is Nullable
                column->insertManyDefaults(nulls);
                filled_result_rows += nulls;
            }
            RUNTIME_CHECK(filled_result_rows == offset_in_pack);

            // TODO: We could fill multiple rows if rowid is continuous.
            VectorIndexViewer::Key rowid = pack_start_rowid[pack_id] + offset_in_pack;
            index->get(rowid, value);
            column->insertData(reinterpret_cast<const char *>(value.data()), value.size() * sizeof(Float32));
            filled_result_rows++;
        }

        if (filled_result_rows < expect_result_rows)
        {
            size_t nulls = expect_result_rows - filled_result_rows;
            // Insert [] if column is Not Null, or NULL if column is Nullable
            column->insertManyDefaults(nulls);
            filled_result_rows += nulls;
        }

        RUNTIME_CHECK(filled_result_rows == expect_result_rows);
        remaining_rows_in_pack -= filled_result_rows;
        read_rows -= filled_result_rows;
    }
}

} // namespace DB::DM
