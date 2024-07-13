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

#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/VectorColumnFromIndexReader_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>

#include <vector>

namespace DB::DM
{

/**
 * @brief VectorColumnFromIndexReader reads vector column data from the index
 * while maintaining the same column layout as if it was read from the DMFile.
 * For example, when we want to read vector column data of row id [1, 5, 10],
 * this reader will return [NULL, VEC, NULL, NULL, NULL, VEC, ....].
 *
 * Note: The term "row id" in this class refers to the row offset in this DMFile.
 * It is a file-level row id, not a global row id.
 */
class VectorColumnFromIndexReader
{
private:
    const DMFilePtr dmfile; // Keep a reference of dmfile to keep pack_stats valid.
    const DMFileMeta::PackStats & pack_stats;
    const std::vector<UInt32> pack_start_rowid;

    const VectorIndexViewerPtr index;
    /// results_by_pack[i]=[a,b,c...] means pack[i]'s row offset [a,b,c,...] is contained in the result set.
    /// The rowid of a is pack_start_rowid[i]+a.
    MutableColumnPtr /* ColumnArray of UInt32 */ results_by_pack;

private:
    static std::vector<UInt32> calcPackStartRowID(const DMFileMeta::PackStats & pack_stats);

    static MutableColumnPtr calcResultsByPack(
        const std::vector<VectorIndexViewer::Key> & results,
        const DMFileMeta::PackStats & pack_stats,
        const std::vector<UInt32> & pack_start_rowid);

public:
    /// VectorIndex::Key is the offset of the row in the DMFile (file-level row id),
    /// including NULLs and delete marks.
    explicit VectorColumnFromIndexReader(
        const DMFilePtr & dmfile_,
        const VectorIndexViewerPtr & index_,
        const std::vector<VectorIndexViewer::Key> & sorted_results_)
        : dmfile(dmfile_)
        , pack_stats(dmfile_->getPackStats())
        , pack_start_rowid(calcPackStartRowID(pack_stats))
        , index(index_)
        , results_by_pack(calcResultsByPack(sorted_results_, pack_stats, pack_start_rowid))
    {}

    void read(MutableColumnPtr & column, size_t start_pack_id, UInt32 read_rows);
};

} // namespace DB::DM
