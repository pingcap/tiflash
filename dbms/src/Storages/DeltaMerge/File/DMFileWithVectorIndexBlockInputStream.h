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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterView.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/File/DMFileVectorIndexReader.h>
#include <Storages/DeltaMerge/File/DMFileWithVectorIndexBlockInputStream_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex_fwd.h>
#include <Storages/DeltaMerge/VectorIndexBlockInputStream.h>


namespace DB::DM
{

/**
 * @brief DMFileWithVectorIndexBlockInputStream is similar to DMFileBlockInputStream.
 * However it can read data efficiently with the help of vector index.
 *
 * General steps:
 * 1. Read all PK, Version and Del Marks (respecting Pack filters).
 * 2. Construct a bitmap of valid rows (in memory). This bitmap guides the reading of vector index to determine whether a row is valid or not.
 *
 *  Note: Step 1 and 2 simply rely on the BitmapFilter to avoid repeat IOs.
 *  BitmapFilter is global, which provides row valid info for all DMFile + Delta.
 *  What we need is which rows are valid in THIS DMFile.
 *  To transform a global BitmapFilter result into a local one, RowOffsetTracker is used.
 *
 * 3. Perform a vector search for Top K vector rows. We now have K row_ids whose vector distance is close.
 * 4. Map these row_ids to packids as the new pack filter.
 * 5. Read from other columns with the new pack filter.
 *     For each read, join other columns and vector column together.
 *
 *  Step 3~4 is performed lazily at first read.
 *
 * Before constructing this class, the caller must ensure that vector index
 * exists on the corresponding column. If the index does not exist, the caller
 * should use the standard DMFileBlockInputStream.
 */
class DMFileWithVectorIndexBlockInputStream : public VectorIndexBlockInputStream
{
public:
    static DMFileWithVectorIndexBlockInputStreamPtr create(
        const ANNQueryInfoPtr & ann_query_info,
        const DMFilePtr & dmfile,
        Block && header,
        DMFileReader && reader,
        ColumnDefine && vec_cd,
        const ScanContextPtr & scan_context,
        const VectorIndexCachePtr & vec_index_cache,
        const BitmapFilterView & valid_rows,
        const String & tracing_id)
    {
        return std::make_shared<DMFileWithVectorIndexBlockInputStream>(
            ann_query_info,
            dmfile,
            std::move(header),
            std::move(reader),
            std::move(vec_cd),
            scan_context,
            vec_index_cache,
            valid_rows,
            tracing_id);
    }

    explicit DMFileWithVectorIndexBlockInputStream(
        const ANNQueryInfoPtr & ann_query_info_,
        const DMFilePtr & dmfile_,
        Block && header_,
        DMFileReader && reader_,
        ColumnDefine && vec_cd_,
        const ScanContextPtr & scan_context_,
        const VectorIndexCachePtr & vec_index_cache_,
        const BitmapFilterView & valid_rows_,
        const String & tracing_id);

    ~DMFileWithVectorIndexBlockInputStream() override;

public:
    Block read() override;

    String getName() const override { return "DMFileWithVectorIndex"; }

    Block getHeader() const override { return header; }

    std::vector<VectorIndexViewer::SearchResult> load() override;

    void setSelectedRows(const std::span<const UInt32> & selected_rows) override;

private:
    size_t index_reader_next_pack_id = 0;
    size_t index_reader_next_row_id = 0;

    // Load vector index and update sorted_results.
    void internalLoad();

    // Update the RSResult according to the sorted_results.
    void updateRSResult();

private:
    const LoggerPtr log;

    const ANNQueryInfoPtr ann_query_info;
    const DMFilePtr dmfile;

    // The header contains columns from reader and vec_cd
    Block header;
    // Vector column should be excluded in the reader
    DMFileReader reader;
    // Note: ColumnDefine comes from read path does not have vector_index fields.
    const ColumnDefine vec_cd;
    const ScanContextPtr scan_context;
    const DMFileVectorIndexReaderPtr vec_index_reader;

    // Set after load().
    VectorIndexViewerPtr vec_index = nullptr;
    // VectorColumnFromIndexReaderPtr vec_column_reader = nullptr;
    // Set after load(). Used to filter the output rows.
    std::vector<UInt32> sorted_results{}; // Key is rowid
    IColumn::Filter filter;

    bool loaded = false;

    double duration_read_from_other_columns_seconds = 0;
    size_t valid_packs_before_search = 0;
    size_t valid_packs_after_search = 0;
};

} // namespace DB::DM
