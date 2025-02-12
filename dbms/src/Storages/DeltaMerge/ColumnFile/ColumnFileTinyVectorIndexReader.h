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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterView.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/Index/LocalIndex_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>


namespace DB::DM
{

class ColumnFileTinyVectorIndexReader
{
private:
    const ColumnFileTiny & tiny_file;
    const IColumnFileDataProviderPtr data_provider;

    const ANNQueryInfoPtr ann_query_info;
    // Set after load().
    VectorIndexViewerPtr vec_index;
    const BitmapFilterView valid_rows;
    // Note: ColumnDefine comes from read path does not have vector_index fields.
    const ColumnDefine vec_cd;
    // Global local index cache
    const LocalIndexCachePtr local_index_cache;
    LoggerPtr log;

    // Performance statistics
    struct PerfStat
    {
        double load_vec_index_and_results_seconds = 0;
        double read_vec_column_seconds = 0;
        size_t discarded_nodes = 0;
        size_t visited_nodes = 0;
        size_t returned_rows = 0;
        // Whether the vector index is loaded from cache.
        bool load_from_cache = true;
    };
    PerfStat perf_stat;

    // Whether the vector index and search results are loaded.
    bool loaded = false;

public:
    ColumnFileTinyVectorIndexReader(
        const ColumnFileTiny & tiny_file_,
        const IColumnFileDataProviderPtr & data_provider_,
        const ANNQueryInfoPtr & ann_query_info_,
        const BitmapFilterView && valid_rows_,
        const ColumnDefine & vec_cd_,
        const LocalIndexCachePtr & local_index_cache_)
        : tiny_file(tiny_file_)
        , data_provider(data_provider_)
        , ann_query_info(ann_query_info_)
        , valid_rows(std::move(valid_rows_))
        , vec_cd(vec_cd_)
        , local_index_cache(local_index_cache_)
        , log(Logger::get())
    {}

    ~ColumnFileTinyVectorIndexReader();

    // Read vector column data with the specified rowids.
    void read(
        MutableColumnPtr & vec_column,
        const std::span<const VectorIndexViewer::Key> & read_rowids,
        size_t rowid_start_offset);

    // Load vector index and search results.
    // Return the rowids of the selected rows.
    std::vector<VectorIndexViewer::SearchResult> load();

private:
    void loadVectorIndex();
    std::vector<VectorIndexViewer::SearchResult> loadVectorSearchResult();
};

using ColumnFileTinyVectorIndexReaderPtr = std::shared_ptr<ColumnFileTinyVectorIndexReader>;

} // namespace DB::DM
