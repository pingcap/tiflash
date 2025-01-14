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

#pragma once

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile_fwd.h>
#include <Storages/DeltaMerge/Filter/ColumnValueSet.h>
#include <Storages/DeltaMerge/Index/InvertedIndex.h>
#include <Storages/DeltaMerge/Index/LocalIndex_fwd.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>

namespace DB::DM
{

class DMFileInvertedIndexReader
{
private:
    const DMFilePtr & dmfile;
    const ColumnValueSetPtr column_value_set;
    // Global local index cache
    const LocalIndexCachePtr local_index_cache;

    // Performance statistics
    struct PerfStat
    {
        double duration_search;
        double duration_load_index;
        size_t index_size;
        size_t selected_nodes;
        bool has_s3_download;
        bool has_load_from_file;

        String toString() const;
    };
    PerfStat perf_stat;

    bool loaded = false;

public:
    DMFileInvertedIndexReader(
        const ColumnValueSetPtr & column_value_set_,
        const DMFilePtr & dmfile_,
        const LocalIndexCachePtr & local_index_cache_)
        : dmfile(dmfile_)
        , column_value_set(column_value_set_)
        , local_index_cache(local_index_cache_)
        , perf_stat()
    {}

    ~DMFileInvertedIndexReader();

    // Load inverted index and search results.
    BitmapFilterPtr load();

private:
    InvertedIndexViewerPtr loadInvertedIndex(const ColumnValueSetPtr & set);
    BitmapFilterPtr loadSearchResult(
        const InvertedIndexViewerPtr & inverted_index,
        const ColumnValueSetPtr & set,
        size_t size);
};

using DMFileInvertedIndexReaderPtr = std::shared_ptr<DMFileInvertedIndexReader>;

} // namespace DB::DM
