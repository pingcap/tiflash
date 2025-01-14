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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterView.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/Filter/ColumnValueSet.h>
#include <Storages/DeltaMerge/Index/LocalIndex_fwd.h>

namespace DB::DM
{

class ColumnFileTinyInvertedIndexReader
{
private:
    const ColumnFileTiny & tiny_file;
    const IColumnFileDataProviderPtr data_provider;

    const ColumnValueSetPtr column_value_set;
    // Global local index cache
    const LocalIndexCachePtr local_index_cache;
    LoggerPtr log;

    // Performance statistics
    struct PerfStat
    {
        double duration_search = 0.0;
        size_t returned_rows = 0;
        // Whether the vector index is loaded from cache.
        bool load_from_cache = true;
    };
    PerfStat perf_stat;

    // Whether the vector index and search results are loaded.
    bool loaded = false;

public:
    ColumnFileTinyInvertedIndexReader(
        const ColumnFileTiny & tiny_file_,
        const IColumnFileDataProviderPtr & data_provider_,
        const ColumnValueSetPtr & column_value_set_,
        const LocalIndexCachePtr & local_index_cache_)
        : tiny_file(tiny_file_)
        , data_provider(data_provider_)
        , column_value_set(column_value_set_)
        , local_index_cache(local_index_cache_)
        , log(Logger::get())
    {}

    ~ColumnFileTinyInvertedIndexReader();

    // Load vector index and search results.
    BitmapFilterPtr load();

private:
    InvertedIndexViewerPtr loadInvertedIndex(const ColumnValueSetPtr & set);
    BitmapFilterPtr loadSearchResult(
        const InvertedIndexViewerPtr & inverted_index,
        const ColumnValueSetPtr & set,
        size_t size);
};

using ColumnFileTinyInvertedIndexReaderPtr = std::shared_ptr<ColumnFileTinyInvertedIndexReader>;

} // namespace DB::DM
