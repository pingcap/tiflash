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

#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Filter/ColumnRange_fwd.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache_fwd.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>

namespace DB::DM
{

class InvertedIndexReaderFromColumnFileTiny
{
private:
    const ColumnFileTiny & tiny_file;
    const IColumnFileDataProviderPtr data_provider;
    const ColumnRangePtr column_range;
    // Global local index cache
    const LocalIndexCachePtr local_index_cache;
    const ScanContextPtr scan_context;

    bool loaded = false;

public:
    InvertedIndexReaderFromColumnFileTiny(
        const ColumnRangePtr & column_range_,
        const ColumnFileTiny & tiny_file_,
        const IColumnFileDataProviderPtr & data_provider_,
        const LocalIndexCachePtr & local_index_cache_,
        const ScanContextPtr & scan_context_ = nullptr);

    ~InvertedIndexReaderFromColumnFileTiny();

    // Load inverted index and search results.
    BitmapFilterPtr load();

private:
    BitmapFilterPtr load(const SingleColumnRangePtr & column_range);
};

using InvertedIndexReaderFromColumnFileTinyPtr = std::shared_ptr<InvertedIndexReaderFromColumnFileTiny>;

} // namespace DB::DM
