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

#include <Storages/DeltaMerge/File/DMFile_fwd.h>
#include <Storages/DeltaMerge/Filter/ColumnRange_fwd.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache_fwd.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilter.h>

namespace DB::DM
{

class InvertedIndexReaderFromDMFile
{
private:
    const DMFilePtr & dmfile;
    const ColumnRangePtr column_range;
    // Global local index cache
    const LocalIndexCachePtr local_index_cache;

    bool loaded = false;

public:
    InvertedIndexReaderFromDMFile(
        const ColumnRangePtr & column_range_,
        const DMFilePtr & dmfile_,
        const LocalIndexCachePtr & local_index_cache_);

    ~InvertedIndexReaderFromDMFile();

    // Load inverted index and search results.
    BitmapFilterPtr load();

private:
    BitmapFilterPtr load(const SingleColumnRangePtr & column_range);
};

using InvertedIndexReaderFromDMFilePtr = std::shared_ptr<InvertedIndexReaderFromDMFile>;

} // namespace DB::DM
