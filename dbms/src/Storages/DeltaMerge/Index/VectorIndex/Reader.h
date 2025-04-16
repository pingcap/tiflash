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

#include <IO/Buffer/ReadBuffer.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterView.h>
#include <Storages/DeltaMerge/Index/ICacheableLocalIndexReader.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf_fwd.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader_fwd.h>
#include <Storages/DeltaMerge/dtpb/dmfile.pb.h>
#include <Storages/DeltaMerge/dtpb/index_file.pb.h>
#include <VectorSearch/USearch.h>

namespace DB::DM
{

/// Views a VectorIndex file.
/// It may nor may not read the whole content of the file into memory.
class VectorIndexReader : public ICacheableLocalIndexReader
{
protected:
    using USearchImplType = unum::usearch::index_dense_gt</* key_at */ UInt32, /* compressed_slot_at */ UInt32>;

public:
    /// The key is the row's offset in the DMFile.
    using Key = UInt32;

    using SearchResults = USearchImplType::search_result_t;

    /// True bit means the row is valid and should be kept in the search result.
    /// False bit lets the row filtered out and will search for more results.
    using RowFilter = BitmapFilterView;

public:
    static VectorIndexReaderPtr createFromMmap(
        const dtpb::IndexFilePropsV2Vector & file_props,
        const VectorIndexPerfPtr & perf, // must not be null
        std::string_view path);
    static VectorIndexReaderPtr createFromMemory(
        const dtpb::IndexFilePropsV2Vector & file_props,
        const VectorIndexPerfPtr & perf, // must not be null
        ReadBuffer & buf);

public:
    explicit VectorIndexReader(
        bool is_in_memory_,
        const dtpb::IndexFilePropsV2Vector & file_props_,
        const VectorIndexPerfPtr & perf_ // must not be null
    );

    ~VectorIndexReader() override;

    /// The result is sorted by distance.
    /// WARNING: Due to usearch's impl, invalid rows in `valid_rows` may be still contained in the search result.
    /// WARNING: Drop the result as soon as possible, because it is "reader local", blocks more concurrent reads.
    /// We choose to return search result directly without any copying to improve performance.
    SearchResults search(const ANNQueryInfoPtr & query_info, const RowFilter & valid_rows) const;

    // Get the value (i.e. vector content) of a Key.
    void get(Key key, std::vector<Float32> & out) const;

public:
    const bool is_in_memory;
    const dtpb::IndexFilePropsV2Vector file_props;

private:
    USearchImplType index;

    const VectorIndexPerfPtr perf;
    size_t last_reported_memory_usage = 0;
};

} // namespace DB::DM
