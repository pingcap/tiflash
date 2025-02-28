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
    using Distance = Float32;

    struct SearchResult
    {
        Key key;
        Distance distance;
    };

    /// True bit means the row is valid and should be kept in the search result.
    /// False bit lets the row filtered out and will search for more results.
    using RowFilter = BitmapFilterView;

public:
    static VectorIndexReaderPtr createFromMmap(const dtpb::IndexFilePropsV2Vector & file_props, std::string_view path);
    static VectorIndexReaderPtr createFromMemory(const dtpb::IndexFilePropsV2Vector & file_props, ReadBuffer & buf);

public:
    explicit VectorIndexReader(const dtpb::IndexFilePropsV2Vector & file_props_);

    ~VectorIndexReader() override;

    // Invalid rows in `valid_rows` will be discared when applying the search
    std::vector<SearchResult> search(const ANNQueryInfoPtr & query_info, const RowFilter & valid_rows) const;

    size_t size() const;

    // Get the value (i.e. vector content) of a Key.
    void get(Key key, std::vector<Float32> & out) const;

private:
    auto searchImpl(const ANNQueryInfoPtr & query_info, const RowFilter & valid_rows) const;

public:
    const dtpb::IndexFilePropsV2Vector file_props;

private:
    USearchImplType index;

    size_t last_reported_memory_usage = 0;
};

} // namespace DB::DM
