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

#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/DeltaMerge/dtpb/dmfile.pb.h>
#include <VectorSearch/USearch.h>

namespace DB::DM
{

using USearchImplType = unum::usearch::
    index_dense_gt</* key_at */ VectorIndexBuilder::Key, /* compressed_slot_at */ VectorIndexBuilder::Key>;

class VectorIndexHNSWBuilder : public VectorIndexBuilder
{
public:
    static tipb::VectorIndexKind kind();

    explicit VectorIndexHNSWBuilder(const TiDB::VectorIndexDefinitionPtr & definition_);

    ~VectorIndexHNSWBuilder() override;

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark, ProceedCheckFn should_proceed) override;

    void save(std::string_view path) const override;

private:
    USearchImplType index;
    UInt64 added_rows = 0; // Includes nulls and deletes. Used as the index key.

    mutable double total_duration = 0;
    size_t last_reported_memory_usage = 0;
};

class VectorIndexHNSWViewer : public VectorIndexViewer
{
public:
    static VectorIndexViewerPtr view(const dtpb::VectorIndexFileProps & props, std::string_view path);

    static tipb::VectorIndexKind kind();

    explicit VectorIndexHNSWViewer(const dtpb::VectorIndexFileProps & props);

    ~VectorIndexHNSWViewer() override;

    std::vector<Key> search(const ANNQueryInfoPtr & query_info, const RowFilter & valid_rows) const override;

    size_t size() const override;

    void get(Key key, std::vector<Float32> & out) const override;

private:
    USearchImplType index;

    size_t last_reported_memory_usage = 0;
};

} // namespace DB::DM
