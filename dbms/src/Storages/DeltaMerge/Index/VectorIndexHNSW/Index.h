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
#include <Storages/DeltaMerge/Index/VectorIndexHNSW/usearch_index_dense.h>

namespace DB::DM
{

using USearchImplType
    = unum::usearch::index_dense_gt</* key_at */ VectorIndex::Key, /* compressed_slot_at */ VectorIndex::Key>;

template <unum::usearch::metric_kind_t Metric>
class USearchIndexWithSerialization : public USearchImplType
{
    using Base = USearchImplType;

public:
    explicit USearchIndexWithSerialization(size_t dimensions);
    void serialize(WriteBuffer & ostr) const;
    void deserialize(ReadBuffer & istr);
};

template <unum::usearch::metric_kind_t Metric>
using USearchIndexWithSerializationPtr = std::shared_ptr<USearchIndexWithSerialization<Metric>>;

template <unum::usearch::metric_kind_t Metric>
class VectorIndexHNSW : public VectorIndex
{
public:
    explicit VectorIndexHNSW(UInt32 dimensions_);

    void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark) override;

    void serializeBinary(WriteBuffer & ostr) const override;
    static VectorIndexPtr deserializeBinary(ReadBuffer & istr);

    size_t memoryUsage() const override { return index->memory_usage(); }

    std::vector<Key> search( //
        const ANNQueryInfoPtr & query_info,
        const RowFilter & valid_rows,
        SearchStatistics & statistics) const override;

    void get(Key key, std::vector<Float32> & out) const override;

private:
    const UInt32 dimensions;
    const USearchIndexWithSerializationPtr<Metric> index;

    UInt64 added_rows = 0; // Includes nulls and deletes. Used as the index key.
};

} // namespace DB::DM
