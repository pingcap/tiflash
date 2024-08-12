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

#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/LRUCache.h>
#include <DataTypes/IDataType.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBuffer.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterView.h>
#include <Storages/DeltaMerge/Index/VectorIndex_fwd.h>
#include <TiDB/Schema/VectorIndex.h>

namespace DB
{
namespace DM
{

class VectorIndex
{
public:
    /// The key is the row's offset in the DMFile.
    using Key = UInt32;

    /// True bit means the row is valid and should be kept in the search result.
    /// False bit lets the row filtered out and will search for more results.
    using RowFilter = BitmapFilterView;

    struct SearchStatistics
    {
        size_t visited_nodes = 0;
        size_t discarded_nodes = 0; // Rows filtered out by MVCC
    };

    static bool isSupportedType(const IDataType & type);

    static VectorIndexPtr create(const TiDB::VectorIndexInfo & index_info);

    static VectorIndexPtr load(TiDB::VectorIndexKind kind, TiDB::DistanceMetric distance_metric, ReadBuffer & istr);

    VectorIndex(TiDB::VectorIndexKind kind_, TiDB::DistanceMetric distance_metric_)
        : kind(kind_)
        , distance_metric(distance_metric_)
    {}

    virtual ~VectorIndex() = default;

    virtual void addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark) = 0;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;

    virtual size_t memoryUsage() const = 0;

    // Invalid rows in `valid_rows` will be discared when applying the search
    virtual std::vector<Key> search( //
        const ANNQueryInfoPtr & query_info,
        const RowFilter & valid_rows,
        SearchStatistics & statistics) const
        = 0;

    // Get the value (i.e. vector content) of a Key.
    virtual void get(Key key, std::vector<Float32> & out) const = 0;

public:
    const TiDB::VectorIndexKind kind;
    const TiDB::DistanceMetric distance_metric;
};

struct VectorIndexWeightFunction
{
    size_t operator()(const String &, const VectorIndex & index) const { return index.memoryUsage(); }
};

class VectorIndexCache : public LRUCache<String, VectorIndex, std::hash<String>, VectorIndexWeightFunction>
{
private:
    using Base = LRUCache<String, VectorIndex, std::hash<String>, VectorIndexWeightFunction>;

public:
    explicit VectorIndexCache(size_t max_size_in_bytes)
        : Base(max_size_in_bytes)
    {}

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        return result.first;
    }
};

} // namespace DM

} // namespace DB
