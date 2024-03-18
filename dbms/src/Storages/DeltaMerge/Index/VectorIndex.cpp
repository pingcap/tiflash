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

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/DeltaMerge/Index/VectorIndexHNSW/Index.h>

namespace DB::ErrorCodes
{
extern const int INCORRECT_QUERY;
} // namespace DB::ErrorCodes

namespace DB::DM
{

bool VectorIndex::isSupportedType(const IDataType & type)
{
    const auto * nullable = checkAndGetDataType<DataTypeNullable>(&type);
    if (nullable)
        return checkDataTypeArray<DataTypeFloat32>(&*nullable->getNestedType());

    return checkDataTypeArray<DataTypeFloat32>(&type);
}

VectorIndexPtr VectorIndex::create(const TiDB::VectorIndexInfo & index_info)
{
    RUNTIME_CHECK(index_info.dimension > 0);
    RUNTIME_CHECK(index_info.dimension <= std::numeric_limits<UInt32>::max());

    switch (index_info.kind)
    {
    case TiDB::VectorIndexKind::HNSW:
        switch (index_info.distance_metric)
        {
        case TiDB::DistanceMetric::L2:
            return std::make_shared<VectorIndexHNSW<unum::usearch::metric_kind_t::l2sq_k>>(index_info.dimension);
        case TiDB::DistanceMetric::COSINE:
            return std::make_shared<VectorIndexHNSW<unum::usearch::metric_kind_t::cos_k>>(index_info.dimension);
        default:
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Unsupported vector index distance metric {}",
                index_info.distance_metric);
        }
    default:
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unsupported vector index {}", index_info.kind);
    }
}

VectorIndexPtr VectorIndex::load(TiDB::VectorIndexKind kind, TiDB::DistanceMetric distance_metric, ReadBuffer & istr)
{
    RUNTIME_CHECK(kind != TiDB::VectorIndexKind::INVALID);
    RUNTIME_CHECK(distance_metric != TiDB::DistanceMetric::INVALID);

    switch (kind)
    {
    case TiDB::VectorIndexKind::HNSW:
        switch (distance_metric)
        {
        case TiDB::DistanceMetric::L2:
            return VectorIndexHNSW<unum::usearch::metric_kind_t::l2sq_k>::deserializeBinary(istr);
        case TiDB::DistanceMetric::COSINE:
            return VectorIndexHNSW<unum::usearch::metric_kind_t::cos_k>::deserializeBinary(istr);
        default:
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Unsupported vector index distance metric {}",
                distance_metric);
        }
    default:
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Unsupported vector index {}", kind);
    }
}

} // namespace DB::DM
