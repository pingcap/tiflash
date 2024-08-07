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

#include <Columns/ColumnArray.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/Index/VectorIndexHNSW/Index.h>

#include <algorithm>

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int INCORRECT_QUERY;
extern const int CANNOT_ALLOCATE_MEMORY;
} // namespace DB::ErrorCodes

namespace DB::DM
{

template <unum::usearch::metric_kind_t Metric>
USearchIndexWithSerialization<Metric>::USearchIndexWithSerialization(size_t dimensions)
    : Base(Base::make(unum::usearch::metric_punned_t(dimensions, Metric)))
{}

template <unum::usearch::metric_kind_t Metric>
void USearchIndexWithSerialization<Metric>::serialize(WriteBuffer & ostr) const
{
    auto callback = [&ostr](void * from, size_t n) {
        ostr.write(reinterpret_cast<const char *>(from), n);
        return true;
    };
    Base::save_to_stream(callback);
}

template <unum::usearch::metric_kind_t Metric>
void USearchIndexWithSerialization<Metric>::deserialize(ReadBuffer & istr)
{
    auto callback = [&istr](void * from, size_t n) {
        istr.readStrict(reinterpret_cast<char *>(from), n);
        return true;
    };
    Base::load_from_stream(callback);
}

template class USearchIndexWithSerialization<unum::usearch::metric_kind_t::l2sq_k>;
template class USearchIndexWithSerialization<unum::usearch::metric_kind_t::cos_k>;

constexpr TiDB::DistanceMetric toTiDBDistanceMetric(unum::usearch::metric_kind_t metric)
{
    switch (metric)
    {
    case unum::usearch::metric_kind_t::l2sq_k:
        return TiDB::DistanceMetric::L2;
    case unum::usearch::metric_kind_t::cos_k:
        return TiDB::DistanceMetric::COSINE;
    default:
        return TiDB::DistanceMetric::INVALID;
    }
}

constexpr tipb::ANNQueryDistanceMetric toTiDBQueryDistanceMetric(unum::usearch::metric_kind_t metric)
{
    switch (metric)
    {
    case unum::usearch::metric_kind_t::l2sq_k:
        return tipb::ANNQueryDistanceMetric::L2;
    case unum::usearch::metric_kind_t::cos_k:
        return tipb::ANNQueryDistanceMetric::Cosine;
    default:
        return tipb::ANNQueryDistanceMetric::InvalidMetric;
    }
}

template <unum::usearch::metric_kind_t Metric>
VectorIndexHNSW<Metric>::VectorIndexHNSW(UInt32 dimensions_)
    : VectorIndex(TiDB::VectorIndexKind::HNSW, toTiDBDistanceMetric(Metric))
    , dimensions(dimensions_)
    , index(std::make_shared<USearchIndexWithSerialization<Metric>>(static_cast<size_t>(dimensions_)))
{}

template <unum::usearch::metric_kind_t Metric>
void VectorIndexHNSW<Metric>::addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark)
{
    // Note: column may be nullable.
    const ColumnArray * col_array;
    if (column.isColumnNullable())
        col_array = checkAndGetNestedColumn<ColumnArray>(&column);
    else
        col_array = checkAndGetColumn<ColumnArray>(&column);

    RUNTIME_CHECK(col_array != nullptr, column.getFamilyName());
    RUNTIME_CHECK(checkAndGetColumn<ColumnVector<Float32>>(col_array->getDataPtr().get()) != nullptr);

    const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());

    if (!index->reserve(unum::usearch::ceil2(index->size() + column.size())))
    {
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for HNSW index");
    }

    for (int i = 0, i_max = col_array->size(); i < i_max; ++i)
    {
        auto row_offset = added_rows;
        added_rows++;

        // Ignore rows with del_mark, as the column values are not meaningful.
        if (del_mark_data != nullptr && (*del_mark_data)[i])
            continue;

        // Ignore NULL values, as they are not meaningful to store in index.
        if (column.isNullAt(i))
            continue;

        // Expect all data to have matching dimensions.
        RUNTIME_CHECK(col_array->sizeAt(i) == dimensions);

        auto data = col_array->getDataAt(i);
        RUNTIME_CHECK(data.size == dimensions * sizeof(Float32));

        if (auto rc = index->add(row_offset, reinterpret_cast<const Float32 *>(data.data)); !rc)
            throw Exception(ErrorCodes::INCORRECT_DATA, rc.error.release());
    }
}

template <unum::usearch::metric_kind_t Metric>
void VectorIndexHNSW<Metric>::serializeBinary(WriteBuffer & ostr) const
{
    writeStringBinary(magic_enum::enum_name(kind), ostr);
    writeStringBinary(magic_enum::enum_name(distance_metric), ostr);
    writeIntBinary<UInt32>(dimensions, ostr);
    index->serialize(ostr);
}

template <unum::usearch::metric_kind_t Metric>
VectorIndexPtr VectorIndexHNSW<Metric>::deserializeBinary(ReadBuffer & istr)
{
    String kind;
    readStringBinary(kind, istr);
    RUNTIME_CHECK(magic_enum::enum_cast<TiDB::VectorIndexKind>(kind) == TiDB::VectorIndexKind::HNSW);

    String distance_metric;
    readStringBinary(distance_metric, istr);
    RUNTIME_CHECK(magic_enum::enum_cast<TiDB::DistanceMetric>(distance_metric) == toTiDBDistanceMetric(Metric));

    UInt32 dimensions;
    readIntBinary(dimensions, istr);

    auto vi = std::make_shared<VectorIndexHNSW<Metric>>(dimensions);
    vi->index->deserialize(istr);
    return vi;
}

template <unum::usearch::metric_kind_t Metric>
std::vector<VectorIndex::Key> VectorIndexHNSW<Metric>::search(
    const ANNQueryInfoPtr & queryInfo,
    const RowFilter & valid_rows,
    SearchStatistics & statistics) const
{
    RUNTIME_CHECK(queryInfo->ref_vec_f32().size() >= sizeof(UInt32));
    auto query_vec_size = readLittleEndian<UInt32>(queryInfo->ref_vec_f32().data());
    if (query_vec_size != dimensions)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Query vector size {} does not match index dimensions {}",
            query_vec_size,
            dimensions);

    RUNTIME_CHECK(queryInfo->ref_vec_f32().size() >= sizeof(UInt32) + query_vec_size * sizeof(Float32));

    if (queryInfo->distance_metric() != toTiDBQueryDistanceMetric(Metric))
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Query distance metric {} does not match index distance metric {}",
            tipb::ANNQueryDistanceMetric_Name(queryInfo->distance_metric()),
            tipb::ANNQueryDistanceMetric_Name(toTiDBQueryDistanceMetric(Metric)));

    RUNTIME_CHECK(index != nullptr);

    auto predicate
        = [&valid_rows, &statistics](typename USearchIndexWithSerialization<Metric>::member_cref_t const & member) {
              statistics.visited_nodes++;
              if (!valid_rows[member.key])
                  statistics.discarded_nodes++;
              return valid_rows[member.key];
          };

    // TODO: Support efSearch.
    auto result = index->search( //
        reinterpret_cast<const Float32 *>(queryInfo->ref_vec_f32().data() + sizeof(UInt32)),
        queryInfo->top_k(),
        predicate);
    std::vector<Key> keys(result.size());
    result.dump_to(keys.data());

    // For some reason usearch does not always do the predicate for all search results.
    // So we need to filter again.
    keys.erase(
        std::remove_if(keys.begin(), keys.end(), [&valid_rows](Key key) { return !valid_rows[key]; }),
        keys.end());

    return keys;
}

template <unum::usearch::metric_kind_t Metric>
void VectorIndexHNSW<Metric>::get(Key key, std::vector<Float32> & out) const
{
    out.resize(dimensions);
    index->get(key, out.data());
}

template class VectorIndexHNSW<unum::usearch::metric_kind_t::l2sq_k>;
template class VectorIndexHNSW<unum::usearch::metric_kind_t::cos_k>;

} // namespace DB::DM
