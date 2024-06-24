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
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/File/dtpb/dmfile.pb.h>
#include <Storages/DeltaMerge/Index/VectorIndexHNSW/Index.h>
#include <Storages/DeltaMerge/Index/VectorSearchPerf.h>
#include <tipb/executor.pb.h>

#include <algorithm>
#include <ext/scope_guard.h>
#include <usearch/index.hpp>
#include <usearch/index_plugins.hpp>

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int INCORRECT_QUERY;
extern const int CANNOT_ALLOCATE_MEMORY;
} // namespace DB::ErrorCodes

namespace DB::DM
{

unum::usearch::metric_kind_t getUSearchMetricKind(tipb::VectorDistanceMetric d)
{
    switch (d)
    {
    case tipb::VectorDistanceMetric::INNER_PRODUCT:
        return unum::usearch::metric_kind_t::ip_k;
    case tipb::VectorDistanceMetric::COSINE:
        return unum::usearch::metric_kind_t::cos_k;
    case tipb::VectorDistanceMetric::L2:
        return unum::usearch::metric_kind_t::l2sq_k;
    default:
        // Specifically, L1 is currently unsupported by usearch.

        RUNTIME_CHECK_MSG( //
            false,
            "Unsupported vector distance {}",
            tipb::VectorDistanceMetric_Name(d));
    }
}

VectorIndexHNSWBuilder::VectorIndexHNSWBuilder(const TiDB::VectorIndexDefinitionPtr & definition_)
    : VectorIndexBuilder(definition_)
    , index(USearchImplType::make(unum::usearch::metric_punned_t( //
          definition_->dimension,
          getUSearchMetricKind(definition->distance_metric))))
{
    RUNTIME_CHECK(definition_->kind == tipb::VectorIndexKind::HNSW);
    GET_METRIC(tiflash_vector_index_active_instances, type_build).Increment();
}

void VectorIndexHNSWBuilder::addBlock(const IColumn & column, const ColumnVector<UInt8> * del_mark)
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

    if (!index.reserve(unum::usearch::ceil2(index.size() + column.size())))
    {
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Could not reserve memory for HNSW index");
    }

    Stopwatch w;
    SCOPE_EXIT({ total_duration += w.elapsedSeconds(); });

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
        RUNTIME_CHECK(col_array->sizeAt(i) == definition->dimension);

        auto data = col_array->getDataAt(i);
        RUNTIME_CHECK(data.size == definition->dimension * sizeof(Float32));

        if (auto rc = index.add(row_offset, reinterpret_cast<const Float32 *>(data.data)); !rc)
            throw Exception(ErrorCodes::INCORRECT_DATA, rc.error.release());
    }

    auto current_memory_usage = index.memory_usage();
    auto delta = static_cast<Int64>(current_memory_usage) - static_cast<Int64>(last_reported_memory_usage);
    GET_METRIC(tiflash_vector_index_memory_usage, type_build).Increment(static_cast<double>(delta));
    last_reported_memory_usage = current_memory_usage;
}

void VectorIndexHNSWBuilder::save(std::string_view path) const
{
    Stopwatch w;
    SCOPE_EXIT({ total_duration += w.elapsedSeconds(); });

    auto result = index.save(unum::usearch::output_file_t(path.data()));
    RUNTIME_CHECK_MSG(result, "Failed to save vector index: {}", result.error.what());
}

VectorIndexHNSWBuilder::~VectorIndexHNSWBuilder()
{
    GET_METRIC(tiflash_vector_index_duration, type_build).Observe(total_duration);
    GET_METRIC(tiflash_vector_index_memory_usage, type_build)
        .Decrement(static_cast<double>(last_reported_memory_usage));
    GET_METRIC(tiflash_vector_index_active_instances, type_build).Decrement();
}

VectorIndexViewerPtr VectorIndexHNSWViewer::view(const dtpb::VectorIndexFileProps & file_props, std::string_view path)
{
    RUNTIME_CHECK(file_props.index_kind() == tipb::VectorIndexKind_Name(tipb::VectorIndexKind::HNSW));

    tipb::VectorDistanceMetric metric;
    RUNTIME_CHECK(tipb::VectorDistanceMetric_Parse(file_props.distance_metric(), &metric));
    RUNTIME_CHECK(metric != tipb::VectorDistanceMetric::INVALID_DISTANCE_METRIC);

    Stopwatch w;
    SCOPE_EXIT({ GET_METRIC(tiflash_vector_index_duration, type_view).Observe(w.elapsedSeconds()); });

    auto vi = std::make_shared<VectorIndexHNSWViewer>(file_props);
    vi->index = USearchImplType::make(unum::usearch::metric_punned_t( //
        file_props.dimensions(),
        getUSearchMetricKind(metric)));
    auto result = vi->index.view(unum::usearch::memory_mapped_file_t(path.data()));
    RUNTIME_CHECK_MSG(result, "Failed to load vector index: {}", result.error.what());

    auto current_memory_usage = vi->index.memory_usage();
    GET_METRIC(tiflash_vector_index_memory_usage, type_view).Increment(static_cast<double>(current_memory_usage));
    vi->last_reported_memory_usage = current_memory_usage;

    return vi;
}

std::vector<VectorIndexBuilder::Key> VectorIndexHNSWViewer::search(
    const ANNQueryInfoPtr & queryInfo,
    const RowFilter & valid_rows) const
{
    RUNTIME_CHECK(queryInfo->ref_vec_f32().size() >= sizeof(UInt32));
    auto query_vec_size = readLittleEndian<UInt32>(queryInfo->ref_vec_f32().data());
    if (query_vec_size != file_props.dimensions())
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Query vector size {} does not match index dimensions {}",
            query_vec_size,
            file_props.dimensions());

    RUNTIME_CHECK(queryInfo->ref_vec_f32().size() >= sizeof(UInt32) + query_vec_size * sizeof(Float32));

    if (tipb::VectorDistanceMetric_Name(queryInfo->distance_metric()) != file_props.distance_metric())
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Query distance metric {} does not match index distance metric {}",
            tipb::VectorDistanceMetric_Name(queryInfo->distance_metric()),
            file_props.distance_metric());

    std::atomic<size_t> visited_nodes = 0;
    std::atomic<size_t> discarded_nodes = 0;
    std::atomic<bool> has_exception_in_search = false;

    auto predicate = [&](const Key & key) {
        // Must catch exceptions in the predicate, because search runs on other threads.
        try
        {
            // Note: We don't increase the thread_local perf, because search runs on other threads.
            visited_nodes++;
            if (!valid_rows[key])
                discarded_nodes++;
            return valid_rows[key];
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            has_exception_in_search = true;
            return false;
        }
    };

    Stopwatch w;
    SCOPE_EXIT({ GET_METRIC(tiflash_vector_index_duration, type_search).Observe(w.elapsedSeconds()); });

    // TODO: Support efSearch.
    auto result = index.filtered_search( //
        reinterpret_cast<const Float32 *>(queryInfo->ref_vec_f32().data() + sizeof(UInt32)),
        queryInfo->top_k(),
        predicate);

    if (has_exception_in_search)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Exception happened occurred during search");

    std::vector<Key> keys(result.size());
    result.dump_to(keys.data());

    PerfContext::vector_search.visited_nodes += visited_nodes;
    PerfContext::vector_search.discarded_nodes += discarded_nodes;

    // For some reason usearch does not always do the predicate for all search results.
    // So we need to filter again.
    keys.erase(
        std::remove_if(keys.begin(), keys.end(), [&valid_rows](Key key) { return !valid_rows[key]; }),
        keys.end());

    return keys;
}

void VectorIndexHNSWViewer::get(Key key, std::vector<Float32> & out) const
{
    out.resize(file_props.dimensions());
    index.get(key, out.data());
}

VectorIndexHNSWViewer::VectorIndexHNSWViewer(const dtpb::VectorIndexFileProps & props)
    : VectorIndexViewer(props)
{
    GET_METRIC(tiflash_vector_index_active_instances, type_view).Increment();
}

VectorIndexHNSWViewer::~VectorIndexHNSWViewer()
{
    GET_METRIC(tiflash_vector_index_memory_usage, type_view).Decrement(static_cast<double>(last_reported_memory_usage));
    GET_METRIC(tiflash_vector_index_active_instances, type_view).Decrement();
}

} // namespace DB::DM
