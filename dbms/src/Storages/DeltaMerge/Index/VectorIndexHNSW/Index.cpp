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
#include <Storages/DeltaMerge/Index/VectorIndexHNSW/Index.h>
#include <Storages/DeltaMerge/Index/VectorSearchPerf.h>
#include <Storages/DeltaMerge/dtpb/dmfile.pb.h>
#include <tipb/executor.pb.h>

#include <ext/scope_guard.h>

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int INCORRECT_QUERY;
extern const int ABORTED;
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
    RUNTIME_CHECK(definition_->kind == kind());
    GET_METRIC(tiflash_vector_index_active_instances, type_build).Increment();
}

void VectorIndexHNSWBuilder::addBlock(
    const IColumn & column,
    const ColumnVector<UInt8> * del_mark,
    ProceedCheckFn should_proceed)
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

    index.reserve(unum::usearch::ceil2(index.size() + column.size()));

    Stopwatch w;
    SCOPE_EXIT({ total_duration += w.elapsedSeconds(); });

    Stopwatch w_proceed_check(CLOCK_MONOTONIC_COARSE);

    for (int i = 0, i_max = col_array->size(); i < i_max; ++i)
    {
        auto row_offset = added_rows;
        added_rows++;

        if (unlikely(i % 100 == 0 && w_proceed_check.elapsedSeconds() > 0.5))
        {
            // The check of should_proceed could be non-trivial, so do it not too often.
            w_proceed_check.restart();
            if (!should_proceed())
                throw Exception(ErrorCodes::ABORTED, "Index build is interrupted");
        }

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
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Failed to add vector to HNSW index, i={} row_offset={} error={}",
                i,
                row_offset,
                rc.error.release());
    }

    auto current_memory_usage = index.memory_usage();
    auto delta = static_cast<Int64>(current_memory_usage) - static_cast<Int64>(last_reported_memory_usage);
    GET_METRIC(tiflash_vector_index_memory_usage, type_build).Increment(static_cast<double>(delta));
    last_reported_memory_usage = current_memory_usage;
}

void VectorIndexHNSWBuilder::saveToFile(std::string_view path) const
{
    Stopwatch w;
    SCOPE_EXIT({ total_duration += w.elapsedSeconds(); });

    auto result = index.save(unum::usearch::output_file_t(path.data()));
    RUNTIME_CHECK_MSG(result, "Failed to save vector index: {} path={}", result.error.what(), path);
}

void VectorIndexHNSWBuilder::saveToBuffer(WriteBuffer & write_buf) const
{
    Stopwatch w;
    SCOPE_EXIT({ total_duration += w.elapsedSeconds(); });

    auto result = index.save_to_stream([&](void const * buffer, std::size_t length) {
        write_buf.write(reinterpret_cast<const char *>(buffer), length);
        return true;
    });
    RUNTIME_CHECK_MSG(result, "Failed to save vector index: {}", result.error.what());
}

VectorIndexHNSWBuilder::~VectorIndexHNSWBuilder()
{
    GET_METRIC(tiflash_vector_index_duration, type_build).Observe(total_duration);
    GET_METRIC(tiflash_vector_index_memory_usage, type_build)
        .Decrement(static_cast<double>(last_reported_memory_usage));
    GET_METRIC(tiflash_vector_index_active_instances, type_build).Decrement();
}

tipb::VectorIndexKind VectorIndexHNSWBuilder::kind()
{
    return tipb::VectorIndexKind::HNSW;
}

VectorIndexViewerPtr VectorIndexHNSWViewer::view(const dtpb::IndexFilePropsV2Vector & file_props, std::string_view path)
{
    tipb::VectorDistanceMetric metric;
    RUNTIME_CHECK(tipb::VectorDistanceMetric_Parse(file_props.distance_metric(), &metric));
    RUNTIME_CHECK(metric != tipb::VectorDistanceMetric::INVALID_DISTANCE_METRIC);

    Stopwatch w;
    SCOPE_EXIT({ GET_METRIC(tiflash_vector_index_duration, type_view).Observe(w.elapsedSeconds()); });

    auto vi = std::make_shared<VectorIndexHNSWViewer>(file_props);

    vi->index = USearchImplType::make(
        unum::usearch::metric_punned_t( //
            file_props.dimensions(),
            getUSearchMetricKind(metric)),
        unum::usearch::index_dense_config_t(
            unum::usearch::default_connectivity(),
            unum::usearch::default_expansion_add(),
            16 /* default is 64 */));

    // Currently may have a lot of threads querying concurrently
    auto limit = unum::usearch::index_limits_t(0, /* threads */ std::thread::hardware_concurrency() * 10);
    vi->index.reserve(limit);

    auto result = vi->index.view(unum::usearch::memory_mapped_file_t(path.data()));
    RUNTIME_CHECK_MSG(
        result,
        "Failed to load vector index: {} props={} path={}",
        result.error.what(),
        file_props.ShortDebugString(),
        path);

    auto current_memory_usage = vi->index.memory_usage();
    GET_METRIC(tiflash_vector_index_memory_usage, type_view).Increment(static_cast<double>(current_memory_usage));
    vi->last_reported_memory_usage = current_memory_usage;

    return vi;
}

VectorIndexViewerPtr VectorIndexHNSWViewer::load(const dtpb::IndexFilePropsV2Vector & file_props, ReadBuffer & buf)
{
    tipb::VectorDistanceMetric metric;
    RUNTIME_CHECK(tipb::VectorDistanceMetric_Parse(file_props.distance_metric(), &metric));
    RUNTIME_CHECK(metric != tipb::VectorDistanceMetric::INVALID_DISTANCE_METRIC);

    Stopwatch w;
    SCOPE_EXIT({ GET_METRIC(tiflash_vector_index_duration, type_view).Observe(w.elapsedSeconds()); });

    auto vi = std::make_shared<VectorIndexHNSWViewer>(file_props);

    vi->index = USearchImplType::make(
        unum::usearch::metric_punned_t( //
            file_props.dimensions(),
            getUSearchMetricKind(metric)),
        unum::usearch::index_dense_config_t(
            unum::usearch::default_connectivity(),
            unum::usearch::default_expansion_add(),
            16 /* default is 64 */));

    // Currently may have a lot of threads querying concurrently
    auto limit = unum::usearch::index_limits_t(0, /* threads */ std::thread::hardware_concurrency() * 10);
    vi->index.reserve(limit);

    auto result = vi->index.load_from_stream([&](void * buffer, std::size_t length) {
        return buf.read(reinterpret_cast<char *>(buffer), length) == length;
    });
    RUNTIME_CHECK_MSG(result, "Failed to load vector index: {}", result.error.what());

    auto current_memory_usage = vi->index.memory_usage();
    GET_METRIC(tiflash_vector_index_memory_usage, type_view).Increment(static_cast<double>(current_memory_usage));
    vi->last_reported_memory_usage = current_memory_usage;

    return vi;
}

auto VectorIndexHNSWViewer::searchImpl(const ANNQueryInfoPtr & query_info, const RowFilter & valid_rows) const
{
    RUNTIME_CHECK(query_info->ref_vec_f32().size() >= sizeof(UInt32));
    auto query_vec_size = readLittleEndian<UInt32>(query_info->ref_vec_f32().data());
    if (query_vec_size != file_props.dimensions())
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Query vector size {} does not match index dimensions {}, index_id={} column_id={}",
            query_vec_size,
            file_props.dimensions(),
            query_info->index_id(),
            query_info->column_id());

    RUNTIME_CHECK(query_info->ref_vec_f32().size() == sizeof(UInt32) + query_vec_size * sizeof(Float32));

    if (tipb::VectorDistanceMetric_Name(query_info->distance_metric()) != file_props.distance_metric())
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Query distance metric {} does not match index distance metric {}, index_id={} column_id={}",
            tipb::VectorDistanceMetric_Name(query_info->distance_metric()),
            file_props.distance_metric(),
            query_info->index_id(),
            query_info->column_id());

    std::atomic<size_t> visited_nodes = 0;
    std::atomic<size_t> discarded_nodes = 0;
    std::atomic<bool> has_exception_in_search = false;

    // The non-valid rows should be discarded by this lambda.
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

    // TODO(vector-index): Support efSearch.
    auto result = index.filtered_search( //
        reinterpret_cast<const Float32 *>(query_info->ref_vec_f32().data() + sizeof(UInt32)),
        query_info->top_k(),
        predicate);

    if (has_exception_in_search)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Exception happened occurred during search");

    PerfContext::vector_search.visited_nodes += visited_nodes;
    PerfContext::vector_search.discarded_nodes += discarded_nodes;
    return result;
}

std::vector<VectorIndexViewer::SearchResult> VectorIndexHNSWViewer::search(
    const ANNQueryInfoPtr & query_info,
    const RowFilter & valid_rows) const
{
    auto result = searchImpl(query_info, valid_rows);

    // For some reason usearch does not always do the predicate for all search results.
    // So we need to filter again.
    const size_t result_size = result.size();
    std::vector<SearchResult> search_results;
    search_results.reserve(result_size);
    for (size_t i = 0; i < result_size; ++i)
    {
        const auto rowid = result[i].member.key;
        if (valid_rows[rowid])
            search_results.emplace_back(rowid, result[i].distance);
    }
    return search_results;
}

size_t VectorIndexHNSWViewer::size() const
{
    return index.size();
}

void VectorIndexHNSWViewer::get(Key key, std::vector<Float32> & out) const
{
    out.resize(file_props.dimensions());
    index.get(key, out.data());
}

VectorIndexHNSWViewer::VectorIndexHNSWViewer(const dtpb::IndexFilePropsV2Vector & props)
    : VectorIndexViewer(props)
{
    GET_METRIC(tiflash_vector_index_active_instances, type_view).Increment();
}

VectorIndexHNSWViewer::~VectorIndexHNSWViewer()
{
    GET_METRIC(tiflash_vector_index_memory_usage, type_view).Decrement(static_cast<double>(last_reported_memory_usage));
    GET_METRIC(tiflash_vector_index_active_instances, type_view).Decrement();
}

tipb::VectorIndexKind VectorIndexHNSWViewer::kind()
{
    return tipb::VectorIndexKind::HNSW;
}

} // namespace DB::DM
