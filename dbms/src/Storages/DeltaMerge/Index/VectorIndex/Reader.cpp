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

#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <IO/Endian.h>
#include <Storages/DeltaMerge/Index/VectorIndex/CommonUtil.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader.h>
#include <TiDB/Schema/VectorIndex.h>

namespace DB::ErrorCodes
{
extern const int INCORRECT_QUERY;
} // namespace DB::ErrorCodes

namespace DB::DM
{

VectorIndexReaderPtr VectorIndexReader::createFromMmap(
    const dtpb::IndexFilePropsV2Vector & file_props,
    const VectorIndexPerfPtr & perf,
    std::string_view path)
{
    tipb::VectorDistanceMetric metric;
    RUNTIME_CHECK(tipb::VectorDistanceMetric_Parse(file_props.distance_metric(), &metric));
    RUNTIME_CHECK(metric != tipb::VectorDistanceMetric::INVALID_DISTANCE_METRIC);

    auto vi = std::make_shared<VectorIndexReader>(file_props, perf);

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

VectorIndexReaderPtr VectorIndexReader::createFromMemory(
    const dtpb::IndexFilePropsV2Vector & file_props,
    const VectorIndexPerfPtr & perf,
    ReadBuffer & buf)
{
    tipb::VectorDistanceMetric metric;
    RUNTIME_CHECK(tipb::VectorDistanceMetric_Parse(file_props.distance_metric(), &metric));
    RUNTIME_CHECK(metric != tipb::VectorDistanceMetric::INVALID_DISTANCE_METRIC);

    auto vi = std::make_shared<VectorIndexReader>(file_props, perf);

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
    GET_METRIC(tiflash_vector_index_memory_usage, type_view)
        .Increment(static_cast<double>(current_memory_usage)); // FIXME, should be type_load
    vi->last_reported_memory_usage = current_memory_usage;

    return vi;
}

VectorIndexReader::SearchResults VectorIndexReader::search(
    const ANNQueryInfoPtr & query_info,
    const RowFilter & valid_rows) const
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
            visited_nodes.fetch_add(1, std::memory_order_relaxed);
            if (!valid_rows[key])
                discarded_nodes.fetch_add(1, std::memory_order_relaxed);
            return valid_rows[key];
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            has_exception_in_search = true;
            return false;
        }
    };

    Stopwatch w(CLOCK_MONOTONIC_COARSE);
    SCOPE_EXIT({
        double elapsed = w.elapsedSeconds();
        perf->n_searches += 1;
        perf->total_search_ms += elapsed * 1000;
        GET_METRIC(tiflash_vector_index_duration, type_search).Observe(w.elapsedSeconds());
    });

    // TODO(vector-index): Support efSearch.
    auto result = index.filtered_search( //
        reinterpret_cast<const Float32 *>(query_info->ref_vec_f32().data() + sizeof(UInt32)),
        query_info->top_k(),
        predicate);

    perf->visited_nodes += visited_nodes;
    perf->discarded_nodes += discarded_nodes;
    perf->returned_nodes += result.size();

    if (has_exception_in_search)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Exception happened occurred during search");

    if (result.error)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Search resulted in an error: {}", result.error.what());

    return result;
}

void VectorIndexReader::get(Key key, std::vector<Float32> & out) const
{
    out.resize(file_props.dimensions());
    index.get(key, out.data());
}

VectorIndexReader::VectorIndexReader(const dtpb::IndexFilePropsV2Vector & file_props_, const VectorIndexPerfPtr & perf_)
    : file_props(file_props_)
    , perf(perf_)
{
    RUNTIME_CHECK(perf_ != nullptr);
    RUNTIME_CHECK(file_props.dimensions() > 0);
    RUNTIME_CHECK(file_props.dimensions() <= TiDB::MAX_VECTOR_DIMENSION);

    GET_METRIC(tiflash_vector_index_active_instances, type_view).Increment();
}

VectorIndexReader::~VectorIndexReader()
{
    GET_METRIC(tiflash_vector_index_memory_usage, type_view).Decrement(static_cast<double>(last_reported_memory_usage));
    GET_METRIC(tiflash_vector_index_active_instances, type_view).Decrement();
}

} // namespace DB::DM
