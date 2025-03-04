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

#include <Columns/ColumnArray.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/DeltaMerge/Index/VectorIndex/CommonUtil.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Writer.h>
#include <TiDB/Schema/VectorIndex.h>


namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int INCORRECT_QUERY;
extern const int ABORTED;
} // namespace DB::ErrorCodes

namespace DB::DM
{

std::shared_ptr<VectorIndexWriterInternal> VectorIndexWriterInternal::createInMemory(
    IndexID index_id,
    const TiDB::VectorIndexDefinitionPtr & definition)
{
    return std::make_shared<VectorIndexWriterInMemory>(index_id, definition);
}

std::shared_ptr<VectorIndexWriterInternal> VectorIndexWriterInternal::createOnDisk(
    std::string_view index_file,
    IndexID index_id,
    const TiDB::VectorIndexDefinitionPtr & definition)
{
    return std::make_shared<VectorIndexWriterOnDisk>(index_file, index_id, definition);
}

VectorIndexWriterInternal::VectorIndexWriterInternal(
    IndexID index_id,
    const TiDB::VectorIndexDefinitionPtr & definition_)
    : LocalIndexWriter(index_id)
    , definition(definition_)
{
    RUNTIME_CHECK(definition != nullptr);
    RUNTIME_CHECK(definition->kind == tipb::VectorIndexKind::HNSW);
    RUNTIME_CHECK(definition->dimension > 0);
    RUNTIME_CHECK(definition->dimension <= TiDB::MAX_VECTOR_DIMENSION);

    index = USearchImplType::make(unum::usearch::metric_punned_t( //
        definition->dimension,
        getUSearchMetricKind(definition->distance_metric)));

    GET_METRIC(tiflash_vector_index_active_instances, type_build).Increment();
}

void VectorIndexWriterInternal::addBlock(
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
        RUNTIME_CHECK(col_array->sizeAt(i) == definition->dimension, col_array->sizeAt(i), definition->dimension);

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

void VectorIndexWriterInternal::saveToFile(std::string_view path) const
{
    Stopwatch w;
    SCOPE_EXIT({ total_duration += w.elapsedSeconds(); });

    auto result = index.save(unum::usearch::output_file_t(path.data()));
    RUNTIME_CHECK_MSG(result, "Failed to save vector index: {} path={}", result.error.what(), path);
}

void VectorIndexWriterInternal::saveToBuffer(WriteBuffer & write_buf) const
{
    Stopwatch w;
    SCOPE_EXIT({ total_duration += w.elapsedSeconds(); });

    auto result = index.save_to_stream([&](void const * buffer, std::size_t length) {
        write_buf.write(reinterpret_cast<const char *>(buffer), length);
        return true;
    });
    RUNTIME_CHECK_MSG(result, "Failed to save vector index: {}", result.error.what());
}

VectorIndexWriterInternal::~VectorIndexWriterInternal()
{
    GET_METRIC(tiflash_vector_index_duration, type_build).Observe(total_duration);
    GET_METRIC(tiflash_vector_index_memory_usage, type_build)
        .Decrement(static_cast<double>(last_reported_memory_usage));
    GET_METRIC(tiflash_vector_index_active_instances, type_build).Decrement();
}

void VectorIndexWriterInternal::saveFilePros(dtpb::IndexFilePropsV2 * pb_idx) const
{
    auto * pb_vec_idx = pb_idx->mutable_vector_index();
    pb_vec_idx->set_format_version(0);
    pb_vec_idx->set_dimensions(definition->dimension);
    pb_vec_idx->set_distance_metric(tipb::VectorDistanceMetric_Name(definition->distance_metric));
}

} // namespace DB::DM
