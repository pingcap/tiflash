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

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/ReaderFromColumnFileTiny.h>

namespace DB::DM
{

VectorIndexReaderPtr VectorIndexReaderFromColumnFileTiny::load(
    const VectorIndexStreamCtxPtr & ctx,
    const ColumnFileTiny & tiny_file)
{
    Stopwatch w(CLOCK_MONOTONIC_COARSE);

    auto const * index_info = tiny_file.findIndexInfo(ctx->ann_query_info->index_id());
    RUNTIME_CHECK(index_info != nullptr);
    RUNTIME_CHECK(index_info->index_props().kind() == dtpb::IndexFileKind::VECTOR_INDEX);
    RUNTIME_CHECK(index_info->index_props().has_vector_index());

    auto index_page_id = index_info->index_page_id();

    bool is_load_from_storage = false;
    auto load_from_page_storage = [&]() {
        is_load_from_storage = true;
        std::vector<size_t> index_fields = {0};
        auto index_page = ctx->data_provider->readTinyData(index_page_id, index_fields);
        ReadBufferFromOwnString read_buf(index_page.data);
        CompressedReadBuffer compressed(read_buf);
        return VectorIndexReader::createFromMemory(index_info->index_props().vector_index(), ctx->perf, compressed);
    };

    VectorIndexReaderPtr vec_index = nullptr;
    // ColumnFile vector index stores all data in memory, can not be evicted by system, so use heavy cache.
    if (ctx->index_cache_heavy)
    {
        const auto key = fmt::format("{}{}", LocalIndexCache::COLUMNFILETINY_INDEX_NAME_PREFIX, index_page_id);
        auto local_index = ctx->index_cache_heavy->getOrSet(key, load_from_page_storage);
        vec_index = std::dynamic_pointer_cast<VectorIndexReader>(local_index);
    }
    else
        vec_index = load_from_page_storage();

    RUNTIME_CHECK(vec_index != nullptr);

    { // Statistics
        double elapsed = w.elapsedSeconds();
        if (is_load_from_storage)
        {
            ctx->perf->load_from_column_file += 1;
            GET_METRIC(tiflash_vector_index_duration, type_load_cf).Observe(elapsed);
        }
        else
        {
            ctx->perf->load_from_cache += 1;
            GET_METRIC(tiflash_vector_index_duration, type_load_cache).Observe(elapsed);
        }
        ctx->perf->total_load_ms += elapsed * 1000;
    }

    return vec_index;
}

} // namespace DB::DM
