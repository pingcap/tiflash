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

#include <Common/config.h>

#if ENABLE_CLARA
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <IO/copyData.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/ReaderFromColumnFileTiny.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache.h>

namespace DB::DM
{

FullTextIndexReaderPtr FullTextIndexReaderFromColumnFileTiny::load(
    const FullTextIndexStreamCtxPtr & ctx,
    const ColumnFileTiny & tiny_file)
{
    Stopwatch w(CLOCK_MONOTONIC_COARSE);

    auto const * index_info = tiny_file.findIndexInfo(ctx->fts_query_info->index_id());
    RUNTIME_CHECK(index_info != nullptr);
    RUNTIME_CHECK(index_info->index_props().kind() == dtpb::IndexFileKind::FULLTEXT_INDEX);
    RUNTIME_CHECK(index_info->index_props().has_fulltext_index());

    auto index_page_id = index_info->index_page_id();

    bool is_load_from_storage = false;
    auto load_from_page_storage = [&]() {
        is_load_from_storage = true;
        std::vector<size_t> index_fields = {0};
        auto index_page = ctx->data_provider->readTinyData(index_page_id, index_fields);
        ReadBufferFromOwnString reader(index_page.data);
        CompressedReadBuffer compressed_reader(reader);
        WriteBufferFromOwnString decompressed_buf;
        copyData(compressed_reader, decompressed_buf);
        return FullTextIndexReader::createFromMemory(rust::Slice( //
            reinterpret_cast<const UInt8 *>(decompressed_buf.str().data()),
            decompressed_buf.str().size()));
    };

    FullTextIndexReaderPtr idx_reader = nullptr;
    if (ctx->index_cache_heavy)
    {
        const auto key = fmt::format("{}{}", LocalIndexCache::COLUMNFILETINY_INDEX_NAME_PREFIX, index_page_id);
        auto local_index = ctx->index_cache_heavy->getOrSet(key, load_from_page_storage);
        idx_reader = std::dynamic_pointer_cast<FullTextIndexReader>(local_index);
    }
    else
        idx_reader = load_from_page_storage();

    RUNTIME_CHECK(idx_reader != nullptr);

    { // Statistics
        double elapsed = w.elapsedSeconds();
        if (is_load_from_storage)
        {
            ctx->perf->idx_load_from_column_file += 1;
            GET_METRIC(tiflash_fts_index_duration, type_load_cf).Observe(elapsed);
        }
        else
        {
            ctx->perf->idx_load_from_cache += 1;
            GET_METRIC(tiflash_fts_index_duration, type_load_cache).Observe(elapsed);
        }
        ctx->perf->idx_load_total_ms += elapsed * 1000;
    }

    return idx_reader;
}

} // namespace DB::DM
#endif
