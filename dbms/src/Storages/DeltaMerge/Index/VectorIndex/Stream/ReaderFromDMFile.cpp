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
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/ReaderFromDMFile.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/FileCachePerf.h>

namespace DB::ErrorCodes
{
extern const int S3_ERROR;
} // namespace DB::ErrorCodes

namespace DB::DM
{

VectorIndexReaderPtr VectorIndexReaderFromDMFile::load(const VectorIndexStreamCtxPtr & ctx, const DMFilePtr & dmfile)
{
    Stopwatch w(CLOCK_MONOTONIC_COARSE);

    const auto col_id = ctx->ann_query_info->deprecated_column_id();
    const auto index_id = ctx->ann_query_info->index_id();

    RUNTIME_CHECK(dmfile->useMetaV2()); // v3

    // Check vector index exists on the column
    auto vector_index = dmfile->getLocalIndex(col_id, index_id);
    RUNTIME_CHECK(vector_index.has_value(), col_id, index_id);
    RUNTIME_CHECK(vector_index->index_props().kind() == dtpb::IndexFileKind::VECTOR_INDEX);
    RUNTIME_CHECK(vector_index->index_props().has_vector_index());

    bool has_s3_download = false;
    bool has_load_from_file = false;

    // If local file is invalidated, cache is not valid anymore. So we
    // need to ensure file exists on local fs first.
    const auto index_file_path = index_id > 0 //
        ? dmfile->localIndexPath(index_id, TiDB::ColumnarIndexKind::Vector) //
        : dmfile->colIndexPath(DMFile::getFileNameBase(col_id));
    String local_index_file_path;
    if (auto s3_file_name = S3::S3FilenameView::fromKeyWithPrefix(index_file_path); s3_file_name.isValid())
    {
        // Disaggregated mode
        auto * file_cache = FileCache::instance();
        RUNTIME_CHECK_MSG(file_cache, "Must enable S3 file cache to use vector index");
        if (auto [file_seg, downloaded] = file_cache->downloadFileForLocalReadWithRetry( //
                s3_file_name,
                vector_index->index_props().file_size(),
                3);
            file_seg)
        {
            local_index_file_path = file_seg->getLocalFileName();
            has_s3_download = downloaded;
        }
        else
        {
            throw Exception(ErrorCodes::S3_ERROR, "Failed to download vector index file {}", index_file_path);
        }
    }
    else
    {
        // Not disaggregated mode
        local_index_file_path = index_file_path;
    }

    auto load_from_file = [&]() {
        has_load_from_file = true;
        return VectorIndexReader::createFromMmap(
            vector_index->index_props().vector_index(),
            ctx->perf,
            local_index_file_path);
    };

    VectorIndexReaderPtr vec_index = nullptr;
    // DMFile vector index uses mmap to read data, does not directly occupy memory, so use the light cache.
    if (ctx->index_cache_light)
    {
        // Note: must use local_index_file_path as the cache key, because cache
        // will check whether file is still valid and try to remove memory references
        // when file is dropped.
        auto local_index = ctx->index_cache_light->getOrSet(local_index_file_path, load_from_file);
        vec_index = std::dynamic_pointer_cast<VectorIndexReader>(local_index);
    }
    else
        vec_index = load_from_file();

    RUNTIME_CHECK(vec_index != nullptr);

    { // Statistics
        double elapsed = w.elapsedSeconds();
        if (has_s3_download)
        {
            // it could be possible that s3=true but load_from_file=false, it means we download a file
            // and then reuse the memory cache. The majority time comes from s3 download
            // so we still count it as s3 download.
            ctx->perf->load_from_stable_s3 += 1;
            GET_METRIC(tiflash_vector_index_duration, type_load_dmfile_s3).Observe(elapsed);
        }
        else if (has_load_from_file)
        {
            ctx->perf->load_from_stable_disk += 1;
            GET_METRIC(tiflash_vector_index_duration, type_load_dmfile_local).Observe(elapsed);
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
