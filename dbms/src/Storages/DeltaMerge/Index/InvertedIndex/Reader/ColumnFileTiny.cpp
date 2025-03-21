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
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache.h>

namespace DB::DM
{

InvertedIndexReaderFromColumnFileTiny::InvertedIndexReaderFromColumnFileTiny(
    const ColumnRangePtr & column_range_,
    const ColumnFileTiny & tiny_file_,
    const IColumnFileDataProviderPtr & data_provider_,
    const LocalIndexCachePtr & local_index_cache_)
    : tiny_file(tiny_file_)
    , data_provider(data_provider_)
    , column_range(column_range_)
    , local_index_cache(local_index_cache_)
{
    GET_METRIC(tiflash_inverted_index_active_instances, type_memory_reader).Increment();
}

BitmapFilterPtr InvertedIndexReaderFromColumnFileTiny::load()
{
    if (loaded)
        return nullptr;

    Stopwatch watch;

    auto sorted_results = column_range->check(
        [&](const SingleColumnRangePtr & column_range) { return load(column_range); },
        tiny_file.getRows());

    loaded = true;
    return sorted_results;
}

BitmapFilterPtr InvertedIndexReaderFromColumnFileTiny::load(const SingleColumnRangePtr & column_range)
{
    Stopwatch w(CLOCK_MONOTONIC_COARSE);

    const auto & index_infos = tiny_file.getIndexInfos();
    if (!index_infos || index_infos->empty())
        return nullptr;
    auto index_id = column_range->index_id;
    const auto info_iter = std::find_if(index_infos->cbegin(), index_infos->cend(), [index_id](const auto & info) {
        return info.index_props().index_id() == index_id;
    });
    if (info_iter == index_infos->cend() || info_iter->index_props().kind() != dtpb::IndexFileKind::INVERTED_INDEX)
        return nullptr;
    const auto & inverted_index = info_iter->index_props();
    auto index_page_id = info_iter->index_page_id();

    bool is_load_from_storage = false;
    auto load_from_page_storage = [&]() {
        is_load_from_storage = true;
        std::vector<size_t> index_fields = {0};
        auto index_page = data_provider->readTinyData(index_page_id, index_fields);
        ReadBufferFromOwnString read_buf(index_page.data);
        CompressedReadBuffer compressed(read_buf);
        const auto type = tiny_file.getDataType(column_range->column_id);
        return InvertedIndexReader::view(type, compressed, inverted_index.inverted_index().uncompressed_size());
    };

    InvertedIndexReaderPtr index_reader;
    if (local_index_cache)
    {
        const auto key = fmt::format("{}{}", LocalIndexCache::COLUMNFILETINY_INDEX_NAME_PREFIX, index_page_id);
        auto local_index = local_index_cache->getOrSet(key, load_from_page_storage);
        index_reader = std::dynamic_pointer_cast<InvertedIndexReader>(local_index);
    }
    else
        index_reader = load_from_page_storage();

    { // Statistics
        double elapsed = w.elapsedSecondsFromLastTime();
        if (is_load_from_storage)
        {
            // it could be possible that s3=true but load_from_file=false, it means we download a file
            // and then reuse the memory cache. The majority time comes from s3 download
            // so we still count it as s3 download.
            GET_METRIC(tiflash_inverted_index_duration, type_load_cf).Observe(elapsed);
        }
        else
        {
            GET_METRIC(tiflash_inverted_index_duration, type_load_cache).Observe(elapsed);
        }
    }

    RUNTIME_CHECK(index_reader != nullptr);
    auto bitmap_filter = column_range->set->search(index_reader, tiny_file.getRows());
    GET_METRIC(tiflash_inverted_index_duration, type_search).Observe(w.elapsedSecondsFromLastTime());
    return bitmap_filter;
}

InvertedIndexReaderFromColumnFileTiny::~InvertedIndexReaderFromColumnFileTiny()
{
    GET_METRIC(tiflash_inverted_index_active_instances, type_memory_reader).Decrement();
}

} // namespace DB::DM
