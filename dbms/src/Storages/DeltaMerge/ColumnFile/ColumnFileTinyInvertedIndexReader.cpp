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
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyInvertedIndexReader.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache.h>

namespace DB::DM
{

BitmapFilterPtr ColumnFileTinyInvertedIndexReader::load()
{
    if (loaded)
        return nullptr;

    Stopwatch watch;

    auto sorted_results = column_value_set->check(
        [&](const ColumnValueSetPtr & set, size_t size) {
            auto inverted_index = loadInvertedIndex(set);
            if (!inverted_index)
                return BitmapFilterPtr();
            return loadSearchResult(inverted_index, set, size);
        },
        tiny_file.getRows());

    if (sorted_results)
        perf_stat.returned_rows = sorted_results->count();
    loaded = true;
    return sorted_results;
}

InvertedIndexViewerPtr ColumnFileTinyInvertedIndexReader::loadInvertedIndex(const ColumnValueSetPtr & set)
{
    auto single_column_value_set = std::dynamic_pointer_cast<SingleColumnValueSet>(set);
    RUNTIME_CHECK(single_column_value_set != nullptr);
    const auto & index_infos = tiny_file.index_infos;
    if (!index_infos || index_infos->empty())
        return nullptr;
    auto index_id = single_column_value_set->index_id;
    const auto info_iter = std::find_if(index_infos->cbegin(), index_infos->cend(), [index_id](const auto & info) {
        return info.index_props().index_id() == index_id;
    });
    if (info_iter == index_infos->cend() || info_iter->index_props().kind() != dtpb::IndexFileKind::INVERTED_INDEX)
        return nullptr;
    const auto & inverted_index = info_iter->index_props();
    auto index_page_id = info_iter->index_page_id();
    auto load_from_page_storage = [&]() {
        perf_stat.load_from_cache = false;
        std::vector<size_t> index_fields = {0};
        auto index_page = data_provider->readTinyData(index_page_id, index_fields);
        ReadBufferFromOwnString read_buf(index_page.data);
        CompressedReadBuffer compressed(read_buf);
        auto type_id = tiny_file.getDataType(single_column_value_set->column_id)->getTypeId();
        return InvertedIndexViewer::view(type_id, compressed, inverted_index.inverted_index().uncompressed_size());
    };
    if (local_index_cache)
    {
        const auto key = fmt::format("{}{}", LocalIndexCache::COLUMNFILETINY_INDEX_NAME_PREFIX, index_page_id);
        auto local_index = local_index_cache->getOrSet(key, load_from_page_storage);
        return std::dynamic_pointer_cast<InvertedIndexViewer>(local_index);
    }
    else
        return load_from_page_storage();
}

ColumnFileTinyInvertedIndexReader::~ColumnFileTinyInvertedIndexReader()
{
    LOG_DEBUG(
        log,
        "Finish search inverted index over column tiny_{}(rows={}){} cached, returned {} rows, search={:.3f}s",
        tiny_file.getDataPageId(),
        tiny_file.getRows(),
        perf_stat.load_from_cache ? "" : " not",
        perf_stat.returned_rows,
        perf_stat.duration_search);
}

BitmapFilterPtr ColumnFileTinyInvertedIndexReader::loadSearchResult(
    const InvertedIndexViewerPtr & inverted_index,
    const ColumnValueSetPtr & set,
    size_t size)
{
    Stopwatch watch;

    auto single_column_value_set = std::dynamic_pointer_cast<SingleColumnValueSet>(set);
    RUNTIME_CHECK(single_column_value_set != nullptr);

    auto search_results = single_column_value_set->set->search(inverted_index, size);
    perf_stat.duration_search += watch.elapsedSeconds();
    return search_results;
}

} // namespace DB::DM
