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

#pragma once

#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/VectorColumnFromIndexReader.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/DeltaMerge/Index/VectorIndex.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>


namespace DB::DM
{

/**
 * @brief DMFileWithVectorIndexBlockInputStream is similar to DMFileBlockInputStream.
 * However it can read data efficiently with the help of vector index.
 *
 * General steps:
 * 1. Read all PK, Version and Del Marks (respecting Pack filters).
 * 2. Construct a bitmap of valid rows (in memory). This bitmap guides the reading of vector index to determine whether a row is valid or not.
 *
 *  Note: Step 1 and 2 simply rely on the BitmapFilter to avoid repeat IOs.
 *  BitmapFilter is global, which provides row valid info for all DMFile + Delta.
 *  What we need is which rows are valid in THIS DMFile.
 *  To transform a global BitmapFilter result into a local one, RowOffsetTracker is used.
 *
 * 3. Perform a vector search for Top K vector rows. We now have K row_ids whose vector distance is close.
 * 4. Map these row_ids to packids as the new pack filter.
 * 5. Read from other columns with the new pack filter.
 *     For each read, join other columns and vector column together.
 *
 *  Step 3~4 is performed lazily at first read.
 *
 * Before constructing this class, the caller must ensure that vector index
 * exists on the corresponding column. If the index does not exist, the caller
 * should use the standard DMFileBlockInputStream.
 */
class DMFileWithVectorIndexBlockInputStream : public SkippableBlockInputStream
{
public:
    static DMFileWithVectorIndexBlockInputStreamPtr create(
        const ANNQueryInfoPtr & ann_query_info,
        const DMFilePtr & dmfile,
        Block && header_layout,
        DMFileReader && reader,
        ColumnDefine && vec_cd,
        const FileProviderPtr & file_provider,
        const ReadLimiterPtr & read_limiter,
        const ScanContextPtr & scan_context,
        const VectorIndexCachePtr & vec_index_cache,
        const BitmapFilterView & valid_rows,
        const String & tracing_id)
    {
        return std::make_shared<DMFileWithVectorIndexBlockInputStream>(
            ann_query_info,
            dmfile,
            std::move(header_layout),
            std::move(reader),
            std::move(vec_cd),
            file_provider,
            read_limiter,
            scan_context,
            vec_index_cache,
            valid_rows,
            tracing_id);
    }

    explicit DMFileWithVectorIndexBlockInputStream(
        const ANNQueryInfoPtr & ann_query_info_,
        const DMFilePtr & dmfile_,
        Block && header_layout_,
        DMFileReader && reader_,
        ColumnDefine && vec_cd_,
        const FileProviderPtr & file_provider_,
        const ReadLimiterPtr & read_limiter_,
        const ScanContextPtr & scan_context_,
        const VectorIndexCachePtr & vec_index_cache_,
        const BitmapFilterView & valid_rows_,
        const String & tracing_id)
        : log(Logger::get(tracing_id))
        , ann_query_info(ann_query_info_)
        , dmfile(dmfile_)
        , header_layout(std::move(header_layout_))
        , reader(std::move(reader_))
        , vec_cd(std::move(vec_cd_))
        , file_provider(file_provider_)
        , read_limiter(read_limiter_)
        , scan_context(scan_context_)
        , vec_index_cache(vec_index_cache_)
        , valid_rows(valid_rows_)
    {
        RUNTIME_CHECK(ann_query_info);
        RUNTIME_CHECK(vec_cd.id == ann_query_info->column_id());
        for (const auto & cd : reader.read_columns)
        {
            RUNTIME_CHECK(header_layout.has(cd.name), cd.name);
            RUNTIME_CHECK(cd.id != vec_cd.id);
        }
        RUNTIME_CHECK(header_layout.has(vec_cd.name));
        RUNTIME_CHECK(header_layout.columns() == reader.read_columns.size() + 1);

        // Fill start_offset_to_pack_id
        const auto & pack_stats = dmfile->getPackStats();
        start_offset_to_pack_id.reserve(pack_stats.size());
        UInt32 start_offset = 0;
        for (size_t pack_id = 0, pack_id_max = pack_stats.size(); pack_id < pack_id_max; pack_id++)
        {
            start_offset_to_pack_id[start_offset] = pack_id;
            start_offset += pack_stats[pack_id].rows;
        }

        // Fill header
        header = toEmptyBlock(reader.read_columns);
        addColumnToBlock(
            header,
            vec_cd.id,
            vec_cd.name,
            vec_cd.type,
            vec_cd.type->createColumn(),
            vec_cd.default_value);
    }

    ~DMFileWithVectorIndexBlockInputStream() override
    {
        if (!vec_column_reader)
            return;

        scan_context->total_vector_idx_read_vec_time_ms
            += static_cast<UInt64>(duration_read_from_vec_index_seconds * 1000);
        scan_context->total_vector_idx_read_others_time_ms
            += static_cast<UInt64>(duration_read_from_other_columns_seconds * 1000);

        LOG_DEBUG( //
            log,
            "Finished read DMFile with vector index for column dmf_{}/{}(id={}), "
            "query_top_k={} load_index+result={:.2f}s read_from_index={:.2f}s read_from_others={:.2f}s",
            dmfile->fileId(),
            vec_cd.name,
            vec_cd.id,
            ann_query_info->top_k(),
            duration_load_vec_index_and_results_seconds,
            duration_read_from_vec_index_seconds,
            duration_read_from_other_columns_seconds);
    }

public:
    Block read() override
    {
        FilterPtr filter = nullptr;
        return read(filter, false);
    }

    // When all rows in block are not filtered out,
    // `res_filter` will be set to null.
    // The caller needs to do handle this situation.
    Block read(FilterPtr & res_filter, bool return_filter) override
    {
        if (return_filter)
            return readImpl(res_filter);

        // If return_filter == false, we must filter by ourselves.

        FilterPtr filter = nullptr;
        auto res = readImpl(filter);
        if (filter != nullptr)
        {
            for (auto & col : res)
                col.column = col.column->filter(*filter, -1);
        }
        // filter == nullptr means all rows are valid and no need to filter.

        return res;
    }

    // When all rows in block are not filtered out,
    // `res_filter` will be set to null.
    // The caller needs to do handle this situation.
    Block readImpl(FilterPtr & res_filter)
    {
        load();

        Block res;
        if (!reader.read_columns.empty())
            res = readByFollowingOtherColumns();
        else
            res = readByIndexReader();

        // Filter the output rows. If no rows need to filter, res_filter is nullptr.
        filter.resize(res.rows());
        bool all_match = valid_rows_after_search.get(filter, res.startOffset(), res.rows());

        if unlikely (all_match)
            res_filter = nullptr;
        else
            res_filter = &filter;
        return res;
    }

    bool getSkippedRows(size_t &) override
    {
        RUNTIME_CHECK_MSG(false, "DMFileWithVectorIndexBlockInputStream does not support getSkippedRows");
    }

    size_t skipNextBlock() override
    {
        RUNTIME_CHECK_MSG(false, "DMFileWithVectorIndexBlockInputStream does not support skipNextBlock");
    }

    Block readWithFilter(const IColumn::Filter &) override
    {
        // We don't support the normal late materialization, because
        // we are already doing it.
        RUNTIME_CHECK_MSG(false, "DMFileWithVectorIndexBlockInputStream does not support late materialization");
    }

    String getName() const override { return "DMFileWithVectorIndex"; }

    Block getHeader() const override { return header; }

private:
    // Only used in readByIndexReader()
    size_t index_reader_next_pack_id = 0;
    // Only used in readByIndexReader()
    size_t index_reader_next_row_id = 0;

    // Read data totally from the VectorColumnFromIndexReader. This is used
    // when there is no other column to read.
    Block readByIndexReader()
    {
        const auto & pack_stats = dmfile->getPackStats();
        size_t all_packs = pack_stats.size();
        const auto & use_packs = reader.pack_filter.getPackResConst();

        RUNTIME_CHECK(use_packs.size() == all_packs);

        // Skip as many packs as possible according to Pack Filter
        while (index_reader_next_pack_id < all_packs)
        {
            if (use_packs[index_reader_next_pack_id] != RSResult::None)
                break;
            index_reader_next_row_id += pack_stats[index_reader_next_pack_id].rows;
            index_reader_next_pack_id++;
        }

        if (index_reader_next_pack_id >= all_packs)
            // Finished
            return {};

        auto read_pack_id = index_reader_next_pack_id;
        auto block_start_row_id = index_reader_next_row_id;
        auto read_rows = pack_stats[read_pack_id].rows;

        index_reader_next_pack_id++;
        index_reader_next_row_id += read_rows;

        Block block;
        block.setStartOffset(block_start_row_id);

        auto vec_column = vec_cd.type->createColumn();

        Stopwatch w;
        vec_column_reader->read(vec_column, read_pack_id, read_rows);
        duration_read_from_vec_index_seconds += w.elapsedSeconds();

        block.insert(ColumnWithTypeAndName{//
                                           std::move(vec_column),
                                           vec_cd.type,
                                           vec_cd.name,
                                           vec_cd.id});

        return block;
    }

    // Read data from other columns first, then read from VectorColumnFromIndexReader. This is used
    // when there are other columns to read.
    Block readByFollowingOtherColumns()
    {
        // First read other columns.
        Stopwatch w;
        auto block_others = reader.read();
        duration_read_from_other_columns_seconds += w.elapsedSeconds();

        if (!block_others)
            return {};

        // Using vec_cd.type to construct a Column directly instead of using
        // the type from dmfile, so that we don't need extra transforms
        // (e.g. wrap with a Nullable). vec_column_reader is compatible with
        // both Nullable and NotNullable.
        auto vec_column = vec_cd.type->createColumn();

        // Then read from vector index for the same pack.
        w.restart();

        vec_column_reader->read(vec_column, getPackIdFromBlock(block_others), block_others.rows());
        duration_read_from_vec_index_seconds += w.elapsedSeconds();

        // Re-assemble block using the same layout as header_layout.
        Block res = header_layout.cloneEmpty();
        // Note: the start offset counts from the beginning of THIS dmfile. It
        // is not a global offset.
        res.setStartOffset(block_others.startOffset());
        for (const auto & elem : block_others)
        {
            RUNTIME_CHECK(res.has(elem.name));
            res.getByName(elem.name).column = std::move(elem.column);
        }
        RUNTIME_CHECK(res.has(vec_cd.name));
        res.getByName(vec_cd.name).column = std::move(vec_column);

        return res;
    }

private:
    void load()
    {
        if (loaded)
            return;

        Stopwatch w;

        loadVectorIndex();
        loadVectorSearchResult();

        duration_load_vec_index_and_results_seconds = w.elapsedSeconds();

        loaded = true;
    }

    void loadVectorIndex()
    {
        bool is_index_load_from_cache = true;

        auto col_id = ann_query_info->column_id();

        RUNTIME_CHECK(dmfile->useMetaV2()); // v3

        // Check vector index exists on the column
        const auto & column_stat = dmfile->getColumnStat(col_id);
        RUNTIME_CHECK(column_stat.index_bytes > 0);

        const auto & type = column_stat.type;
        RUNTIME_CHECK(VectorIndex::isSupportedType(*type));
        RUNTIME_CHECK(column_stat.vector_index.has_value());

        const auto file_name_base = DMFile::getFileNameBase(col_id);
        auto load_vector_index = [&]() {
            is_index_load_from_cache = false;

            auto index_guard = S3::S3RandomAccessFile::setReadFileInfo(
                {dmfile->getReadFileSize(col_id, colIndexFileName(file_name_base)), scan_context});

            auto * dmfile_meta = typeid_cast<DMFileMetaV2 *>(dmfile->meta.get());
            assert(dmfile_meta != nullptr);

            auto info = dmfile_meta->merged_sub_file_infos.find(colIndexFileName(file_name_base));
            if (info == dmfile_meta->merged_sub_file_infos.end())
            {
                throw Exception(
                    fmt::format("Unknown index file {}", dmfile->colIndexPath(file_name_base)),
                    ErrorCodes::LOGICAL_ERROR);
            }

            auto file_path = dmfile_meta->mergedPath(info->second.number);
            auto encryp_path = dmfile_meta->encryptionMergedPath(info->second.number);
            auto offset = info->second.offset;
            auto data_size = info->second.size;

            auto buffer = ReadBufferFromRandomAccessFileBuilder::build(
                file_provider,
                file_path,
                encryp_path,
                dmfile->getConfiguration()->getChecksumFrameLength(),
                read_limiter);
            buffer.seek(offset);

            // TODO(vector-index): Read from file directly?
            String raw_data;
            raw_data.resize(data_size);
            buffer.read(reinterpret_cast<char *>(raw_data.data()), data_size);

            auto buf = ChecksumReadBufferBuilder::build(
                std::move(raw_data),
                dmfile->colDataPath(file_name_base),
                dmfile->getConfiguration()->getChecksumFrameLength(),
                dmfile->getConfiguration()->getChecksumAlgorithm(),
                dmfile->getConfiguration()->getChecksumFrameLength());

            auto index_kind = magic_enum::enum_cast<TiDB::VectorIndexKind>(column_stat.vector_index->index_kind());
            RUNTIME_CHECK(index_kind.has_value());
            RUNTIME_CHECK(index_kind.value() != TiDB::VectorIndexKind::INVALID);

            auto index_distance_metric
                = magic_enum::enum_cast<TiDB::DistanceMetric>(column_stat.vector_index->distance_metric());
            RUNTIME_CHECK(index_distance_metric.has_value());
            RUNTIME_CHECK(index_distance_metric.value() != TiDB::DistanceMetric::INVALID);

            auto index = VectorIndex::load(index_kind.value(), index_distance_metric.value(), *buf);
            return index;
        };

        Stopwatch watch;

        if (vec_index_cache)
        {
            // TODO(vector-index): Is cache key valid on Compute Node for different Write Nodes?
            vec_index = vec_index_cache->getOrSet(dmfile->colIndexCacheKey(file_name_base), load_vector_index);
        }
        else
        {
            // try load from the cache first
            if (vec_index_cache)
                vec_index = vec_index_cache->get(dmfile->colIndexCacheKey(file_name_base));
            if (vec_index == nullptr)
                vec_index = load_vector_index();
        }

        double duration_load_index = watch.elapsedSeconds();
        RUNTIME_CHECK(vec_index != nullptr);
        scan_context->total_vector_idx_load_time_ms += static_cast<UInt64>(duration_load_index * 1000);
        if (is_index_load_from_cache)
            scan_context->total_vector_idx_load_from_cache++;
        else
            scan_context->total_vector_idx_load_from_disk++;

        LOG_DEBUG( //
            log,
            "Loaded vector index for column dmf_{}/{}(id={}), index_size={} kind={} cost={:.2f}s from_cache={}",
            dmfile->fileId(),
            vec_cd.name,
            vec_cd.id,
            column_stat.index_bytes,
            column_stat.vector_index->index_kind(),
            duration_load_index,
            is_index_load_from_cache);
    }

    void loadVectorSearchResult()
    {
        Stopwatch watch;

        VectorIndex::SearchStatistics statistics;
        auto results_rowid = vec_index->search(ann_query_info, valid_rows, statistics);

        double search_duration = watch.elapsedSeconds();
        scan_context->total_vector_idx_search_time_ms += static_cast<UInt64>(search_duration * 1000);
        scan_context->total_vector_idx_search_discarded_nodes += statistics.discarded_nodes;
        scan_context->total_vector_idx_search_visited_nodes += statistics.visited_nodes;

        size_t rows_after_mvcc = valid_rows.count();
        size_t rows_after_vector_search = results_rowid.size();

        // After searching with the BitmapFilter, we create a bitmap
        // to exclude rows that are not in the search result, because these rows
        // are produced as [] or NULL, which is not a valid vector for future use.
        // The bitmap will be used when returning the output to the caller.
        {
            valid_rows_after_search = BitmapFilter(valid_rows.size(), false);
            for (auto rowid : results_rowid)
                valid_rows_after_search.set(rowid, 1, true);
            valid_rows_after_search.runOptimize();
        }

        vec_column_reader = std::make_shared<VectorColumnFromIndexReader>( //
            dmfile,
            vec_index,
            std::move(results_rowid));

        // Vector index is very likely to filter out some packs. For example,
        // if we query for Top 1, then only 1 pack will be remained. So we
        // update the pack filter used by the DMFileReader to avoid reading
        // unnecessary data for other columns.
        size_t valid_packs_before_search = 0;
        size_t valid_packs_after_search = 0;
        const auto & pack_stats = dmfile->getPackStats();
        auto & packs_res = reader.pack_filter.getPackRes();

        size_t results_it = 0;
        const size_t results_it_max = results_rowid.size();

        UInt32 pack_start = 0;

        for (size_t pack_id = 0, pack_id_max = dmfile->getPacks(); pack_id < pack_id_max; pack_id++)
        {
            if (packs_res[pack_id] != RSResult::None)
                valid_packs_before_search++;

            bool pack_has_result = false;

            UInt32 pack_end = pack_start + pack_stats[pack_id].rows;
            while (results_it < results_it_max //
                   && results_rowid[results_it] >= pack_start //
                   && results_rowid[results_it] < pack_end)
            {
                pack_has_result = true;
                results_it++;
            }

            if (!pack_has_result)
                packs_res[pack_id] = RSResult::None;

            if (packs_res[pack_id] != RSResult::None)
                valid_packs_after_search++;

            pack_start = pack_end;
        }

        RUNTIME_CHECK_MSG(results_it == results_it_max, "All packs has been visited but not all results are consumed");

        LOG_DEBUG( //
            log,
            "Finished vector search over column dmf_{}/{}(id={}), cost={:.2f}s "
            "top_k_[query/visited/discarded/result]={}/{}/{}/{} "
            "rows_[file/after_mvcc/after_search]={}/{}/{} "
            "pack_[total/before_search/after_search]={}/{}/{}",

            dmfile->fileId(),
            vec_cd.name,
            vec_cd.id,
            search_duration,

            ann_query_info->top_k(),
            statistics.visited_nodes, // Visited nodes will be larger than query_top_k when there are MVCC rows
            statistics.discarded_nodes, // How many nodes are skipped by MVCC
            results_rowid.size(),

            dmfile->getRows(),
            rows_after_mvcc,
            rows_after_vector_search,

            pack_stats.size(),
            valid_packs_before_search,
            valid_packs_after_search);
    }

    inline UInt32 getPackIdFromBlock(const Block & block)
    {
        // The start offset of a block is ensured to be aligned with the pack.
        // This is how we know which pack the block comes from.
        auto start_offset = block.startOffset();
        auto it = start_offset_to_pack_id.find(start_offset);
        RUNTIME_CHECK(it != start_offset_to_pack_id.end());
        return it->second;
    }

private:
    const LoggerPtr log;

    const ANNQueryInfoPtr ann_query_info;
    const DMFilePtr dmfile;

    // The header_layout should contain columns from reader and vec_cd
    Block header_layout;
    // Vector column should be excluded in the reader
    DMFileReader reader;
    // Note: ColumnDefine comes from read path does not have vector_index fields.
    const ColumnDefine vec_cd;
    const FileProviderPtr file_provider;
    const ReadLimiterPtr read_limiter;
    const ScanContextPtr scan_context;
    const VectorIndexCachePtr vec_index_cache;
    const BitmapFilterView valid_rows; // TODO(vector-index): Currently this does not support ColumnFileBig

    Block header; // Filled in constructor;

    std::unordered_map<UInt32, UInt32> start_offset_to_pack_id; // Filled from reader in constructor

    // Set after load().
    VectorIndexPtr vec_index = nullptr;
    // Set after load().
    VectorColumnFromIndexReaderPtr vec_column_reader = nullptr;
    // Set after load(). Used to filter the output rows.
    BitmapFilter valid_rows_after_search{0, false};
    IColumn::Filter filter;

    bool loaded = false;

    double duration_load_vec_index_and_results_seconds = 0;
    double duration_read_from_vec_index_seconds = 0;
    double duration_read_from_other_columns_seconds = 0;
};

} // namespace DB::DM
