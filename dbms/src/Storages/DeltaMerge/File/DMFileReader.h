// Copyright 2023 PingCAP, Inc.
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

#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/ColumnCacheLongTerm_fwd.h>
#include <Storages/DeltaMerge/File/ColumnStream.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/Filter/RSOperator_fwd.h>
#include <Storages/DeltaMerge/ReadMode.h>
#include <Storages/DeltaMerge/ReadThread/DMFileReaderPool.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <Storages/MarkCache.h>

namespace DB::DM
{

class DMFileWithVectorIndexBlockInputStream;


class DMFileReader
{
    friend class DMFileWithVectorIndexBlockInputStream;
    friend class DMFileReaderPoolSharding;

public:
    static bool isCacheableColumn(const ColumnDefine & cd);

    DMFileReader(
        const DMFilePtr & dmfile_,
        const ColumnDefines & read_columns_,
        bool is_common_handle_,
        // Only set this param to true when
        // 1. There is no delta.
        // 2. You don't need pk, version and delete_tag columns
        // If you have no idea what it means, then simply set it to false.
        bool enable_handle_clean_read_,
        bool enable_del_clean_read_,
        bool is_fast_scan_,
        // The the MVCC filter version. Used by clean read check.
        UInt64 max_read_version_,
        // filters
        DMFilePackFilter && pack_filter_,
        // caches
        const MarkCachePtr & mark_cache_,
        bool enable_column_cache_,
        const ColumnCachePtr & column_cache_,
        size_t max_read_buffer_size,
        const FileProviderPtr & file_provider_,
        const ReadLimiterPtr & read_limiter,
        size_t rows_threshold_per_read_,
        bool read_one_pack_every_time_,
        const String & tracing_id_,
        size_t max_sharing_column_bytes_,
        const ScanContextPtr & scan_context_,
        ReadTag read_tag_);

    Block getHeader() const { return toEmptyBlock(read_columns); }

    /// Skipped rows before next call of #read().
    /// Return false if it is the end of stream.
    bool getSkippedRows(size_t & skip_rows);

    /// NOTE: skipNextBlock and readWithFilter are only used by late materialization.

    /// Skip the packs to read next
    /// Return the number of rows skipped.
    /// Return 0 if it is the end of file.
    size_t skipNextBlock();

    /// Read specified rows.
    Block readWithFilter(const IColumn::Filter & filter);

    Block read();
    std::string path() const
    {
        // Status of DMFile can be updated when DMFileReader in used and the pathname will be changed.
        // For DMFileReader, always use the readable path.
        return getPathByStatus(dmfile->parentPath(), dmfile->fileId(), DMFileStatus::READABLE);
    }

    friend class MarkLoader;
    friend class ColumnReadStream;
    friend class tests::DMFileMetaV2Test;

private:
    ColumnPtr readExtraColumn(
        const ColumnDefine & cd,
        size_t start_pack_id,
        size_t pack_count,
        size_t read_rows,
        const std::vector<size_t> & clean_read_packs);
    ColumnPtr readFromDisk(
        const ColumnDefine & cd,
        const DataTypePtr & type_on_disk,
        size_t start_pack_id,
        size_t read_rows);
    ColumnPtr readFromDiskOrSharingCache(
        const ColumnDefine & cd,
        const DataTypePtr & type_on_disk,
        size_t start_pack_id,
        size_t pack_count,
        size_t read_rows);
    ColumnPtr readColumn(const ColumnDefine & cd, size_t start_pack_id, size_t pack_count, size_t read_rows);
    ColumnPtr cleanRead(
        const ColumnDefine & cd,
        size_t rows_count,
        std::pair<size_t, size_t> range,
        const DMFileMeta::PackStats & pack_stats);

    void addColumnToCache(
        const ColumnCachePtr & data_cache,
        ColId col_id,
        size_t start_pack_id,
        size_t pack_count,
        ColumnPtr & col);
    ColumnPtr getColumnFromCache(
        const ColumnCachePtr & data_cache,
        const ColumnDefine & cd,
        const DataTypePtr & type_on_disk,
        size_t start_pack_id,
        size_t pack_count,
        size_t read_rows,
        std::function<ColumnPtr(const ColumnDefine &, const DataTypePtr &, size_t, size_t, size_t)> on_cache_miss);

    void addScannedRows(UInt64 rows);
    void addSkippedRows(UInt64 rows);

    void initReadBlockInfos();
    // Will add some new read info to read_block_infos
    // Used by readWithFilter
    // Return the number of new added read infos
    size_t updateReadBlockInfos(const IColumn::Filter & filter, size_t pack_start, size_t pack_end);

    DMFilePtr dmfile;
    ColumnDefines read_columns;
    ColumnReadStreamMap column_streams;

    const bool is_common_handle;

    // read_one_pack_every_time is used to create info for every pack
    const bool read_one_pack_every_time;

    /// Clean read optimize
    // In normal mode, if there is no delta for some packs in stable, we can try to do clean read (enable_handle_clean_read is true).
    // In fast mode, if we don't need handle column, we will try to do clean read on handle_column(enable_handle_clean_read is true).
    //               if we don't need del column, we will try to do clean read on del_column(enable_del_clean_read is true).
    const bool enable_handle_clean_read;
    const bool enable_del_clean_read;
    const bool is_fast_scan;

    const bool enable_column_cache;

    const UInt64 max_read_version;

private:
    /// Filters
    DMFilePackFilter pack_filter;

    /// Caches
    MarkCachePtr mark_cache;
    ColumnCachePtr column_cache;

    const ScanContextPtr scan_context;
    const ReadTag read_tag;

    const size_t rows_threshold_per_read;
    const size_t max_sharing_column_bytes;

    FileProviderPtr file_provider;

    LoggerPtr log;

    // DataSharing
    ColumnCachePtr data_sharing_col_data_cache;

    // <start_pack_id, pack_count, RSResult, read_rows>
    std::deque<std::tuple<size_t, size_t, RSResult, size_t>> read_block_infos;
    // <pack_id, offset>
    std::unordered_map<size_t, size_t> pack_id_to_offset;
    // last read pack_id + 1, used by getSkippedRows
    size_t next_pack_id = 0;

public:
    void setColumnCacheLongTerm(ColumnCacheLongTermPtr column_cache_long_term_, ColumnID pk_col_id_)
    {
        column_cache_long_term = column_cache_long_term_;
        pk_col_id = pk_col_id_;
    }

private:
    ColumnCacheLongTermPtr column_cache_long_term = nullptr;
    ColumnID pk_col_id = 0;
};

} // namespace DB::DM
