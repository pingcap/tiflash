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

#include <DataStreams/MarkInCompressedFile.h>
#include <Encryption/CompressedReadBufferFromFileProvider.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/ReadThread/ColumnSharingCache.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/MarkCache.h>

namespace DB
{
namespace DM
{
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;

inline static const size_t DMFILE_READ_ROWS_THRESHOLD = DEFAULT_MERGE_BLOCK_SIZE * 3;

class DMFileReader
{
public:
    // Read stream for single column
    struct Stream
    {
        Stream(DMFileReader & reader,
               ColId col_id,
               const String & file_name_base,
               size_t aio_threshold,
               size_t max_read_buffer_size,
               const LoggerPtr & log,
               const ReadLimiterPtr & read_limiter);

        const bool single_file_mode;
        double avg_size_hint;
        MarksInCompressedFilePtr marks;
        MarkWithSizesInCompressedFilePtr mark_with_sizes;

        size_t getOffsetInFile(size_t i) const
        {
            return single_file_mode ? (*mark_with_sizes)[i].mark.offset_in_compressed_file : (*marks)[i].offset_in_compressed_file;
        }

        size_t getOffsetInDecompressedBlock(size_t i) const
        {
            return single_file_mode ? (*mark_with_sizes)[i].mark.offset_in_decompressed_block : (*marks)[i].offset_in_decompressed_block;
        }

        std::unique_ptr<CompressedSeekableReaderBuffer> buf;
    };
    using StreamPtr = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<String, StreamPtr>;

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
        size_t aio_threshold,
        size_t max_read_buffer_size,
        const FileProviderPtr & file_provider_,
        const ReadLimiterPtr & read_limiter,
        size_t rows_threshold_per_read_,
        bool read_one_pack_every_time_,
        const String & tracing_id_,
<<<<<<< HEAD
        size_t max_sharing_column_count,
        const ScanContextPtr & scan_context_);
=======
        size_t max_sharing_column_bytes_,
        const ScanContextPtr & scan_context_,
        ReadTag read_tag_);
>>>>>>> 12d7a9617a (Storages: Refine memory tracker of data sharing (#8857))

    Block getHeader() const { return toEmptyBlock(read_columns); }

    /// Skipped rows before next call of #read().
    /// Return false if it is the end of stream.
    bool getSkippedRows(size_t & skip_rows);
    Block read();
    std::string path() const
    {
        // Status of DMFile can be updated when DMFileReader in used and the pathname will be changed.
        // For DMFileReader, always use the readable path.
        return DMFile::getPathByStatus(dmfile->parentPath(), dmfile->fileId(), DMFile::Status::READABLE);
    }
    void addCachedPacks(ColId col_id, size_t start_pack_id, size_t pack_count, ColumnPtr & col);

private:
    bool shouldSeek(size_t pack_id);

    void readFromDisk(ColumnDefine & column_define,
                      MutableColumnPtr & column,
                      size_t start_pack_id,
                      size_t read_rows,
                      size_t skip_packs,
                      bool force_seek);
    void readColumn(ColumnDefine & column_define,
                    ColumnPtr & column,
                    size_t start_pack_id,
                    size_t pack_count,
                    size_t read_rows,
                    size_t skip_packs,
                    bool force_seek);
    bool getCachedPacks(ColId col_id, size_t start_pack_id, size_t pack_count, size_t read_rows, ColumnPtr & col);

private:
    DMFilePtr dmfile;
    ColumnDefines read_columns;
    ColumnStreams column_streams{};

    const bool is_common_handle;

    // read_one_pack_every_time is used to create info for every pack
    const bool read_one_pack_every_time;

    const bool single_file_mode{};

    /// Clean read optimize
    // In normal mode, if there is no delta for some packs in stable, we can try to do clean read (enable_handle_clean_read is true).
    // In fast mode, if we don't need handle column, we will try to do clean read on handle_column(enable_handle_clean_read is true).
    //               if we don't need del column, we will try to do clean read on del_column(enable_del_clean_read is true).
    const bool enable_handle_clean_read;
    const bool enable_del_clean_read;
    const bool is_fast_scan;

    const UInt64 max_read_version;

    /// Filters
    DMFilePackFilter pack_filter;

    std::vector<size_t> skip_packs_by_column{};

    /// Caches
    MarkCachePtr mark_cache;
    const bool enable_column_cache;
    ColumnCachePtr column_cache;

    const ScanContextPtr scan_context;

    const size_t rows_threshold_per_read;
    const size_t max_sharing_column_bytes;

    size_t next_pack_id = 0;

    FileProviderPtr file_provider;

    LoggerPtr log;

    std::unique_ptr<ColumnSharingCacheMap> col_data_cache{};
    std::unordered_map<ColId, bool> last_read_from_cache{};
};

} // namespace DM
} // namespace DB
