#pragma once

#include <DataStreams/MarkInCompressedFile.h>
#include <Encryption/CompressedReadBufferFromFileProvider.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/MarkCache.h>

namespace DB
{
namespace DM
{

static const size_t DMFILE_READ_ROWS_THRESHOLD = DEFAULT_MERGE_BLOCK_SIZE * 3;

class DMFileReader
{
public:
    // Read stream for single column
    struct Stream
    {
        Stream(DMFileReader & reader, //
               ColId          col_id,
               const String & file_name_base,
               size_t         aio_threshold,
               size_t         max_read_buffer_size,
               Logger *       log);

        bool                             single_file_mode;
        double                           avg_size_hint;
        MarksInCompressedFilePtr         marks;
        MarkWithSizesInCompressedFilePtr mark_with_sizes;

        size_t getOffsetInFile(size_t i) const
        {
            return single_file_mode ? (*mark_with_sizes)[i].mark.offset_in_compressed_file : (*marks)[i].offset_in_compressed_file;
        }

        size_t getOffsetInDecompressedBlock(size_t i) const
        {
            return single_file_mode ? (*mark_with_sizes)[i].mark.offset_in_decompressed_block : (*marks)[i].offset_in_decompressed_block;
        }

        std::unique_ptr<CompressedReadBufferFromFileProvider> buf;
    };
    using StreamPtr     = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<String, StreamPtr>;

    DMFileReader(const DMFilePtr &     dmfile_,
                 const ColumnDefines & read_columns_,
                 // clean read
                 bool   enable_clean_read_,
                 UInt64 max_data_version_,
                 // filters
                 const RowKeyRange &   rowkey_range_,
                 const RSOperatorPtr & filter_,
                 const IdSetPtr &      read_packs_, // filter by pack index
                 // caches
                 UInt64                  hash_salt_,
                 MarkCache *             mark_cache_,
                 MinMaxIndexCache *      index_cache_,
                 bool                    enable_column_cache_,
                 ColumnCachePtr &        column_cache_,
                 size_t                  aio_threshold,
                 size_t                  max_read_buffer_size,
                 const FileProviderPtr & file_provider_,
                 size_t                  rows_threshold_per_read_ = DMFILE_READ_ROWS_THRESHOLD);

    Block getHeader() const { return toEmptyBlock(read_columns); }

    /// Skipped rows before next call of #read().
    bool  getSkippedRows(size_t & skip_rows);
    Block read();

private:
    bool shouldSeek(size_t pack_id);

    void readFromDisk(ColumnDefine &     column_define,
                      MutableColumnPtr & column,
                      size_t             start_pack_id,
                      size_t             read_rows,
                      size_t             skip_packs,
                      bool               force_seek);

private:
    DMFilePtr     dmfile;
    ColumnDefines read_columns;
    ColumnStreams column_streams;

    /// Clean read optimize
    // If there is no delta for some packs in stable, we can try to do clean read.
    const bool   enable_clean_read;
    const UInt64 max_read_version;

    /// Filters
    DMFilePackFilter              pack_filter;
    const std::vector<RSResult> & handle_res; // alias of handle_res in pack_filter
    const std::vector<UInt8> &    use_packs;  // alias of use_packs in pack_filter

    bool is_common_handle;

    std::vector<size_t> skip_packs_by_column;

    /// Caches
    const UInt64   hash_salt;
    MarkCache *    mark_cache;
    const bool     enable_column_cache;
    ColumnCachePtr column_cache;

    const size_t rows_threshold_per_read;

    size_t next_pack_id = 0;

    FileProviderPtr file_provider;

    bool single_file_mode;

    Logger * log;
};

} // namespace DM
} // namespace DB
