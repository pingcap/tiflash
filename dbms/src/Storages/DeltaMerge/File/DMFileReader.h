#pragma once

#include <DataStreams/MarkInCompressedFile.h>
#include <IO/CompressedReadBufferFromFile.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/MarkCache.h>
#include <Storages/DeltaMerge/File/ColumnCache.h>

namespace DB
{
namespace DM
{

static const size_t DMFILE_READ_ROWS_THRESHOLD = DEFAULT_MERGE_BLOCK_SIZE * 3;

class DMFileReader
{
public:
    struct Stream
    {
        Stream(DMFileReader & reader, //
               ColId          col_id,
               const String & file_name_base,
               size_t         aio_threshold,
               size_t         max_read_buffer_size,
               Logger *       log);

        double                   avg_size_hint;
        MarksInCompressedFilePtr marks;

        std::unique_ptr<CompressedReadBufferFromFile> buf;
    };
    using StreamPtr     = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<String, StreamPtr>;

    DMFileReader(bool                  enable_clean_read_,
                 UInt64                max_data_version_,
                 const DMFilePtr &     dmfile_,
                 const ColumnDefines & read_columns_,
                 const HandleRange &   handle_range_,
                 const RSOperatorPtr & filter,
                 ColumnCachePtr & column_cache_,
                 bool enable_column_cache_,
                 const IdSetPtr &      read_packs,
                 MarkCache *           mark_cache_,
                 MinMaxIndexCache *    index_cache_,
                 UInt64                hash_salt_,
                 size_t                aio_threshold,
                 size_t                max_read_buffer_size,
                 size_t                rows_threshold_per_read_ = DMFILE_READ_ROWS_THRESHOLD);

    Block getHeader() const { return toEmptyBlock(read_columns); }

    /// Skipped rows before next call of #read().
    bool  getSkippedRows(size_t & skip_rows);
    Block read();

private:
    bool shouldSeek(size_t pack_id);

    ColumnPtr readFromDisk(ColumnDefine & column_define, size_t start_pack_id, size_t read_rows, size_t skip_packs);

private:
    bool          enable_clean_read;
    UInt64        max_data_version;
    DMFilePtr     dmfile;
    ColumnDefines read_columns;
    HandleRange   handle_range;

    MarkCache * mark_cache;
    UInt64      hash_salt;
    size_t      rows_threshold_per_read;

    DMFilePackFilter pack_filter;

    ColumnCachePtr column_cache;
    bool enable_column_cache;

    const std::vector<RSResult> & handle_res;
    const std::vector<UInt8> &    use_packs;

    std::vector<size_t> skip_packs_by_column;

    size_t next_pack_id = 0;

    ColumnStreams column_streams;

    Logger * log;
};

} // namespace DM
} // namespace DB
