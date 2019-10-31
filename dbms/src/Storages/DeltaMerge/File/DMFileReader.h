#pragma once

#include <DataStreams/MarkInCompressedFile.h>
#include <IO/CompressedReadBufferFromFile.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/MarkCache.h>

namespace DB
{
namespace DM
{

class DMFileReader
{
public:
    struct Stream
    {
        Stream(DMFileReader & reader, ColId col_id, size_t aio_threshold, size_t max_read_buffer_size, Logger * log);

        double                   avg_size_hint;
        MarksInCompressedFilePtr marks;

        std::unique_ptr<CompressedReadBufferFromFile> buf;
    };
    using StreamPtr     = std::unique_ptr<Stream>;
    using ColumnStreams = std::map<ColId, StreamPtr>;

    DMFileReader(const DMFilePtr &     dmfile_,
                 const ColumnDefines & read_columns_,
                 const RSOperatorPtr & filter,
                 MarkCache *           mark_cache_,
                 MinMaxIndexCache *    index_cache_,
                 size_t                aio_threshold,
                 size_t                max_read_buffer_size);

    Block getHeader() const { return toEmptyBlock(read_columns); }

    /// Skipped rows before next call of #read().
    size_t getSkippedRows();
    Block  read();

private:
    void loadIndex(RSCheckParam & param, ColId col_id);
    bool shouldSeek(size_t chunk_id);

private:
    DMFilePtr     dmfile;
    ColumnDefines read_columns;

    MarkCache *        mark_cache;
    MinMaxIndexCache * index_cache;

    std::vector<UInt8> use_chunks;
    size_t             next_chunk_id = 0;

    ColumnStreams column_streams;

    Logger * log;
};

} // namespace DM
} // namespace DB
