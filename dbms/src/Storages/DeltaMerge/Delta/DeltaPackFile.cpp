#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/DeltaPackFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>
#include <Storages/PathPool.h>

namespace DB
{
namespace DM
{


DeltaPackFile::DeltaPackFile(const DMContext & context, const DMFilePtr & file_, const RowKeyRange & segment_range_)
    : file(file_), segment_range(segment_range_)
{
    calculateStat(context);
}

void DeltaPackFile::calculateStat(const DMContext & context)
{
    auto index_cache = context.db_context.getGlobalContext().getMinMaxIndexCache();
    auto hash_salt   = context.hash_salt;

    auto pack_filter
        = DMFilePackFilter::loadFrom(file, index_cache, hash_salt, segment_range, EMPTY_FILTER, {}, context.db_context.getFileProvider());

    std::tie(valid_rows, valid_bytes) = pack_filter.validRowsAndBytes();
}

DeltaPackReaderPtr
DeltaPackFile::getReader(const DMContext & context, const StorageSnapshotPtr & /*storage_snap*/, const ColumnDefinesPtr & col_defs) const
{
    return std::make_shared<DPFileReader>(context, *this, col_defs);
}

void DeltaPackFile::serializeMetadata(WriteBuffer & buf, bool /*save_schema*/) const
{
    writeIntBinary(file->refId(), buf);
    writeIntBinary(valid_rows, buf);
    writeIntBinary(valid_bytes, buf);
}

DeltaPackPtr DeltaPackFile::deserializeMetadata(DMContext &         context, //
                                                const RowKeyRange & segment_range,
                                                ReadBuffer &        buf)
{
    UInt64 file_ref_id;
    size_t valid_rows, valid_bytes;

    readIntBinary(file_ref_id, buf);
    readIntBinary(valid_rows, buf);
    readIntBinary(valid_bytes, buf);

    auto file_id          = context.storage_pool.data().getNormalPageId(file_ref_id);
    auto file_parent_path = context.path_pool.getStableDiskDelegator().getDTFilePath(file_id);

    auto dmfile = DMFile::restore(context.db_context.getFileProvider(), file_id, file_ref_id, file_parent_path);

    auto dp_file = new DeltaPackFile(dmfile, valid_rows, valid_bytes, segment_range);
    return std::shared_ptr<DeltaPackFile>(dp_file);
}

void DPFileReader::initStream()
{
    if (file_stream)
        return;

    file_stream = std::make_shared<DMFileBlockInputStream>(context.db_context,
                                                           /*max_version*/ MAX_UINT64,
                                                           /*clean_read*/ false,
                                                           context.hash_salt,
                                                           pack.getFile(),
                                                           *col_defs,
                                                           pack.segment_range,
                                                           RSOperatorPtr{},
                                                           ColumnCachePtr{},
                                                           IdSetPtr{});

    // If we only need to read pk and version columns, then cache columns data in memory.
    if (pk_ver_only)
    {
        Block  block;
        size_t total_rows = 0;
        while ((block = file_stream->read()))
        {
            Columns columns;
            columns.push_back(block.getByPosition(0).column);
            if (col_defs->size() == 2)
                columns.push_back(block.getByPosition(1).column);
            cached_pk_ver_columns.push_back(std::move(columns));
            total_rows += block.rows();
            cached_block_rows_end.push_back(total_rows);
        }
    }
}

size_t DPFileReader::readRowsRepeatedly(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range)
{
    if (unlikely(rows_offset + rows_limit > pack.valid_rows))
        throw Exception("Try to read more rows", ErrorCodes::LOGICAL_ERROR);

    /// Read pk and version columns from cached.

    auto [start_block_index, rows_start_in_start_block] = locatePosByAccumulation(cached_block_rows_end, rows_offset);
    auto [end_block_index, rows_end_in_end_block]       = locatePosByAccumulation(cached_block_rows_end, //
                                                                            rows_offset + rows_limit);

    size_t actual_read = 0;
    for (size_t block_index = start_block_index; block_index < cached_pk_ver_columns.size() && block_index <= end_block_index;
         ++block_index)
    {
        size_t rows_start_in_block = block_index == start_block_index ? rows_start_in_start_block : 0;
        size_t rows_end_in_block
            = block_index == end_block_index ? rows_end_in_end_block : cached_pk_ver_columns[block_index].at(0)->size();
        size_t rows_in_block_limit = rows_end_in_block - rows_start_in_block;

        // Nothing to read.
        if (rows_start_in_block == rows_end_in_block)
            continue;

        const auto & columns   = cached_pk_ver_columns.at(block_index);
        const auto & pk_column = columns[0];

        actual_read += copyColumnsData(columns, pk_column, output_cols, rows_start_in_block, rows_in_block_limit, range);
    }
    return actual_read;
}

size_t DPFileReader::readRowsOnce(MutableColumns &    output_cols, //
                                  size_t              rows_offset,
                                  size_t              rows_limit,
                                  const RowKeyRange * range)
{
    auto read_next_block = [&, this]() -> bool {
        rows_before_cur_block += ((bool)cur_block) ? cur_block.rows() : 0;
        cur_block_data.clear();

        cur_block        = file_stream->read();
        cur_block_offset = 0;

        if (!cur_block)
        {
            file_stream = {};
            return false;
        }
        else
        {
            for (size_t col_index = 0; col_index < output_cols.size(); ++col_index)
                cur_block_data.push_back(cur_block.getByPosition(col_index).column);
            return true;
        }
    };

    size_t rows_end    = rows_offset + rows_limit;
    size_t actual_read = 0;
    size_t read_offset = rows_offset;
    while (read_offset < rows_end)
    {
        if (!cur_block || cur_block_offset == cur_block.rows())
        {
            if (unlikely(!read_next_block()))
                throw Exception("Not enough delta data to read [offset=" + DB::toString(rows_offset)
                                    + "] [limit=" + DB::toString(rows_limit) + "] [read_offset=" + DB::toString(read_offset) + "]",
                                ErrorCodes::LOGICAL_ERROR);
        }
        if (unlikely(read_offset < rows_before_cur_block + cur_block_offset))
            throw Exception("read_offset is too small [offset=" + DB::toString(rows_offset) + "] [limit=" + DB::toString(rows_limit)
                                + "] [read_offset=" + DB::toString(read_offset)
                                + "] [min_offset=" + DB::toString(rows_before_cur_block + cur_block_offset) + "]",
                            ErrorCodes::LOGICAL_ERROR);

        if (read_offset >= rows_before_cur_block + cur_block.rows())
        {
            cur_block_offset = cur_block.rows();
            continue;
        }
        auto read_end_for_cur_block = std::min(rows_end, rows_before_cur_block + cur_block.rows());

        auto read_start_in_block = read_offset - rows_before_cur_block;
        auto read_limit_in_block = read_end_for_cur_block - read_offset;

        actual_read += copyColumnsData(cur_block_data, cur_block_data[0], output_cols, read_start_in_block, read_limit_in_block, range);
        read_offset += read_limit_in_block;
        cur_block_offset += read_limit_in_block;
    }
    return actual_read;
}

size_t DPFileReader::readRows(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range)
{
    initStream();

    try
    {
        if (pk_ver_only)
            return readRowsRepeatedly(output_cols, rows_offset, rows_limit, range);
        else
            return readRowsOnce(output_cols, rows_offset, rows_limit, range);
    }
    catch (DB::Exception & e)
    {
        e.addMessage(" while reading DTFile " + pack.getFile()->path());
        throw;
    }
}

Block DPFileReader::readNextBlock()
{
    initStream();

    return file_stream->read();
}

DeltaPackReaderPtr DPFileReader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    // Currently we don't reuse the cache data.
    return std::make_shared<DPFileReader>(context, pack, new_col_defs);
}

} // namespace DM
} // namespace DB
