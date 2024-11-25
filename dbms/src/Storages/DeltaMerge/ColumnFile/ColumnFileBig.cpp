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

#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/RestoreDMFile.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/PathPool.h>


namespace DB::DM
{

ColumnFileBig::ColumnFileBig(const DMContext & dm_context, const DMFilePtr & file_, const RowKeyRange & segment_range_)
    : file(file_)
    , segment_range(segment_range_)
{
    calculateStat(dm_context);
}

void ColumnFileBig::calculateStat(const DMContext & dm_context)
{
    auto index_cache = dm_context.global_context.getMinMaxIndexCache();

    auto pack_filter = DMFilePackFilter::loadFrom(
        file,
        index_cache,
        /*set_cache_if_miss*/ false,
        {segment_range},
        EMPTY_RS_OPERATOR,
        {},
        dm_context.global_context.getFileProvider(),
        dm_context.getReadLimiter(),
        dm_context.scan_context,
        /*tracing_id*/ dm_context.tracing_id,
        ReadTag::Internal);

    std::tie(valid_rows, valid_bytes) = pack_filter.validRowsAndBytes();
}

void ColumnFileBig::removeData(WriteBatches & wbs) const
{
    // Here we remove the data id instead of file_id.
    // Because a dmfile could be used in several places, and only after all page ids are removed,
    // then the file_id got removed.
    wbs.removed_data.delPage(file->pageId());
}

ColumnFileReaderPtr ColumnFileBig::getReader(
    const DMContext & dm_context,
    const IColumnFileDataProviderPtr &,
    const ColumnDefinesPtr & col_defs,
    ReadTag read_tag) const
{
    return std::make_shared<ColumnFileBigReader>(dm_context, *this, col_defs, read_tag);
}

void ColumnFileBig::serializeMetadata(WriteBuffer & buf, bool /*save_schema*/) const
{
    writeIntBinary(file->pageId(), buf);
    writeIntBinary(valid_rows, buf);
    writeIntBinary(valid_bytes, buf);
}

void ColumnFileBig::serializeMetadata(dtpb::ColumnFilePersisted * cf_pb, bool /*save_schema*/) const
{
    auto * big_pb = cf_pb->mutable_big_file();
    big_pb->set_id(file->pageId());
    big_pb->set_valid_rows(valid_rows);
    big_pb->set_valid_bytes(valid_bytes);
    big_pb->set_meta_version(file->metaVersion());
}

ColumnFilePersistedPtr ColumnFileBig::deserializeMetadata(
    const DMContext & dm_context,
    const RowKeyRange & segment_range,
    ReadBuffer & buf)
{
    UInt64 file_page_id;
    size_t valid_rows, valid_bytes;

    readIntBinary(file_page_id, buf);
    readIntBinary(valid_rows, buf);
    readIntBinary(valid_bytes, buf);

    auto remote_data_store = dm_context.global_context.getSharedContextDisagg()->remote_data_store;
    // In this version, ColumnFileBig's meta_version is always 0.
    auto dmfile = remote_data_store
        ? restoreDMFileFromRemoteDataSource(dm_context, remote_data_store, file_page_id, /* meta_version */ 0)
        : restoreDMFileFromLocal(dm_context, file_page_id, /* meta_version */ 0);
    auto * dp_file = new ColumnFileBig(dmfile, valid_rows, valid_bytes, segment_range);
    return std::shared_ptr<ColumnFileBig>(dp_file);
}

ColumnFilePersistedPtr ColumnFileBig::deserializeMetadata(
    const DMContext & dm_context,
    const RowKeyRange & segment_range,
    const dtpb::ColumnFileBig & cf_pb)
{
    auto remote_data_store = dm_context.global_context.getSharedContextDisagg()->remote_data_store;
    auto dmfile = remote_data_store
        ? restoreDMFileFromRemoteDataSource(dm_context, remote_data_store, cf_pb.id(), cf_pb.meta_version())
        : restoreDMFileFromLocal(dm_context, cf_pb.id(), cf_pb.meta_version());
    auto * dp_file = new ColumnFileBig(dmfile, cf_pb.valid_rows(), cf_pb.valid_bytes(), segment_range);
    return std::shared_ptr<ColumnFileBig>(dp_file);
}

ColumnFilePersistedPtr ColumnFileBig::createFromCheckpoint(
    DMContext & dm_context,
    const RowKeyRange & target_range,
    ReadBuffer & buf,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs)
{
    UInt64 file_page_id;
    size_t valid_rows, valid_bytes;

    readIntBinary(file_page_id, buf);
    readIntBinary(valid_rows, buf);
    readIntBinary(valid_bytes, buf);

    // In this version, ColumnFileBig's meta_version is always 0.
    UInt64 meta_version = 0;

    auto remote_data_store = dm_context.global_context.getSharedContextDisagg()->remote_data_store;
    auto dmfile = restoreDMFileFromCheckpoint(dm_context, remote_data_store, temp_ps, wbs, file_page_id, meta_version);
    auto * dp_file = new ColumnFileBig(dmfile, valid_rows, valid_bytes, target_range);
    return std::shared_ptr<ColumnFileBig>(dp_file);
}

ColumnFilePersistedPtr ColumnFileBig::createFromCheckpoint(
    DMContext & dm_context,
    const RowKeyRange & target_range,
    const dtpb::ColumnFileBig & cf_pb,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs)
{
    UInt64 file_page_id = cf_pb.id();
    size_t valid_rows = cf_pb.valid_rows();
    size_t valid_bytes = cf_pb.valid_bytes();
    size_t meta_version = cf_pb.meta_version();

    auto remote_data_store = dm_context.global_context.getSharedContextDisagg()->remote_data_store;
    auto dmfile = restoreDMFileFromCheckpoint(dm_context, remote_data_store, temp_ps, wbs, file_page_id, meta_version);
    auto * dp_file = new ColumnFileBig(dmfile, valid_rows, valid_bytes, target_range);
    return std::shared_ptr<ColumnFileBig>(dp_file);
}

void ColumnFileBigReader::initStream()
{
    if (file_stream)
        return;

    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    file_stream = builder.setTracingID(dm_context.tracing_id)
                      .setReadTag(read_tag)
                      .build(
                          column_file.getFile(),
                          *col_defs,
                          RowKeyRanges{column_file.segment_range},
                          dm_context.scan_context);

    header = file_stream->getHeader();
    // If we only need to read pk and version columns, then cache columns data in memory.
    if (pk_ver_only)
    {
        Block block;
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

std::pair<size_t, size_t> ColumnFileBigReader::readRowsRepeatedly(
    MutableColumns & output_cols,
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range)
{
    if (unlikely(rows_offset + rows_limit > column_file.valid_rows))
        throw Exception("Try to read more rows", ErrorCodes::LOGICAL_ERROR);

    /// Read pk and version columns from cached.

    auto [start_block_index, rows_start_in_start_block] = locatePosByAccumulation(cached_block_rows_end, rows_offset);
    auto [end_block_index, rows_end_in_end_block] = locatePosByAccumulation(
        cached_block_rows_end, //
        rows_offset + rows_limit);
    size_t actual_offset = 0;
    size_t actual_read = 0;
    for (size_t block_index = start_block_index;
         block_index < cached_pk_ver_columns.size() && block_index <= end_block_index;
         ++block_index)
    {
        size_t rows_start_in_block = block_index == start_block_index ? rows_start_in_start_block : 0;
        size_t rows_end_in_block
            = block_index == end_block_index ? rows_end_in_end_block : cached_pk_ver_columns[block_index].at(0)->size();
        size_t rows_in_block_limit = rows_end_in_block - rows_start_in_block;

        // Nothing to read.
        if (rows_start_in_block == rows_end_in_block)
            continue;

        const auto & columns = cached_pk_ver_columns.at(block_index);
        const auto & pk_column = columns[0];

        auto [offset, rows]
            = copyColumnsData(columns, pk_column, output_cols, rows_start_in_block, rows_in_block_limit, range);
        // For DMFile, records are sorted by row key. Only the prefix blocks and the suffix blocks will be filtered by range.
        // It will read a continuous piece of data here. Update `actual_offset` after first successful read of data.
        if (actual_read == 0 && rows > 0)
        {
            auto rows_before_block_index = block_index == 0 ? 0 : cached_block_rows_end[block_index - 1];
            actual_offset = rows_before_block_index + offset;
        }
        actual_read += rows;
    }
    return {actual_offset, actual_read};
}

std::pair<size_t, size_t> ColumnFileBigReader::readRowsOnce(
    MutableColumns & output_cols, //
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range)
{
    auto read_next_block = [&, this]() -> bool {
        rows_before_cur_block += (static_cast<bool>(cur_block)) ? cur_block.rows() : 0;
        cur_block_data.clear();

        cur_block = file_stream->read();
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

    size_t rows_end = rows_offset + rows_limit;
    size_t actual_offset = 0;
    size_t actual_read = 0;
    size_t read_offset = rows_offset;
    while (read_offset < rows_end)
    {
        if (!cur_block || cur_block_offset == cur_block.rows())
        {
            if (unlikely(!read_next_block()))
                throw Exception(
                    "Not enough delta data to read [offset=" + DB::toString(rows_offset)
                        + "] [limit=" + DB::toString(rows_limit) + "] [read_offset=" + DB::toString(read_offset) + "]",
                    ErrorCodes::LOGICAL_ERROR);
        }
        if (unlikely(read_offset < rows_before_cur_block + cur_block_offset))
            throw Exception(
                "read_offset is too small [offset=" + DB::toString(rows_offset) + "] [limit=" + DB::toString(rows_limit)
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

        auto [offset, rows] = copyColumnsData(
            cur_block_data,
            cur_block_data[0],
            output_cols,
            read_start_in_block,
            read_limit_in_block,
            range);
        // For DMFile, records are sorted by row key. Only the prefix blocks and the suffix blocks will be filtered by range.
        // It will read a continuous piece of data here. Update `actual_offset` after first successful read of data.
        if (actual_read == 0 && rows > 0)
        {
            actual_offset = rows_before_cur_block + offset;
        }
        actual_read += rows;
        read_offset += read_limit_in_block;
        cur_block_offset += read_limit_in_block;
    }
    return {actual_offset, actual_read};
}

std::pair<size_t, size_t> ColumnFileBigReader::readRows(
    MutableColumns & output_cols,
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range)
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
        e.addMessage(" while reading DTFile " + column_file.getFile()->path());
        throw;
    }
}

Block ColumnFileBigReader::readNextBlock()
{
    initStream();

    if (pk_ver_only)
    {
        if (next_block_index_in_cache >= cached_pk_ver_columns.size())
        {
            return {};
        }
        auto & columns = cached_pk_ver_columns[next_block_index_in_cache];
        next_block_index_in_cache += 1;
        return header.cloneWithColumns(std::move(columns));
    }
    else
    {
        return file_stream->read();
    }
}

Block ColumnFileBigReader::readWithFilter(const IColumn::Filter & filter)
{
    initStream();

    if (pk_ver_only)
    {
        if (next_block_index_in_cache >= cached_pk_ver_columns.size())
        {
            return {};
        }
        auto & columns = cached_pk_ver_columns[next_block_index_in_cache];
        size_t passed_count = countBytesInFilter(filter);
        for (auto & col : columns)
            col = col->filter(filter, passed_count);
        next_block_index_in_cache += 1;
        return header.cloneWithColumns(std::move(columns));
    }
    else
    {
        return file_stream->readWithFilter(filter);
    }
}

size_t ColumnFileBigReader::skipNextBlock()
{
    initStream();

    if (pk_ver_only)
    {
        if (next_block_index_in_cache >= cached_pk_ver_columns.size())
        {
            return 0;
        }
        return cached_pk_ver_columns[next_block_index_in_cache++].front()->size();
    }
    else
    {
        return file_stream->skipNextBlock();
    }
}

ColumnFileReaderPtr ColumnFileBigReader::createNewReader(const ColumnDefinesPtr & new_col_defs, ReadTag read_tag)
{
    // Currently we don't reuse the cache data.
    return std::make_shared<ColumnFileBigReader>(dm_context, column_file, new_col_defs, read_tag);
}

} // namespace DB::DM
