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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetReader.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTinyReader.h>
#include <Storages/DeltaMerge/DMContext.h>


namespace DB::DM
{

std::pair<size_t, size_t> findColumnFile(const ColumnFiles & column_files, size_t rows_offset, size_t deletes_offset)
{
    size_t rows_count = 0;
    size_t deletes_count = 0;
    size_t column_file_index = 0;
    for (; column_file_index < column_files.size(); ++column_file_index)
    {
        if (rows_count == rows_offset && deletes_count == deletes_offset)
            return {column_file_index, 0};
        const auto & column_file = column_files[column_file_index];

        if (column_file->isDeleteRange())
        {
            if (deletes_count == deletes_offset)
            {
                if (unlikely(rows_count != rows_offset))
                    throw Exception(
                        "rows_count and rows_offset are expected to be equal. column_file_index: "
                        + DB::toString(column_file_index) + ", column_file_size: " + DB::toString(column_files.size())
                        + ", rows_count: " + DB::toString(rows_count) + ", rows_offset: " + DB::toString(rows_offset)
                        + ", deletes_count: " + DB::toString(deletes_count)
                        + ", deletes_offset: " + DB::toString(deletes_offset));
                return {column_file_index, 0};
            }
            ++deletes_count;
        }
        else
        {
            size_t column_file_rows = column_file->getRows();
            rows_count += column_file_rows;
            if (rows_count > rows_offset)
            {
                if (unlikely(deletes_count != deletes_offset))
                    throw Exception(
                        "deletes_count and deletes_offset are expected to be equal. column_file_index: "
                            + DB::toString(column_file_index) + ", column_file_size: "
                            + DB::toString(column_files.size()) + ", rows_count: " + DB::toString(rows_count)
                            + ", rows_offset: " + DB::toString(rows_offset) + ", deletes_count: "
                            + DB::toString(deletes_count) + ", deletes_offset: " + DB::toString(deletes_offset),
                        ErrorCodes::LOGICAL_ERROR);

                return {column_file_index, column_file_rows - (rows_count - rows_offset)};
            }
        }
    }
    if (rows_count != rows_offset || deletes_count != deletes_offset)
        throw Exception(
            "illegal rows_offset and deletes_offset. column_file_size: " + DB::toString(column_files.size())
                + ", rows_count: " + DB::toString(rows_count) + ", rows_offset: " + DB::toString(rows_offset)
                + ", deletes_count: " + DB::toString(deletes_count)
                + ", deletes_offset: " + DB::toString(deletes_offset),
            ErrorCodes::LOGICAL_ERROR);

    return {column_file_index, 0};
}

ColumnFileSetReader::ColumnFileSetReader(const DMContext & context_)
    : context(context_)
{}

ColumnFileSetReader::ColumnFileSetReader(
    const DMContext & context_,
    const ColumnFileSetSnapshotPtr & snapshot_,
    const ColumnDefinesPtr & col_defs_,
    const RowKeyRange & segment_range_,
    ReadTag read_tag_)
    : context(context_)
    , snapshot(snapshot_)
    , col_defs(col_defs_)
    , segment_range(segment_range_)
{
    size_t total_rows = 0;
    for (auto & f : snapshot->getColumnFiles())
    {
        total_rows += f->getRows();
        column_file_rows.push_back(f->getRows());
        column_file_rows_end.push_back(total_rows);
        auto reader = f->getReader(context, snapshot->getDataProvider(), col_defs, read_tag_);
        column_file_readers.emplace_back(std::move(reader));
    }
}

ColumnFileSetReaderPtr ColumnFileSetReader::createNewReader(const ColumnDefinesPtr & new_col_defs, ReadTag read_tag)
{
    auto * new_reader = new ColumnFileSetReader(context);
    new_reader->snapshot = snapshot;
    new_reader->col_defs = new_col_defs;
    new_reader->segment_range = segment_range;
    new_reader->column_file_rows = column_file_rows;
    new_reader->column_file_rows_end = column_file_rows_end;

    for (auto & fr : column_file_readers)
    {
        if (!fr)
        {
            new_reader->column_file_readers.emplace_back(nullptr);
            continue;
        }
        auto reader = fr->createNewReader(new_col_defs, read_tag);
        new_reader->column_file_readers.emplace_back(std::move(reader));
    }

    return std::shared_ptr<ColumnFileSetReader>(new_reader);
}

Block ColumnFileSetReader::readPKVersion(size_t offset, size_t limit)
{
    MutableColumns cols;
    for (size_t i = 0; i < 2; ++i)
        cols.push_back((*col_defs)[i].type->createColumn());
    readRows(cols, offset, limit, nullptr);
    Block block;
    for (size_t i = 0; i < 2; ++i)
    {
        const auto & cd = (*col_defs)[i];
        block.insert(ColumnWithTypeAndName(std::move(cols[i]), cd.type, cd.name, cd.id));
    }
    return block;
}

size_t ColumnFileSetReader::readRows(
    MutableColumns & output_columns,
    size_t offset,
    size_t limit,
    const RowKeyRange * range,
    std::vector<UInt32> * row_ids)
{
    // Note that DeltaMergeBlockInputStream could ask for rows with larger index than total_delta_rows,
    // because DeltaIndex::placed_rows could be larger than total_delta_rows.
    // Here is the example:
    //  1. Thread A create a delta snapshot with 10 rows. Now DeltaValueSnapshot::shared_delta_index->placed_rows == 10.
    //  2. Thread B insert 5 rows into the delta
    //  3. Thread B call Segment::ensurePlace to generate a new DeltaTree, placed_rows = 15, and update DeltaValueSnapshot::shared_delta_index = 15
    //  4. Thread A call Segment::ensurePlace, and DeltaValueReader::shouldPlace will return false. Because placed_rows(15) >= 10
    //  5. Thread A use the DeltaIndex with placed_rows = 15 to do the merge in DeltaMergeBlockInputStream
    //
    // So here, we should filter out those out-of-range rows.

    auto total_delta_rows = snapshot->getRows();

    auto start = std::min(offset, total_delta_rows);
    auto end = std::min(offset + limit, total_delta_rows);
    if (end == start)
        return 0;

    auto [start_file_index, rows_start_in_start_file] = locatePosByAccumulation(column_file_rows_end, start);
    auto [end_file_index, rows_end_in_end_file] = locatePosByAccumulation(column_file_rows_end, end);

    size_t actual_read = 0;
    for (size_t file_index = start_file_index; file_index <= end_file_index; ++file_index)
    {
        size_t rows_start_in_file = file_index == start_file_index ? rows_start_in_start_file : 0;
        size_t rows_end_in_file = file_index == end_file_index ? rows_end_in_end_file : column_file_rows[file_index];
        size_t rows_in_file_limit = rows_end_in_file - rows_start_in_file;

        // Nothing to read.
        if (rows_in_file_limit == 0)
            continue;

        auto & column_file_reader = column_file_readers[file_index];
        auto [read_offset, read_rows]
            = column_file_reader->readRows(output_columns, rows_start_in_file, rows_in_file_limit, range);
        actual_read += read_rows;
        if (row_ids != nullptr)
        {
            auto rows_before_cur_file = file_index == 0 ? 0 : column_file_rows_end[file_index - 1];
            auto start_row_id = read_offset + rows_before_cur_file;
            auto row_ids_offset = row_ids->size();
            row_ids->resize(row_ids->size() + read_rows);
            for (size_t i = 0; i < read_rows; ++i)
            {
                (*row_ids)[row_ids_offset + i] = start_row_id + i;
            }
        }
    }

    return actual_read;
}

void ColumnFileSetReader::getPlaceItems(
    BlockOrDeletes & place_items,
    size_t rows_begin,
    size_t deletes_begin,
    size_t rows_end,
    size_t deletes_end,
    size_t place_rows_offset)
{
    /// Note that we merge the consecutive ColumnFileInMemory or ColumnFileTiny together, which are seperated in groups by ColumnFileDeleteRange and ColumnFileBig.
    auto & column_files = snapshot->getColumnFiles();

    auto [start_file_index, rows_start_in_start_file] = findColumnFile(column_files, rows_begin, deletes_begin);
    auto [end_file_index, rows_end_in_end_file] = findColumnFile(column_files, rows_end, deletes_end);

    size_t block_rows_start = rows_begin;
    size_t block_rows_end = rows_begin;

    for (size_t file_index = start_file_index; file_index < column_files.size() && file_index <= end_file_index;
         ++file_index)
    {
        auto & column_file = *column_files[file_index];

        if (column_file.isDeleteRange() || column_file.isBigFile())
        {
            // First, compact the ColumnFileInMemory or ColumnFileTiny before this column file into one block.
            if (block_rows_end != block_rows_start)
            {
                auto block = readPKVersion(block_rows_start, block_rows_end - block_rows_start);
                place_items.emplace_back(std::move(block), block_rows_start + place_rows_offset);
            }

            // Second, take current column file.
            if (auto * dr = column_file.tryToDeleteRange(); dr)
            {
                place_items.emplace_back(dr->getDeleteRange());
            }
            else if (column_file.isBigFile() && column_file.getRows())
            {
                auto block = readPKVersion(block_rows_end, column_file.getRows());
                place_items.emplace_back(std::move(block), block_rows_end + place_rows_offset);
            }

            block_rows_end += column_file.getRows();
            block_rows_start = block_rows_end;
        }
        else
        {
            // It is a ColumnFileInMemory or ColumnFileTiny.
            size_t rows_start_in_file = file_index == start_file_index ? rows_start_in_start_file : 0;
            size_t rows_end_in_file = file_index == end_file_index ? rows_end_in_end_file : column_file.getRows();

            block_rows_end += rows_end_in_file - rows_start_in_file;

            if (file_index == column_files.size() - 1 || file_index == end_file_index)
            {
                // It is the last column file.
                if (block_rows_end != block_rows_start)
                {
                    auto block = readPKVersion(block_rows_start, block_rows_end - block_rows_start);
                    place_items.emplace_back(std::move(block), block_rows_start + place_rows_offset);
                }
                block_rows_start = block_rows_end;
            }
        }
    }
}

bool ColumnFileSetReader::shouldPlace(
    const DMContext & context,
    const RowKeyRange & relevant_range,
    UInt64 start_ts,
    size_t placed_rows)
{
    auto & column_files = snapshot->getColumnFiles();
    auto [start_file_index, rows_start_in_start_file] = locatePosByAccumulation(column_file_rows_end, placed_rows);

    for (size_t file_index = start_file_index; file_index < snapshot->getColumnFileCount(); ++file_index)
    {
        auto & column_file = column_files[file_index];

        // Always do place index if ColumnFileBig exists.
        if (column_file->isBigFile())
            return true;
        if (unlikely(column_file->isDeleteRange()))
            throw Exception("column file is delete range", ErrorCodes::LOGICAL_ERROR);

        size_t rows_start_in_file = file_index == start_file_index ? rows_start_in_start_file : 0;
        size_t rows_end_in_file = column_file_rows[file_index];

        auto & column_file_reader = column_file_readers[file_index];
        if (column_file->isInMemoryFile())
        {
            auto & dpb_reader = typeid_cast<ColumnFileInMemoryReader &>(*column_file_reader);
            auto pk_column = dpb_reader.getPKColumn();
            auto version_column = dpb_reader.getVersionColumn();

            auto rkcc = RowKeyColumnContainer(pk_column, context.is_common_handle);
            const auto & version_col_data = toColumnVectorData<UInt64>(version_column);

            for (auto i = rows_start_in_file; i < rows_end_in_file; ++i)
            {
                if (version_col_data[i] <= start_ts && relevant_range.check(rkcc.getRowKeyValue(i)))
                    return true;
            }
        }
        else if (column_file->isTinyFile())
        {
            auto & dpb_reader = typeid_cast<ColumnFileTinyReader &>(*column_file_reader);
            auto pk_column = dpb_reader.getPKColumn();
            auto version_column = dpb_reader.getVersionColumn();

            auto rkcc = RowKeyColumnContainer(pk_column, context.is_common_handle);
            const auto & version_col_data = toColumnVectorData<UInt64>(version_column);

            for (auto i = rows_start_in_file; i < rows_end_in_file; ++i)
            {
                if (version_col_data[i] <= start_ts && relevant_range.check(rkcc.getRowKeyValue(i)))
                    return true;
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown column file: {}", column_file->toString());
        }
    }

    return false;
}

} // namespace DB::DM
