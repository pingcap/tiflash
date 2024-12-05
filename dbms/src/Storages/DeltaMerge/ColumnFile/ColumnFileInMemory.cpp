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

#include <Columns/countBytesInFilter.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>


namespace DB::DM
{

void ColumnFileInMemory::fillColumns(const ColumnDefines & col_defs, size_t col_count, Columns & result) const
{
    if (result.size() >= col_count)
        return;

    size_t col_start = result.size();
    size_t col_end = col_count;
    Columns read_cols;

    std::scoped_lock lock(cache->mutex);
    const auto & colid_to_offset = schema->getColIdToOffset();
    for (size_t i = col_start; i < col_end; ++i)
    {
        const auto & cd = col_defs[i];
        if (auto it = colid_to_offset.find(cd.id); it != colid_to_offset.end())
        {
            auto col_offset = it->second;
            // Copy data from cache
            const auto & type = getDataType(cd.id);
            auto col_data = type->createColumn();
            col_data->insertRangeFrom(*(cache->block.getByPosition(col_offset).column), 0, rows);
            // Cast if need
            auto col_converted = convertColumnByColumnDefineIfNeed(type, std::move(col_data), cd);
            read_cols.push_back(std::move(col_converted));
        }
        else
        {
            ColumnPtr column = createColumnWithDefaultValue(cd, rows);
            read_cols.emplace_back(std::move(column));
        }
    }
    result.insert(result.end(), read_cols.begin(), read_cols.end());
}

ColumnFileReaderPtr ColumnFileInMemory::getReader(
    const DMContext &,
    const IColumnFileDataProviderPtr &,
    const ColumnDefinesPtr & col_defs,
    ReadTag) const
{
    return std::make_shared<ColumnFileInMemoryReader>(*this, col_defs);
}

bool ColumnFileInMemory::append(
    const DMContext & context,
    const Block & data,
    size_t offset,
    size_t limit,
    size_t data_bytes)
{
    if (disable_append)
        return false;

    std::scoped_lock lock(cache->mutex);
    if (!isSameSchema(cache->block, data))
        return false;

    // check whether this instance overflows
    if (cache->block.rows() >= context.delta_cache_limit_rows
        || cache->block.bytes() >= context.delta_cache_limit_bytes)
        return false;

    for (size_t i = 0; i < cache->block.columns(); ++i)
    {
        const auto & col = data.getByPosition(i).column;
        const auto & cache_col = *cache->block.getByPosition(i).column;
        auto * mutable_cache_col = const_cast<IColumn *>(&cache_col);
        mutable_cache_col->insertRangeFrom(*col, offset, limit);
    }

    rows += limit;
    bytes += data_bytes;
    return true;
}

Block ColumnFileInMemory::readDataForFlush() const
{
    std::scoped_lock lock(cache->mutex);

    auto & cache_block = cache->block;
    MutableColumns columns = cache_block.cloneEmptyColumns();
    for (size_t i = 0; i < cache_block.columns(); ++i)
        columns[i]->insertRangeFrom(*cache_block.getByPosition(i).column, 0, rows);
    return cache_block.cloneWithColumns(std::move(columns));
}

std::pair<ColumnPtr, ColumnPtr> ColumnFileInMemoryReader::getPKAndVersionColumns()
{
    // fill the first 2 columns into `cols_data_cache`
    memory_file.fillColumns(*col_defs, 2, cols_data_cache);
    return {cols_data_cache[0], cols_data_cache[1]};
}

std::pair<size_t, size_t> ColumnFileInMemoryReader::readRows(
    MutableColumns & output_cols,
    size_t rows_offset,
    size_t rows_limit,
    const RowKeyRange * range)
{
    memory_file.fillColumns(*col_defs, output_cols.size(), cols_data_cache);

    auto & pk_col = cols_data_cache[0];
    return copyColumnsData(cols_data_cache, pk_col, output_cols, rows_offset, rows_limit, range);
}

Block ColumnFileInMemoryReader::readNextBlock()
{
    if (read_done)
        return {};

    Columns columns;
    memory_file.fillColumns(*col_defs, col_defs->size(), columns);

    read_done = true;

    return genBlock(*col_defs, columns);
}

Block ColumnFileInMemoryReader::readWithFilter(const IColumn::Filter & filter)
{
    auto block = readNextBlock();
    if (size_t passed_count = countBytesInFilter(filter); passed_count != block.rows())
    {
        for (auto & col : block)
        {
            col.column = col.column->filter(filter, passed_count);
        }
    }
    return block;
}

size_t ColumnFileInMemoryReader::skipNextBlock()
{
    if (read_done)
        return 0;

    read_done = true;
    return memory_file.getRows();
}

ColumnFileReaderPtr ColumnFileInMemoryReader::createNewReader(const ColumnDefinesPtr & new_col_defs, ReadTag)
{
    // Reuse the cache data.
    return std::make_shared<ColumnFileInMemoryReader>(memory_file, new_col_defs, cols_data_cache);
}

} // namespace DB::DM
