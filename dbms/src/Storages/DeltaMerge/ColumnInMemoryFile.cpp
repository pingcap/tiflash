
#include <Storages/DeltaMerge/ColumnInMemoryFile.h>
#include <Storages/DeltaMerge/ColumnTinyFile.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>


namespace DB
{
namespace DM
{
void ColumnInMemoryFile::fillColumns(const ColumnDefines & col_defs, size_t col_count, Columns & result) const
{
    if (result.size() >= col_count)
        return;

    std::scoped_lock lock(cache->mutex);
    size_t col_start = result.size();
    size_t col_end = col_count;
    Columns read_cols;
    for (size_t i = col_start; i < col_end; ++i)
    {
        const auto & cd = col_defs[i];
        if (auto it = colid_to_offset.find(cd.id); it != colid_to_offset.end())
        {
            auto col_offset = it->second;
            // Copy data from cache
            const auto & type = getDataType(cd.id);
            auto col_data = type->createColumn();
            col_data->insertRangeFrom(*cache->block.getByPosition(col_offset).column, 0, rows);
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

bool ColumnInMemoryFile::append(DMContext & context, const Block & data, size_t offset, size_t limit, size_t data_bytes)
{
    if (disable_append)
        return false;

    std::scoped_lock lock(cache->mutex);
    if (!isSameSchema(cache->block, data))
        return false;

    // check whether this instance overflows
    if(cache->block.rows() >= context.delta_cache_limit_rows || cache->block.bytes() >= context.delta_cache_limit_bytes)
        return false;

    for (size_t i = 0; i < cache->block.columns(); ++i)
    {
        auto & col = data.getByPosition(i).column;
        auto & cache_col = *cache->block.getByPosition(i).column;
        auto * mutable_cache_col = const_cast<IColumn *>(&cache_col);
        mutable_cache_col->insertRangeFrom(*col, offset, limit);
    }

    rows += limit;
    bytes += data_bytes;
    return true;
}

ColumnFileReaderPtr
ColumnInMemoryFile::getReader(const DMContext & /*context*/, const StorageSnapshotPtr & /*storage_snap*/, const ColumnDefinesPtr & col_defs) const
{
    return std::make_shared<ColumnInMemoryFileReader>(*this, col_defs);
}

Block ColumnInMemoryFile::readDataForFlush() const
{
    std::scoped_lock lock(cache->mutex);

    auto & cache_block = cache->block;
    MutableColumns columns = cache_block.cloneEmptyColumns();
    for (size_t i = 0; i < cache_block.columns(); ++i)
        columns[i]->insertRangeFrom(*cache_block.getByPosition(i).column, 0, rows);
    return cache_block.cloneWithColumns(std::move(columns));
}


ColumnPtr ColumnInMemoryFileReader::getPKColumn()
{
    memory_file.fillColumns(*col_defs, 1, cols_data_cache);
    return cols_data_cache[0];
}

ColumnPtr ColumnInMemoryFileReader::getVersionColumn()
{
    memory_file.fillColumns(*col_defs, 2, cols_data_cache);
    return cols_data_cache[1];
}

size_t ColumnInMemoryFileReader::readRows(MutableColumns & output_cols, size_t rows_offset, size_t rows_limit, const RowKeyRange * range)
{
    memory_file.fillColumns(*col_defs, output_cols.size(), cols_data_cache);

    auto & pk_col = cols_data_cache[0];
    return copyColumnsData(cols_data_cache, pk_col, output_cols, rows_offset, rows_limit, range);
}

Block ColumnInMemoryFileReader::readNextBlock()
{
    if (read_done)
        return {};

    Columns columns;
    memory_file.fillColumns(*col_defs, col_defs->size(), columns);

    read_done = true;

    return genBlock(*col_defs, columns);
}

ColumnFileReaderPtr ColumnInMemoryFileReader::createNewReader(const ColumnDefinesPtr & new_col_defs)
{
    // Reuse the cache data.
    return std::make_shared<ColumnInMemoryFileReader>(memory_file, new_col_defs, cols_data_cache);
}

}
}
