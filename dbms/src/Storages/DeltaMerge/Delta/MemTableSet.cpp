#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileBig.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInMemory.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Delta/MemTableSet.h>

namespace ProfileEvents
{
extern const Event DMWriteBytes;
}

namespace DB
{
namespace DM
{
void MemTableSet::appendColumnFileInner(const ColumnFilePtr & column_file)
{
    // If this column file's schema is identical to last_schema, then use the last_schema instance,
    // so that we don't have to serialize my_schema instance.
    if (auto * m_file = column_file->tryToInMemoryFile(); m_file)
    {
        auto my_schema = m_file->getSchema();
        if (last_schema && my_schema && last_schema != my_schema && isSameSchema(*my_schema, *last_schema))
            m_file->resetIdenticalSchema(last_schema);
        else
            last_schema = my_schema;
    }
    else if (auto * t_file = column_file->tryToTinyFile(); t_file)
    {
        auto my_schema = t_file->getSchema();
        if (last_schema && my_schema && last_schema != my_schema && isSameSchema(*my_schema, *last_schema))
            t_file->resetIdenticalSchema(last_schema);
        else
            last_schema = my_schema;
    }

    if (!column_files.empty())
    {
        auto & last_column_file = column_files.back();
        if (last_column_file->isAppendable())
            last_column_file->disableAppend();
    }

    column_files.push_back(column_file);

    rows += column_file->getRows();
    bytes += column_file->getBytes();
    deletes += column_file->getDeletes();
}

void MemTableSet::appendColumnFile(const ColumnFilePtr & column_file)
{
    appendColumnFileInner(column_file);
}

void MemTableSet::appendToCache(DMContext & context, const Block & block, size_t offset, size_t limit)
{
    // If the `column_files` is not empty, and the last `column_file` is a `ColumnInMemoryFile`, we will merge the newly block into the last `column_file`.
    // Otherwise, create a new `ColumnInMemoryFile` and write into it.
    bool success = false;
    size_t append_bytes = block.bytes(offset, limit);
    if (!column_files.empty())
    {
        auto & last_column_file = column_files.back();
        if (last_column_file->isAppendable())
            success = last_column_file->append(context, block, offset, limit, append_bytes);
    }

    if (!success)
    {
        // Create a new column file.
        auto my_schema = (last_schema && isSameSchema(block, *last_schema)) ? last_schema : std::make_shared<Block>(block.cloneEmpty());
        auto new_column_file = std::make_shared<ColumnFileInMemory>(my_schema);
        // Must append the empty `new_column_file` to `column_files` before appending data to it,
        // because `appendColumnFileInner` will update stats related to `column_files` but we will update stats relate to `new_column_file` here.
        appendColumnFileInner(new_column_file);
        success = new_column_file->append(context, block, offset, limit, append_bytes);
        if (unlikely(!success))
            throw Exception("Write to MemTableSet failed", ErrorCodes::LOGICAL_ERROR);
    }
    rows += limit;
    bytes += append_bytes;
}

void MemTableSet::appendDeleteRange(const RowKeyRange & delete_range)
{
    auto f = std::make_shared<ColumnFileDeleteRange>(delete_range);
    appendColumnFileInner(f);
}

void MemTableSet::ingestColumnFiles(const RowKeyRange & range, const ColumnFiles & new_column_files, bool clear_data_in_range)
{
    // Prepend a DeleteRange to clean data before applying column files
    if (clear_data_in_range)
    {
        auto f = std::make_shared<ColumnFileDeleteRange>(range);
        appendColumnFileInner(f);
    }

    for (const auto & f : new_column_files)
    {
        appendColumnFileInner(f);
    }
}

ColumnFileSetSnapshotPtr MemTableSet::createSnapshot()
{
    auto snap = std::make_shared<ColumnFileSetSnapshot>(nullptr);
    snap->rows = rows;
    snap->bytes = bytes;
    snap->deletes = deletes;
    snap->column_files.reserve(column_files.size());

    size_t total_rows = 0;
    size_t total_deletes = 0;
    for (const auto & file : column_files)
    {
        // All column files in MemTableSet is constant(except append data to the cache object in ColumnFileInMemory),
        // and we always create new column file object when flushing, so it's safe to reuse the column file object here.
        snap->column_files.push_back(file);
        total_rows += file->getRows();
        total_deletes += file->getDeletes();
    }

    if (unlikely(total_rows != rows || total_deletes != deletes))
        throw Exception("Rows and deletes check failed!", ErrorCodes::LOGICAL_ERROR);

    return snap;
}

ColumnFileFlushTaskPtr MemTableSet::buildFlushTask(DMContext & context, size_t rows_offset, size_t deletes_offset, size_t flush_version)
{
    if (column_files.empty())
        return nullptr;

    // make the last column file not appendable
    if (column_files.back()->isAppendable())
        column_files.back()->disableAppend();

    size_t cur_rows_offset = rows_offset;
    size_t cur_deletes_offset = deletes_offset;
    auto flush_task = std::make_shared<ColumnFileFlushTask>(context, this->shared_from_this(), flush_version);
    for (auto & column_file : column_files)
    {
        auto & task = flush_task->addColumnFile(column_file);
        if (auto * m_file = column_file->tryToInMemoryFile(); m_file)
        {
            task.rows_offset = cur_rows_offset;
            task.deletes_offset = cur_deletes_offset;
            task.block_data = m_file->readDataForFlush();
        }
        cur_rows_offset += column_file->getRows();
        cur_deletes_offset += column_file->getDeletes();
    }
    if (unlikely(flush_task->getFlushRows() != rows || flush_task->getFlushDeletes() != deletes))
        throw Exception("Rows and deletes check failed", ErrorCodes::LOGICAL_ERROR);

    return flush_task;
}

void MemTableSet::removeColumnFilesInFlushTask(const ColumnFileFlushTask & flush_task)
{
    const auto & tasks = flush_task.getAllTasks();
    if (unlikely(tasks.size() > column_files.size()))
        throw Exception("column_files num check failed", ErrorCodes::LOGICAL_ERROR);

    ColumnFiles new_column_files;
    size_t flush_bytes = 0;
    auto column_file_iter = column_files.begin();
    for (const auto & task : tasks)
    {
        if (unlikely(column_file_iter == column_files.end() || *column_file_iter != task.column_file))
        {
            throw Exception("column_files check failed", ErrorCodes::LOGICAL_ERROR);
        }
        flush_bytes += task.column_file->getBytes();
        column_file_iter++;
    }
    size_t new_rows = 0;
    size_t new_bytes = 0;
    size_t new_deletes = 0;
    while (column_file_iter != column_files.end())
    {
        new_column_files.emplace_back(*column_file_iter);
        new_rows += (*column_file_iter)->getRows();
        new_bytes += (*column_file_iter)->getBytes();
        new_deletes += (*column_file_iter)->getDeletes();
    }
    column_files.swap(new_column_files);
    rows = new_rows;
    bytes = new_bytes;
    deletes = new_deletes;

    ProfileEvents::increment(ProfileEvents::DMWriteBytes, flush_bytes);
}


} // namespace DM
} // namespace DB
