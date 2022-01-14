#include <Storages/DeltaMerge/ColumnInMemoryFile.h>
#include <Storages/DeltaMerge/MemTableSet.h>

namespace DB
{
namespace DM
{
// FIXME: difference from previous implementation: cannot get lastSchema from saved column_files
BlockPtr MemTableSet::lastSchema()
{
    for (auto it = column_files.rbegin(); it != column_files.rend(); ++it)
    {
        if (auto m_file = (*it)->tryToInMemoryFile(); m_file)
            return m_file->getSchema();
    }
    return {};
}

void MemTableSet::appendColumnFileInner(const ColumnFilePtr & column_file)
{
    auto last_schema = lastSchema();

    if (auto m_file = column_file->tryToInMemoryFile(); m_file)
    {
        // If this pack's schema is identical to last_schema, then use the last_schema instance,
        // so that we don't have to serialize my_schema instance.
        auto my_schema = m_file->getSchema();
        if (last_schema && my_schema && last_schema != my_schema && isSameSchema(*my_schema, *last_schema))
            m_file->resetIdenticalSchema(last_schema);
    }

    if (!column_files.empty())
    {
        auto last_column_file = column_files.back();
        if (last_column_file->isInMemoryFile())
            last_column_file->tryToInMemoryFile()->disableAppend();
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
        auto new_column_file = std::make_shared<ColumnInMemoryFile>(block);
        success = new_column_file->append(context, block, offset, limit, append_bytes);
        if (unlikely(!success))
            throw Exception("Write to MemTableSet failed", ErrorCodes::LOGICAL_ERROR);
        appendColumnFileInner(new_column_file);
    }
}

void MemTableSet::appendDeleteRange(const RowKeyRange & delete_range)
{
    auto f = std::make_shared<ColumnDeleteRangeFile>(delete_range);
    appendColumnFileInner(f);
}

void MemTableSet::ingestColumnFiles(const RowKeyRange & range, const ColumnFiles & column_files_, bool clear_data_in_range)
{
    // Prepend a DeleteRange to clean data before applying packs
    if (clear_data_in_range)
    {
        auto f = std::make_shared<ColumnDeleteRangeFile>(range);
        appendColumnFileInner(f);
    }

    for (auto & f : column_files_)
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
        // TODO: check the thread safety of ColumnFile
        snap->column_files.push_back(file);
        total_rows += file->getRows();
        total_deletes += file->getDeletes();
    }

    if (unlikely(total_rows != rows || total_deletes != deletes))
        throw Exception("Rows and deletes check failed!", ErrorCodes::LOGICAL_ERROR);

    return snap;
}

FlushColumnFileTaskPtr MemTableSet::buildFlushTask(DMContext & context, size_t rows_offset, size_t deletes_offset)
{
    if (column_files.empty())
        return nullptr;

    // make the last column file not appendable
    if (column_files.back()->isAppendable())
        column_files.back()->disableAppend();

    size_t cur_rows_offset = rows_offset;
    size_t cur_deletes_offset = deletes_offset;
    size_t flush_rows = 0;
    size_t flush_bytes = 0;
    size_t flush_deletes = 0;
    auto flush_task = std::make_shared<FlushColumnFileTask>(context, this->shared_from_this());
    for (auto & column_file : column_files)
    {
        auto & task = flush_task->tasks.emplace_back(column_file);
        if (auto mfile = column_file->tryToInMemoryFile(); mfile)
        {
            task.rows_offset = cur_rows_offset;
            task.deletes_offset = cur_deletes_offset;
            task.block_data = mfile->readDataForFlush();
        }
        flush_rows += column_file->getRows();
        flush_bytes += column_file->getBytes();
        flush_deletes += column_file->getDeletes();
        cur_rows_offset += column_file->getRows();
        cur_deletes_offset += column_file->getDeletes();
    }
    if (unlikely(flush_rows != rows || flush_deletes != deletes))
        throw Exception("Rows and deletes check failed", ErrorCodes::LOGICAL_ERROR);

    return flush_task;
}

void MemTableSet::removeColumnFilesInFlushTask(const FlushColumnFileTask & flush_task)
{
    auto & tasks = flush_task.tasks;
    if (unlikely(tasks.size() > column_files.size()))
        throw Exception("column_files num check failed", ErrorCodes::LOGICAL_ERROR);

    ColumnFiles new_column_files;
    auto column_file_iter = column_files.begin();
    for (auto & task : tasks)
    {
        if (unlikely(column_file_iter == column_files.end() || *column_file_iter != task.column_file))
        {
            throw Exception("column_files check failed", ErrorCodes::LOGICAL_ERROR);
        }
        column_file_iter++;
    }
    while (column_file_iter != column_files.end())
    {
        new_column_files.emplace_back(*column_file_iter);
    }
    column_files.swap(new_column_files);
}



}
}
