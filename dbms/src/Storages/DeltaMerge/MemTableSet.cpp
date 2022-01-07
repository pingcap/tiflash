#include <Storages/DeltaMerge/ColumnInMemoryFile.h>
#include <Storages/DeltaMerge/MemTableSet.h>

namespace DB
{
namespace DM
{
bool MemTableSet::write(DMContext & context, const Block & block, size_t offset, size_t limit)
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return false;

    // TODO: if the block is too big, create a new `ColumnInMemoryFile`

    // If the `column_files` is not empty, and the last `column_file` is a `ColumnInMemoryFile`, we will merge the newly block into the last `column_file`.
    // Otherwise, create a new `ColumnInMemoryFile` and write into it.
    bool success = false;
    size_t append_bytes = block.bytes(offset, limit);
    if (!column_files.empty())
    {
        auto & last_column_file = column_files.back();
        if (last_column_file->isAppendable())
        {
            success = last_column_file->append(context, block, offset, limit, append_bytes);
            if (!success)
            {
                last_column_file->disableAppend();
            }
        }
    }

    if (!success)
    {
        auto new_column_file = std::make_shared<ColumnInMemoryFile>(block);
        success = new_column_file->append(context, block, offset, limit, append_bytes);
        // the last column file(if exists) in `column_files` should already be not appendable
        column_files.push_back(new_column_file);
    }

    if (unlikely(!success))
        throw Exception("Write to MemTableSet failed", ErrorCodes::LOGICAL_ERROR);

    rows += limit;
    bytes += append_bytes;

    return true;
}

ColumnFileSetSnapshotPtr MemTableSet::createSnapshot()
{
    std::scoped_lock lock(mutex);
    if (abandoned.load(std::memory_order_relaxed))
        return {};

    auto snap = std::make_shared<ColumnFileSetSnapshot>(nullptr);
    snap->rows = rows;
    snap->bytes = bytes;
    snap->deletes = deletes;
    snap->column_files.reserve(column_files.size());

    if (DM_RUN_CHECK)
    {
        size_t total_rows = 0;
        size_t total_deletes = 0;
        for (const auto & file : column_files)
        {
            total_rows += file->getRows();
            total_deletes += file->getDeletes();
        }

        if (unlikely(total_rows != rows || total_deletes != deletes))
            throw Exception("Rows and deletes check failed!", ErrorCodes::LOGICAL_ERROR);
    }

    return snap;
}

}
}
