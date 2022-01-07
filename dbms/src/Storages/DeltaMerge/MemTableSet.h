#pragma once

#include "ColumnFile.h"
#include "ColumnFileSetSnapshot.h"


namespace DB
{
namespace DM
{


class MemTableSet : public std::enable_shared_from_this<MemTableSet>
    , private boost::noncopyable
{
private:
    // may contain `ColumnInMemoryFile`, `ColumnDeleteRangeFile`, `ColumnBigFile`
    ColumnFiles column_files;

    std::atomic<size_t> rows = 0;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> deletes = 0;

    /// This instance has been abandoned. Like after full compaction, split/merge.
    std::atomic_bool abandoned = false;

    // Protects the operations in this instance.
    mutable std::mutex mutex;

    Poco::Logger * log;

public:
    MemTableSet(): log(&Poco::Logger::get("MemTableSet")) {}

    bool write(DMContext & dm_context, const Block & block, size_t offset, size_t limit);

    /// Create a constant snapshot for read.
    /// Returns empty if this instance is abandoned, you should try again.
    ColumnFileSetSnapshotPtr createSnapshot();
};

}
}