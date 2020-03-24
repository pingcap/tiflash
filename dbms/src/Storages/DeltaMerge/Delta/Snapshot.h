#pragma once

#include <Storages/DeltaMerge/DeltaValueSpace.h>

namespace DB
{
namespace DM
{
struct DeltaSnapshot;
using DeltaSnapshotPtr = std::shared_ptr<DeltaSnapshot>;

struct DeltaSnapshot : private boost::noncopyable
{
    bool is_update;

    DeltaValueSpacePtr delta;
    StorageSnapshotPtr storage_snap;

    Packs  packs;
    size_t rows;
    size_t deletes;

    ColumnDefines       column_defines;
    std::vector<size_t> pack_rows;
    std::vector<size_t> pack_rows_end; // Speed up pack search.

    // The data of packs when reading.
    std::vector<Columns> packs_data;

    ~DeltaSnapshot()
    {
        if (is_update)
        {
            bool v = true;
            if (!delta->is_updating.compare_exchange_strong(v, false))
            {
                Logger * logger = &Logger::get("DeltaValueSpace::Snapshot");
                LOG_ERROR(logger,
                          "!!!=========================delta [" << delta->getId()
                                                                << "] is expected to be updating=========================!!!");
            }
        }
    }

    /// Create a constant snapshot for read.
    /// Returns empty if this instance is abandoned, you should try again.
    static DeltaSnapshotPtr create(const DMContext & context, const DeltaValueSpacePtr & delta, bool is_update = false);

    size_t getPackCount() const { return packs.size(); }
    size_t getRows() const { return rows; }
    size_t getDeletes() const { return deletes; }

    void prepare(const DMContext & context, const ColumnDefines & column_defines_);

    const Columns & getColumnsOfPack(size_t pack_index, size_t col_num);

    // Get blocks or delete_ranges of `ExtraHandleColumn` and `VersionColumn`.
    // If there are continuous blocks, they will be squashed into one block.
    // We use the result to update DeltaTree.
    BlockOrDeletes getMergeBlocks(size_t rows_begin, size_t deletes_begin, size_t rows_end, size_t deletes_end);

    Block  read(size_t pack_index);
    size_t read(const HandleRange & range, MutableColumns & output_columns, size_t offset, size_t limit);

private:
    Block read(size_t col_num, size_t offset, size_t limit);

    friend class DeltaSnapshotInputStream;
};

class DeltaSnapshotInputStream : public IBlockInputStream
{
    DeltaSnapshotPtr delta_snap;
    size_t           next_pack_index = 0;

public:
    DeltaSnapshotInputStream(const DeltaSnapshotPtr & delta_snap_) : delta_snap(delta_snap_) {}

    String getName() const override { return "DeltaSnapshot"; }
    Block  getHeader() const override { return toEmptyBlock(delta_snap->column_defines); }

    Block read() override
    {
        for (; next_pack_index < delta_snap->packs.size(); ++next_pack_index)
        {
            if (!(delta_snap->packs[next_pack_index]->isDeleteRange()))
                break;
        }
        if (next_pack_index >= delta_snap->packs.size())
            return {};
        return delta_snap->read(next_pack_index++);
    }
};


} // namespace DM
} // namespace DB