#pragma once

#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>

namespace DB
{
namespace DM
{
class ColumnFileDeleteRange;
using ColumnDeleteRangeFilePtr = std::shared_ptr<ColumnFileDeleteRange>;

/// A column file that contains a DeleteRange. It will remove all covered data in the previous column files.
class ColumnFileDeleteRange : public ColumnFilePersisted
{
private:
    RowKeyRange delete_range;

public:
    explicit ColumnFileDeleteRange(const RowKeyRange & delete_range_)
        : delete_range(delete_range_)
    {}
    explicit ColumnFileDeleteRange(RowKeyRange && delete_range_)
        : delete_range(std::move(delete_range_))
    {}
    ColumnFileDeleteRange(const ColumnFileDeleteRange &) = default;

    ColumnFileReaderPtr getReader(const DMContext & /*context*/,
                                  const StorageSnapshotPtr & /*storage_snap*/,
                                  const ColumnDefinesPtr & /*col_defs*/) const override;

    const auto & getDeleteRange() { return delete_range; }

    ColumnDeleteRangeFilePtr cloneWith(const RowKeyRange & range)
    {
        auto new_dpdr = new ColumnFileDeleteRange(*this);
        new_dpdr->delete_range = range;
        return std::shared_ptr<ColumnFileDeleteRange>(new_dpdr);
    }

    Type getType() const override { return Type::DELETE_RANGE; }
    size_t getDeletes() const override { return 1; };

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;

    static ColumnFilePersistedPtr deserializeMetadata(ReadBuffer & buf);

    String toString() const override { return "{delete_range:" + delete_range.toString() + "}"; }
};

class ColumnFileEmptyReader : public ColumnFileReader
{
public:
    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr &) override;
};
} // namespace DM
} // namespace DB
