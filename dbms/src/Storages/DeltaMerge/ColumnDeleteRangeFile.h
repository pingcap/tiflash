#pragma once

#include <Storages/DeltaMerge/ColumnStableFile.h>

namespace DB
{
namespace DM
{
class ColumnDeleteRangeFile : public ColumnStableFile
{
private:
    RowKeyRange delete_range;

public:
    explicit ColumnDeleteRangeFile(const RowKeyRange & delete_range_)
        : delete_range(delete_range_)
    {}
    explicit ColumnDeleteRangeFile(RowKeyRange && delete_range_)
        : delete_range(std::move(delete_range_))
    {}
    ColumnDeleteRangeFile(const ColumnDeleteRangeFile &) = default;

    ColumnFileReaderPtr getReader(const DMContext & /*context*/,
                                 const StorageSnapshotPtr & /*storage_snap*/,
                                 const ColumnDefinesPtr & /*col_defs*/) const override;

    const auto & getDeleteRange() { return delete_range; }

    size_t getDeletes() const override { return 1; };

    Type getType() const override { return Type::DELETE_RANGE; }

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;

    static ColumnStableFilePtr deserializeMetadata(ReadBuffer & buf);

    String toString() const override { return "{delete_range:" + delete_range.toString() + ", saved: " + DB::toString(saved) + "}"; }
};

class ColumnFileEmptyReader : public ColumnFileReader
{
public:
    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr &) override;
};
}
}
