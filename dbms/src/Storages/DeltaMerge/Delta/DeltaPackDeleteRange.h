#pragma once

#include <Storages/DeltaMerge/Delta/DeltaPack.h>

namespace DB
{
namespace DM
{

/// A delta pack which contains a DeleteRange. It will remove all covered data in the previous packs.
class DeltaPackDeleteRange : public DeltaPack
{
private:
    RowKeyRange delete_range;

public:
    explicit DeltaPackDeleteRange(const RowKeyRange & delete_range_) : delete_range(delete_range_) {}
    explicit DeltaPackDeleteRange(RowKeyRange && delete_range_) : delete_range(std::move(delete_range_)) {}
    DeltaPackDeleteRange(const DeltaPackDeleteRange &) = default;

    DeltaPackReaderPtr getReader(const DMContext & /*context*/,
                                 const StorageSnapshotPtr & /*storage_snap*/,
                                 const ColumnDefinesPtr & /*col_defs*/) const override;

    const auto & getDeleteRange() { return delete_range; }

    DeltaPackDeleteRangePtr cloneWith(const RowKeyRange & range)
    {
        auto new_dpdr          = new DeltaPackDeleteRange(*this);
        new_dpdr->delete_range = range;
        return std::shared_ptr<DeltaPackDeleteRange>(new_dpdr);
    }

    Type   getType() const override { return Type::DELETE_RANGE; };
    size_t getDeletes() const override { return 1; };

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;

    static DeltaPackPtr deserializeMetadata(ReadBuffer & buf);

    String toString() const override { return "{delete_range:" + delete_range.toString() + ", saved: " + DB::toString(saved) + "}"; }
};

class DPEmptyReader : public DeltaPackReader
{
public:
    DeltaPackReaderPtr createNewReader(const ColumnDefinesPtr &) override;
};

} // namespace DM
} // namespace DB
