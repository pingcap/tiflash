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

#pragma once

#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/Remote/Serializer_fwd.h>


namespace DB
{
namespace DM
{
class ColumnFileDeleteRange;
using ColumnFileDeleteRangePtr = std::shared_ptr<ColumnFileDeleteRange>;

/// A column file that contains a DeleteRange. It will remove all covered data in the previous column files.
class ColumnFileDeleteRange : public ColumnFilePersisted
{
    friend struct Remote::Serializer;

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

    ColumnFileReaderPtr getReader(const DMContext &, const IColumnFileDataProviderPtr &, const ColumnDefinesPtr &)
        const override;

    const auto & getDeleteRange() { return delete_range; }

    ColumnFileDeleteRangePtr cloneWith(const RowKeyRange & range)
    {
        auto * new_dpdr = new ColumnFileDeleteRange(*this);
        new_dpdr->delete_range = range;
        return std::shared_ptr<ColumnFileDeleteRange>(new_dpdr);
    }

    Type getType() const override { return Type::DELETE_RANGE; }
    size_t getDeletes() const override { return 1; }

    void serializeMetadata(WriteBuffer & buf, bool save_schema) const override;
    void serializeMetadata(dtpb::ColumnFilePersisted * cf_pb, bool save_schema) const override;

    static ColumnFilePersistedPtr deserializeMetadata(ReadBuffer & buf);
    static ColumnFilePersistedPtr deserializeMetadata(const dtpb::ColumnFileDeleteRange & dr_pb);

    bool mayBeFlushedFrom(ColumnFile * from_file) const override
    {
        if (const auto * other = from_file->tryToDeleteRange(); other)
            return delete_range == other->delete_range;
        else
            return false;
    }

    String toString() const override { return "{delete_range:" + delete_range.toString() + "}"; }
};

class ColumnFileEmptyReader : public ColumnFileReader
{
public:
    ColumnFileReaderPtr createNewReader(const ColumnDefinesPtr &) override;
};
} // namespace DM
} // namespace DB
