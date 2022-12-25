// Copyright 2022 PingCAP, Ltd.
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

    ColumnFileReaderPtr getReader(
        const DMContext &,
        const IColumnFileSetStorageReaderPtr & reader,
        const ColumnDefinesPtr & col_defs) const override
    {
        return getReader(reader, col_defs);
    }

    ColumnFileReaderPtr getReader(
        const IColumnFileSetStorageReaderPtr &,
        const ColumnDefinesPtr &) const override;

    const auto & getDeleteRange() const { return delete_range; }

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

    dtpb::ColumnFileRemote serializeToRemoteProtocol() const override
    {
        dtpb::ColumnFileRemote ret;
        auto * remote_del = ret.mutable_delete_range();
        {
            WriteBufferFromString wb(*remote_del->mutable_key_range());
            delete_range.serialize(wb);
        }
        return ret;
    }

    static std::shared_ptr<ColumnFileDeleteRange> deserializeFromRemoteProtocol(
        const dtpb::ColumnFileDeleteRange & proto)
    {
        ReadBufferFromString rb(proto.key_range());
        auto range = RowKeyRange::deserialize(rb);

        LOG_DEBUG(Logger::get(), "Rebuild local ColumnFileDeleteRange from remote, range={}", range.toDebugString());

        return std::make_shared<ColumnFileDeleteRange>(range);
    }

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
