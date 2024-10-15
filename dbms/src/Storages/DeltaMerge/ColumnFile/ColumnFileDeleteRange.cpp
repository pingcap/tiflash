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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileDeleteRange.h>
#include <Storages/DeltaMerge/DMContext.h>


namespace DB::DM
{

ColumnFileReaderPtr ColumnFileDeleteRange::getReader(
    const DMContext &,
    const IColumnFileDataProviderPtr &,
    const ColumnDefinesPtr &) const
{
    // ColumnFileDeleteRange is not readable.
    return nullptr;
}

void ColumnFileDeleteRange::serializeMetadata(WriteBuffer & buf, bool /*save_schema*/) const
{
    delete_range.serialize(buf);
}

void ColumnFileDeleteRange::serializeMetadata(dtpb::ColumnFilePersisted * cf_pb, bool /*save_schema*/) const
{
    auto * delete_range_pb = cf_pb->mutable_delete_range();
    auto range = delete_range.serialize();
    delete_range_pb->mutable_range()->Swap(&range);
}

ColumnFilePersistedPtr ColumnFileDeleteRange::deserializeMetadata(ReadBuffer & buf)
{
    return std::make_shared<ColumnFileDeleteRange>(RowKeyRange::deserialize(buf));
}

ColumnFilePersistedPtr ColumnFileDeleteRange::deserializeMetadata(const dtpb::ColumnFileDeleteRange & dr_pb)
{
    return std::make_shared<ColumnFileDeleteRange>(RowKeyRange::deserialize(dr_pb.range()));
}

} // namespace DB::DM
