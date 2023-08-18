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

#include <IO/CompressedStream.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider_fwd.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Transaction/Types.h>

namespace DB
{
class IColumn;
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
} // namespace DB
namespace DB::DM
{
struct DMContext;
struct RowKeyRange;
using RowKeyRanges = std::vector<RowKeyRange>;
struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;
class ColumnFileSetSnapshot;
using ColumnFileSetSnapshotPtr = std::shared_ptr<ColumnFileSetSnapshot>;
class ColumnFileTiny;
using ColumnFileTinyPtr = std::shared_ptr<ColumnFileTiny>;
class ColumnFileInMemory;
using ColumnFileInMemoryPtr = std::shared_ptr<ColumnFileInMemory>;
class ColumnFileDeleteRange;
using ColumnFileDeleteRangePtr = std::shared_ptr<ColumnFileDeleteRange>;
class ColumnFileBig;
using ColumnFileBigPtr = std::shared_ptr<ColumnFileBig>;
class ColumnFileSchema;
using ColumnFileSchemaPtr = std::shared_ptr<ColumnFileSchema>;
} // namespace DB::DM

namespace DB::DM::Remote
{
struct Serializer
{
    static RemotePb::RemotePhysicalTable serializeTo(
        const DisaggPhysicalTableReadSnapshotPtr & snap,
        const DisaggTaskId & task_id);

    /// segment snapshot ///

    static RemotePb::RemoteSegment serializeTo(
        const SegmentSnapshotPtr & snap,
        PageIdU64 segment_id,
        UInt64 segment_epoch,
        const RowKeyRange & segment_range,
        const RowKeyRanges & read_ranges);

    static SegmentSnapshotPtr deserializeSegmentSnapshotFrom(
        const DMContext & dm_context,
        StoreID remote_store_id,
        TableID table_id,
        const RemotePb::RemoteSegment & proto);

    /// column file set ///

    static google::protobuf::RepeatedPtrField<RemotePb::ColumnFileRemote> serializeTo(
        const ColumnFileSetSnapshotPtr & snap);

    /// Note: This function always build a snapshot over nop data provider. In order to read from this snapshot,
    /// you must explicitly assign a proper data provider.
    static ColumnFileSetSnapshotPtr deserializeColumnFileSet(
        const google::protobuf::RepeatedPtrField<RemotePb::ColumnFileRemote> & proto,
        const Remote::IDataStorePtr & data_store,
        const RowKeyRange & segment_range);

    /// column file ///

    static RemotePb::ColumnFileRemote serializeTo(const ColumnFileInMemory & cf_in_mem);
    static ColumnFileInMemoryPtr deserializeCFInMemory(const RemotePb::ColumnFileInMemory & proto);

    static RemotePb::ColumnFileRemote serializeTo(const ColumnFileTiny & cf_tiny, IColumnFileDataProviderPtr data_provider);
    static ColumnFileTinyPtr deserializeCFTiny(const RemotePb::ColumnFileTiny & proto);

    static RemotePb::ColumnFileRemote serializeTo(const ColumnFileDeleteRange & cf_delete_range);
    static ColumnFileDeleteRangePtr deserializeCFDeleteRange(const RemotePb::ColumnFileDeleteRange & proto);

    static RemotePb::ColumnFileRemote serializeTo(const ColumnFileBig & cf_big);
    static ColumnFileBigPtr deserializeCFBig(
        const RemotePb::ColumnFileBig & proto,
        const Remote::IDataStorePtr & data_store,
        const RowKeyRange & segment_range);
};
} // namespace DB::DM::Remote
