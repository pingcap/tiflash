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

#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider_fwd.h>
#include <Storages/DeltaMerge/DMContext_fwd.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/KVStore/Types.h>
#include <Storages/Page/PageDefinesBase.h>

namespace DB
{
class IColumn;
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
} // namespace DB
namespace DB::DM
{
struct RowKeyRange;
using RowKeyRanges = std::vector<RowKeyRange>;
struct SegmentSnapshot;
using SegmentSnapshotPtr = std::shared_ptr<SegmentSnapshot>;
class ColumnFileSetSnapshot;
using ColumnFileSetSnapshotPtr = std::shared_ptr<ColumnFileSetSnapshot>;
class ColumnFile;
using ColumnFilePtr = std::shared_ptr<ColumnFile>;
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
public:
    static RemotePb::RemotePhysicalTable serializePhysicalTable(
        const DisaggPhysicalTableReadSnapshotPtr & snap,
        const DisaggTaskId & task_id,
        MemTrackerWrapper & mem_tracker_wrapper,
        bool need_mem_data);

    static SegmentSnapshotPtr deserializeSegment(
        const DMContext & dm_context,
        StoreID remote_store_id,
        KeyspaceID keyspace_id,
        TableID table_id,
        const RemotePb::RemoteSegment & proto);

    /// Note: This function always build a snapshot over nop data provider. In order to read from this snapshot,
    /// you must explicitly assign a proper data provider.
    static ColumnFileSetSnapshotPtr deserializeColumnFileSet(
        const DMContext & dm_context,
        const google::protobuf::RepeatedPtrField<RemotePb::ColumnFileRemote> & proto,
        const Remote::IDataStorePtr & data_store,
        const RowKeyRange & segment_range);

    static RemotePb::ColumnFileRemote serializeCF(
        const ColumnFilePtr & cf,
        const IColumnFileDataProviderPtr & data_provider,
        bool need_mem_data);

private:
    static RemotePb::RemoteSegment serializeSegment(
        const SegmentSnapshotPtr & snap,
        PageIdU64 segment_id,
        UInt64 segment_epoch,
        const RowKeyRange & segment_range,
        const RowKeyRanges & read_ranges,
        MemTrackerWrapper & mem_tracker_wrapper,
        bool need_mem_data);

    static google::protobuf::RepeatedPtrField<RemotePb::ColumnFileRemote> serializeColumnFileSet(
        const ColumnFileSetSnapshotPtr & snap,
        MemTrackerWrapper & mem_tracker_wrapper,
        bool need_mem_data);

    static RemotePb::ColumnFileRemote serializeCFInMemory(const ColumnFileInMemory & cf_in_mem, bool need_mem_data);
    static ColumnFileInMemoryPtr deserializeCFInMemory(const RemotePb::ColumnFileInMemory & proto);

    static RemotePb::ColumnFileRemote serializeCFTiny(
        const ColumnFileTiny & cf_tiny,
        IColumnFileDataProviderPtr data_provider);
    static ColumnFileTinyPtr deserializeCFTiny(const DMContext & dm_context, const RemotePb::ColumnFileTiny & proto);

    static RemotePb::ColumnFileRemote serializeCFDeleteRange(const ColumnFileDeleteRange & cf_delete_range);
    static ColumnFileDeleteRangePtr deserializeCFDeleteRange(const RemotePb::ColumnFileDeleteRange & proto);

    static RemotePb::ColumnFileRemote serializeCFBig(const ColumnFileBig & cf_big);
    static ColumnFileBigPtr deserializeCFBig(
        const RemotePb::ColumnFileBig & proto,
        const Remote::IDataStorePtr & data_store,
        const RowKeyRange & segment_range);
};
} // namespace DB::DM::Remote
