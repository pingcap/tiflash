// Copyright 2023 PingCAP, Ltd.
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

#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedSnapshot.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Page/PageDefinesBase.h>

#include <magic_enum.hpp>

namespace DB::DM::Remote
{
RemotePb::RemotePhysicalTable
Serializer::serializeTo(const DisaggPhysicalTableReadSnapshotPtr & snap, const DisaggregatedTaskId & task_id)
{
    RemotePb::RemotePhysicalTable remote_table;
    remote_table.set_snapshot_id(task_id.toMeta().SerializeAsString());
    remote_table.set_table_id(snap->physical_table_id);
    for (const auto & seg_task : snap->tasks)
    {
        auto remote_seg = Serializer::serializeTo(
            seg_task->read_snapshot,
            seg_task->segment->segmentId(),
            seg_task->segment->segmentEpoch(),
            seg_task->segment->getRowKeyRange(),
            /*read_ranges*/ seg_task->ranges);
        remote_table.mutable_segments()->Add(std::move(remote_seg));
    }
    return remote_table;
}

RemotePb::RemoteSegment
Serializer::serializeTo(
    const SegmentSnapshotPtr & snap,
    PageIdU64 segment_id,
    UInt64 segment_epoch,
    const RowKeyRange & segment_range,
    const RowKeyRanges & read_ranges)
{
    RemotePb::RemoteSegment remote;
    remote.set_segment_id(segment_id);
    remote.set_segment_epoch(segment_epoch);

    WriteBufferFromOwnString wb;
    {
        // segment key_range
        segment_range.serialize(wb);
        remote.set_key_range(wb.releaseStr());
    }

    // stable
    for (const auto & dt_file : snap->stable->getDMFiles())
    {
        auto * remote_file = remote.add_stable_pages();
        remote_file->set_page_id(dt_file->pageId());
        remote_file->set_file_id(dt_file->fileId());
    }
    remote.mutable_column_files_memtable()->CopyFrom(serializeTo(snap->delta->getMemTableSetSnapshot()));
    remote.mutable_column_files_persisted()->CopyFrom(serializeTo(snap->delta->getPersistedFileSetSnapshot()));

    // serialize the read ranges to read node
    for (const auto & read_range : read_ranges)
    {
        wb.restart();
        read_range.serialize(wb);
        remote.add_read_key_ranges()->assign(wb.releaseStr());
    }

    return remote;
}

SegmentSnapshotPtr Serializer::deserializeSegmentSnapshotFrom(
    const Context & db_context,
    UInt64 write_node_id,
    Int64 table_id,
    const RemotePb::RemoteSegment & proto)
{
    UNUSED(db_context, write_node_id, table_id, proto);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "");
}

google::protobuf::RepeatedPtrField<RemotePb::ColumnFileRemote>
Serializer::serializeTo(const ColumnFileSetSnapshotPtr & snap)
{
    google::protobuf::RepeatedPtrField<RemotePb::ColumnFileRemote> ret;
    ret.Reserve(snap->column_files.size());
    for (const auto & file : snap->column_files)
    {
        if (auto * cf_in_mem = file->tryToInMemoryFile(); cf_in_mem)
        {
            ret.Add(serializeTo(*cf_in_mem));
        }
        else if (auto * cf_tiny = file->tryToTinyFile(); cf_tiny)
        {
            ret.Add(serializeTo(*cf_tiny));
        }
        else if (auto * cf_delete_range = file->tryToDeleteRange(); cf_delete_range)
        {
            ret.Add(serializeTo(*cf_delete_range));
        }
        else if (auto * cf_big = file->tryToBigFile(); cf_big)
        {
            ret.Add(serializeTo(*cf_big));
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown ColumnFile, type={}", magic_enum::enum_name(file->getType()));
        }
    }
    return ret;
}

ColumnFileSetSnapshotPtr
Serializer::deserializeColumnFileSet(
    const google::protobuf::RepeatedPtrField<RemotePb::ColumnFileRemote> & proto,
    StoreID remote_store_id,
    TableID table_id,
    const RowKeyRange & segment_range)
{
    UNUSED(proto, remote_store_id, table_id, segment_range);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "");
}

RemotePb::ColumnFileRemote Serializer::serializeTo(const ColumnFileInMemory & cf_in_mem)
{
    RemotePb::ColumnFileRemote ret;
    auto * remote_in_memory = ret.mutable_in_memory();
    {
        auto wb = WriteBufferFromString(*remote_in_memory->mutable_schema());
        serializeSchema(wb, cf_in_mem.getSchema()->getSchema());
    }
    const auto block_rows = cf_in_mem.cache->block.rows();
    for (const auto & col : cf_in_mem.cache->block)
    {
        String buf;
        {
            auto wb = WriteBufferFromString(buf);
            // the function is defined in ColumnFilePersisted.h
            serializeColumn(
                wb,
                *col.column,
                col.type,
                0,
                block_rows,
                CompressionMethod::LZ4,
                CompressionSettings::getDefaultLevel(CompressionMethod::LZ4));
        }
        remote_in_memory->add_block_columns(std::move(buf));
    }
    remote_in_memory->set_rows(block_rows);

    return ret;
}

ColumnFileInMemoryPtr Serializer::deserializeCFInMemory(const RemotePb::ColumnFileInMemory & proto)
{
    UNUSED(proto);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "");
}

RemotePb::ColumnFileRemote Serializer::serializeTo(const ColumnFileTiny & cf_tiny)
{
    RemotePb::ColumnFileRemote ret;
    auto * remote_tiny = ret.mutable_tiny();
    remote_tiny->set_page_id(cf_tiny.data_page_id);
    {
        auto wb = WriteBufferFromString(*remote_tiny->mutable_schema());
        serializeSchema(wb, cf_tiny.schema->getSchema()); // defined in ColumnFilePersisted.h
    }
    remote_tiny->set_rows(cf_tiny.rows);
    remote_tiny->set_bytes(cf_tiny.bytes);

    return ret;
}

ColumnFileTinyPtr Serializer::deserializeCFTiny(const RemotePb::ColumnFileTiny & proto)
{
    UNUSED(proto);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "");
}

RemotePb::ColumnFileRemote Serializer::serializeTo(const ColumnFileDeleteRange & cf_delete_range)
{
    RemotePb::ColumnFileRemote ret;
    auto * remote_del = ret.mutable_delete_range();
    {
        WriteBufferFromString wb(*remote_del->mutable_key_range());
        cf_delete_range.delete_range.serialize(wb);
    }
    return ret;
}

ColumnFileDeleteRangePtr Serializer::deserializeCFDeleteRange(const RemotePb::ColumnFileDeleteRange & proto)
{
    ReadBufferFromString rb(proto.key_range());
    auto range = RowKeyRange::deserialize(rb);

    LOG_DEBUG(Logger::get(), "Rebuild local ColumnFileDeleteRange from remote, range={}", range.toDebugString());

    return std::make_shared<ColumnFileDeleteRange>(range);
}

RemotePb::ColumnFileRemote Serializer::serializeTo(const ColumnFileBig & cf_big)
{
    RemotePb::ColumnFileRemote ret;
    auto * remote_big = ret.mutable_big();
    remote_big->set_file_id(cf_big.file->fileId());
    remote_big->set_page_id(cf_big.file->pageId());
    remote_big->set_valid_rows(cf_big.valid_rows);
    remote_big->set_valid_bytes(cf_big.valid_bytes);
    return ret;
}

ColumnFileBigPtr Serializer::deserializeCFBig(const RemotePb::ColumnFileBig & proto)
{
    UNUSED(proto);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "");
}

} // namespace DB::DM::Remote
