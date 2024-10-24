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

#include <Common/StringUtils/StringUtils.h>
#include <IO/Compression/CompressionSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFile.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileDataProvider.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/DeltaMerge/Remote/RNDeltaIndexCache.h>
#include <Storages/DeltaMerge/Remote/Serializer.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Page/PageDefinesBase.h>

#include <magic_enum.hpp>

using google::protobuf::RepeatedPtrField;

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfDisaggReadNodeRead;
}

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace DB::ErrorCodes

namespace DB::DM::Remote
{
RemotePb::RemotePhysicalTable Serializer::serializePhysicalTable(
    const DisaggPhysicalTableReadSnapshotPtr & snap,
    const DisaggTaskId & task_id,
    MemTrackerWrapper & mem_tracker_wrapper,
    bool need_mem_data)
{
    std::shared_lock read_lock(snap->mtx);
    RemotePb::RemotePhysicalTable remote_table;
    remote_table.set_snapshot_id(task_id.toMeta().SerializeAsString());
    remote_table.set_keyspace_id(snap->ks_physical_table_id.first);
    remote_table.set_table_id(snap->ks_physical_table_id.second);
    remote_table.set_pk_col_id(snap->pk_col_id);
    for (const auto & [seg_id, seg_task] : snap->tasks)
    {
        auto remote_seg = Serializer::serializeSegment(
            seg_task->read_snapshot,
            seg_id,
            seg_task->segment->segmentEpoch(),
            seg_task->segment->getRowKeyRange(),
            /*read_ranges*/ seg_task->ranges,
            mem_tracker_wrapper,
            need_mem_data);
        remote_table.mutable_segments()->Add(std::move(remote_seg));
    }
    return remote_table;
}

RemotePb::RemoteSegment Serializer::serializeSegment(
    const SegmentSnapshotPtr & snap,
    PageIdU64 segment_id,
    UInt64 segment_epoch,
    const RowKeyRange & segment_range,
    const RowKeyRanges & read_ranges,
    MemTrackerWrapper & mem_tracker_wrapper,
    bool need_mem_data)
{
    RemotePb::RemoteSegment remote;
    remote.set_segment_id(segment_id);
    remote.set_segment_epoch(segment_epoch);
    remote.set_delta_index_epoch(snap->delta->getDeltaIndexEpoch());

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
        remote_file->set_meta_version(dt_file->metaVersion());
        auto * checkpoint_info = remote_file->mutable_checkpoint_info();
#ifndef DBMS_PUBLIC_GTEST // Don't not check path in unittests.
        RUNTIME_CHECK(startsWith(dt_file->path(), "s3://"), dt_file->path());
#endif
        checkpoint_info->set_data_file_id(dt_file->path()); // It should be a key to remote path
    }
    remote.mutable_column_files_memtable()->CopyFrom(
        serializeColumnFileSet(snap->delta->getMemTableSetSnapshot(), mem_tracker_wrapper, need_mem_data));
    remote.mutable_column_files_persisted()->CopyFrom(
        serializeColumnFileSet(snap->delta->getPersistedFileSetSnapshot(), mem_tracker_wrapper, true));

    // serialize the read ranges to read node
    for (const auto & read_range : read_ranges)
    {
        wb.restart();
        read_range.serialize(wb);
        remote.add_read_key_ranges()->assign(wb.releaseStr());
    }

    return remote;
}

SegmentSnapshotPtr Serializer::deserializeSegment(
    const DMContext & dm_context,
    StoreID remote_store_id,
    KeyspaceID keyspace_id,
    TableID table_id,
    const RemotePb::RemoteSegment & proto)
{
    RowKeyRange segment_range;
    {
        ReadBufferFromString rb(proto.key_range());
        segment_range = RowKeyRange::deserialize(rb);
    }

    auto data_store = dm_context.global_context.getSharedContextDisagg()->remote_data_store;

    auto delta_snap = std::make_shared<DeltaValueSnapshot>(CurrentMetrics::DT_SnapshotOfDisaggReadNodeRead, false);
    delta_snap->mem_table_snap
        = deserializeColumnFileSet(dm_context, proto.column_files_memtable(), data_store, segment_range);
    delta_snap->persisted_files_snap
        = deserializeColumnFileSet(dm_context, proto.column_files_persisted(), data_store, segment_range);

    // Note: At this moment, we still cannot read from `delta_snap->mem_table_snap` and `delta_snap->persisted_files_snap`,
    // because they are constructed using ColumnFileDataProviderNop.

    auto delta_index_cache = dm_context.global_context.getSharedContextDisagg()->rn_delta_index_cache;
    if (delta_index_cache)
    {
        delta_snap->shared_delta_index = delta_index_cache->getDeltaIndex({
            .store_id = remote_store_id,
            .keyspace_id = keyspace_id,
            .table_id = table_id,
            .segment_id = proto.segment_id(),
            .segment_epoch = proto.segment_epoch(),
            .delta_index_epoch = proto.delta_index_epoch(),
        });
    }
    else
    {
        delta_snap->shared_delta_index = std::make_shared<DeltaIndex>();
    }
    // Actually we will not access delta_snap->delta_index_epoch in read node. Just for completeness.
    delta_snap->delta_index_epoch = proto.delta_index_epoch();

    auto new_stable = std::make_shared<StableValueSpace>(/* id */ 0);
    DMFiles dmfiles;
    dmfiles.reserve(proto.stable_pages().size());
    for (const auto & stable_file : proto.stable_pages())
    {
        auto remote_key = stable_file.checkpoint_info().data_file_id();
        auto prepared = data_store->prepareDMFileByKey(remote_key);
        auto dmfile = prepared->restore(DMFileMeta::ReadMode::all(), stable_file.meta_version());
        RUNTIME_CHECK(dmfile != nullptr, remote_key);
        dmfiles.emplace_back(std::move(dmfile));
    }
    new_stable->setFiles(dmfiles, segment_range, &dm_context);
    auto stable_snap = new_stable->createSnapshot();

    return std::make_shared<SegmentSnapshot>(
        std::move(delta_snap),
        std::move(stable_snap),
        Logger::get(dm_context.tracing_id));
}

RepeatedPtrField<RemotePb::ColumnFileRemote> Serializer::serializeColumnFileSet(
    const ColumnFileSetSnapshotPtr & snap,
    MemTrackerWrapper & mem_tracker_wrapper,
    bool need_mem_data)
{
    RepeatedPtrField<RemotePb::ColumnFileRemote> ret;
    ret.Reserve(snap->column_files.size());
    for (const auto & file : snap->column_files)
    {
        ret.Add(serializeCF(file, snap->getDataProvider(), need_mem_data));
        mem_tracker_wrapper.alloc(ret.rbegin()->SpaceUsedLong());
    }
    return ret;
}

RemotePb::ColumnFileRemote Serializer::serializeCF(
    const ColumnFilePtr & cf,
    const IColumnFileDataProviderPtr & data_provider,
    bool need_mem_data)
{
    if (auto * cf_in_mem = cf->tryToInMemoryFile(); cf_in_mem)
    {
        return serializeCFInMemory(*cf_in_mem, need_mem_data);
    }
    else if (auto * cf_tiny = cf->tryToTinyFile(); cf_tiny)
    {
        return serializeCFTiny(*cf_tiny, data_provider);
    }
    else if (auto * cf_delete_range = cf->tryToDeleteRange(); cf_delete_range)
    {
        return serializeCFDeleteRange(*cf_delete_range);
    }
    else if (auto * cf_big = cf->tryToBigFile(); cf_big)
    {
        return serializeCFBig(*cf_big);
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown ColumnFile, type={}", static_cast<UInt32>(cf->getType()));
}

ColumnFileSetSnapshotPtr Serializer::deserializeColumnFileSet(
    const DMContext & dm_context,
    const RepeatedPtrField<RemotePb::ColumnFileRemote> & proto,
    const Remote::IDataStorePtr & data_store,
    const RowKeyRange & segment_range)
{
    auto empty_data_provider = std::make_shared<ColumnFileDataProviderNop>();
    auto ret = std::make_shared<ColumnFileSetSnapshot>(empty_data_provider);
    ret->column_files.reserve(proto.size());
    for (const auto & remote_column_file : proto)
    {
        if (remote_column_file.has_tiny())
        {
            ret->column_files.push_back(deserializeCFTiny(dm_context, remote_column_file.tiny()));
        }
        else if (remote_column_file.has_delete_range())
        {
            ret->column_files.push_back(deserializeCFDeleteRange(remote_column_file.delete_range()));
        }
        else if (remote_column_file.has_big())
        {
            const auto & big_file = remote_column_file.big();
            ret->column_files.push_back(deserializeCFBig(big_file, data_store, segment_range));
        }
        else if (remote_column_file.has_in_memory())
        {
            ret->column_files.push_back(deserializeCFInMemory(remote_column_file.in_memory()));
        }
        else
        {
            RUNTIME_CHECK_MSG(false, "Unexpected proto ColumnFile");
        }
    }
    for (const auto & column_file : ret->column_files)
    {
        ret->rows += column_file->getRows();
        ret->bytes += column_file->getBytes();
        ret->deletes += column_file->getDeletes();
    }
    return ret;
}

RemotePb::ColumnFileRemote Serializer::serializeCFInMemory(const ColumnFileInMemory & cf_in_mem, bool need_mem_data)
{
    RemotePb::ColumnFileRemote ret;
    auto * remote_in_memory = ret.mutable_in_memory();
    if (!need_mem_data)
    {
        std::scoped_lock lock(cf_in_mem.cache->mutex);
        remote_in_memory->set_rows(cf_in_mem.cache->block.rows());
        return ret;
    }

    {
        auto wb = WriteBufferFromString(*remote_in_memory->mutable_schema());
        serializeSchema(wb, cf_in_mem.getSchema()->getSchema());
    }
    std::scoped_lock lock(cf_in_mem.cache->mutex);
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
                CompressionSetting::getDefaultLevel(CompressionMethod::LZ4));
        }
        remote_in_memory->add_block_columns(std::move(buf));
    }
    remote_in_memory->set_rows(block_rows);

    return ret;
}

ColumnFileInMemoryPtr Serializer::deserializeCFInMemory(const RemotePb::ColumnFileInMemory & proto)
{
    LOG_DEBUG(Logger::get(), "Rebuild local ColumnFileInMemory from remote, rows={}", proto.rows());

    if (proto.block_columns().empty())
    {
        return std::make_shared<ColumnFileInMemory>(proto.rows());
    }

    BlockPtr block_schema;
    {
        auto read_buf = ReadBufferFromString(proto.schema());
        block_schema = deserializeSchema(read_buf);
    }

    auto columns = block_schema->cloneEmptyColumns();
    RUNTIME_CHECK(static_cast<int>(columns.size()) == proto.block_columns().size());

    for (size_t index = 0; index < block_schema->columns(); ++index)
    {
        const auto & data = proto.block_columns()[index];
        const auto data_buf = std::string_view(data.data(), data.size());
        const auto & type = block_schema->getByPosition(index).type;
        auto & column = columns[index];
        deserializeColumn(*column, type, data_buf, proto.rows());
    }

    auto block = block_schema->cloneWithColumns(std::move(columns));
    auto cache = std::make_shared<ColumnFile::Cache>(std::move(block));

    // We do not try to reuse the CFSchema from `SharedBlockSchemas`, because the ColumnFile will be freed immediately after the request.
    auto schema = std::make_shared<ColumnFileSchema>(*block_schema);
    return std::make_shared<ColumnFileInMemory>(schema, cache);
}

RemotePb::ColumnFileRemote Serializer::serializeCFTiny(
    const ColumnFileTiny & cf_tiny,
    IColumnFileDataProviderPtr data_provider)
{
    RemotePb::ColumnFileRemote ret;
    auto * remote_tiny = ret.mutable_tiny();
    remote_tiny->set_page_id(cf_tiny.data_page_id);
    // Note: We cannot use cf_tiny.data_page_size, because it is only available after restored.
    remote_tiny->set_page_size(data_provider->getTinyDataSize(cf_tiny.data_page_id));
    {
        auto wb = WriteBufferFromString(*remote_tiny->mutable_schema());
        serializeSchema(wb, cf_tiny.schema->getSchema()); // defined in ColumnFilePersisted.h
    }
    remote_tiny->set_rows(cf_tiny.rows);
    remote_tiny->set_bytes(cf_tiny.bytes);

    if (!cf_tiny.index_infos)
        return ret;

    for (const auto & index_info : *cf_tiny.index_infos)
    {
        auto * index_pb = remote_tiny->add_indexes();
        index_pb->set_index_page_id(index_info.index_page_id);
        if (index_info.vector_index.has_value())
        {
            RemotePb::VectorIndexFileProps index_props;
            index_props.set_index_kind(index_info.vector_index->index_kind());
            index_props.set_distance_metric(index_info.vector_index->distance_metric());
            index_props.set_dimensions(index_info.vector_index->dimensions());
            index_props.set_index_id(index_info.vector_index->index_id());
            index_props.set_index_bytes(index_info.vector_index->index_bytes());
            index_pb->mutable_vector_index()->Swap(&index_props);
        }
    }

    // TODO: read the checkpoint info from data_provider and send it to the compute node

    return ret;
}

ColumnFileTinyPtr Serializer::deserializeCFTiny(const DMContext & dm_context, const RemotePb::ColumnFileTiny & proto)
{
    BlockPtr block_schema;
    {
        auto read_buf = ReadBufferFromString(proto.schema());
        block_schema = deserializeSchema(read_buf);
    }

    // We do not try to reuse the CFSchema from `SharedBlockSchemas`, because the ColumnFile will be freed immediately after the request.
    auto schema = std::make_shared<ColumnFileSchema>(*block_schema);
    auto index_infos = std::make_shared<ColumnFileTiny::IndexInfos>();
    index_infos->reserve(proto.indexes().size());
    for (const auto & index_pb : proto.indexes())
    {
        if (index_pb.has_vector_index())
        {
            dtpb::VectorIndexFileProps index_props;
            index_props.set_index_kind(index_pb.vector_index().index_kind());
            index_props.set_distance_metric(index_pb.vector_index().distance_metric());
            index_props.set_dimensions(index_pb.vector_index().dimensions());
            index_props.set_index_id(index_pb.vector_index().index_id());
            index_props.set_index_bytes(index_pb.vector_index().index_bytes());
            index_infos->emplace_back(index_pb.index_page_id(), index_props);
        }
        else
            index_infos->emplace_back(index_pb.index_page_id(), std::nullopt);
    }

    auto cf = std::make_shared<ColumnFileTiny>(
        schema,
        proto.rows(),
        proto.bytes(),
        proto.page_id(),
        dm_context,
        index_infos);
    cf->data_page_size = proto.page_size();
    return cf;
}

RemotePb::ColumnFileRemote Serializer::serializeCFDeleteRange(const ColumnFileDeleteRange & cf_delete_range)
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

RemotePb::ColumnFileRemote Serializer::serializeCFBig(const ColumnFileBig & cf_big)
{
    RemotePb::ColumnFileRemote ret;
    auto * remote_big = ret.mutable_big();
    auto * checkpoint_info = remote_big->mutable_checkpoint_info();
    checkpoint_info->set_data_file_id(cf_big.file->path());
    remote_big->set_page_id(cf_big.file->pageId());
    remote_big->set_meta_version(cf_big.file->metaVersion());
    remote_big->set_valid_rows(cf_big.valid_rows);
    remote_big->set_valid_bytes(cf_big.valid_bytes);
    return ret;
}

ColumnFileBigPtr Serializer::deserializeCFBig(
    const RemotePb::ColumnFileBig & proto,
    const Remote::IDataStorePtr & data_store,
    const RowKeyRange & segment_range)
{
    RUNTIME_CHECK(proto.has_checkpoint_info());
    LOG_DEBUG(Logger::get(), "Rebuild local ColumnFileBig from remote, key={}", proto.checkpoint_info().data_file_id());
    auto prepared = data_store->prepareDMFileByKey(proto.checkpoint_info().data_file_id());
    auto dmfile = prepared->restore(DMFileMeta::ReadMode::all(), proto.meta_version());
    auto * cf_big = new ColumnFileBig(dmfile, proto.valid_rows(), proto.valid_bytes(), segment_range);
    return std::shared_ptr<ColumnFileBig>(cf_big); // The constructor is private, so we cannot use make_shared.
}

} // namespace DB::DM::Remote
