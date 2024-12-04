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

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/EmptyBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Poco/Logger.h>
#include <Storages/DeltaMerge/BitmapFilter/BitmapFilterBlockInputStream.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetWithVectorIndexInputStream.h>
#include <Storages/DeltaMerge/ConcatSkippableBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMDecoratorStreams.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/DeltaMerge.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaPlace.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/DeltaMerge/LateMaterializationBlockInputStream.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <Storages/DeltaMerge/Remote/RNDeltaIndexCache.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
#include <Storages/DeltaMerge/Segment_fwd.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/DeltaMerge/dtpb/segment.pb.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/Disagg/FastAddPeerCache.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Utils/AsyncTasks.h>
#include <Storages/Page/V3/PageEntryCheckpointInfo.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/PathPool.h>
#include <Storages/S3/S3Filename.h>
#include <common/logger_useful.h>
#include <fiu.h>
#include <fmt/core.h>

#include <ext/scope_guard.h>
#include <memory>


namespace ProfileEvents
{
extern const Event DMWriteBlock;
extern const Event DMWriteBlockNS;
extern const Event DMPlace;
extern const Event DMPlaceNS;
extern const Event DMPlaceUpsert;
extern const Event DMPlaceUpsertNS;
extern const Event DMPlaceDeleteRange;
extern const Event DMPlaceDeleteRangeNS;
extern const Event DMAppendDeltaPrepare;
extern const Event DMAppendDeltaPrepareNS;
extern const Event DMAppendDeltaCommitMemory;
extern const Event DMAppendDeltaCommitMemoryNS;
extern const Event DMAppendDeltaCommitDisk;
extern const Event DMAppendDeltaCommitDiskNS;
extern const Event DMAppendDeltaCleanUp;
extern const Event DMAppendDeltaCleanUpNS;
extern const Event DMSegmentSplit;
extern const Event DMSegmentSplitNS;
extern const Event DMSegmentGetSplitPoint;
extern const Event DMSegmentGetSplitPointNS;
extern const Event DMSegmentMerge;
extern const Event DMSegmentMergeNS;
extern const Event DMDeltaMerge;
extern const Event DMDeltaMergeNS;
extern const Event DMSegmentIsEmptyFastPath;
extern const Event DMSegmentIsEmptySlowPath;
extern const Event DMSegmentIngestDataByReplace;
extern const Event DMSegmentIngestDataIntoDelta;

} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric DT_DeltaCompact;
extern const Metric DT_DeltaFlush;
extern const Metric DT_PlaceIndexUpdate;
extern const Metric DT_SnapshotOfRead;
extern const Metric DT_SnapshotOfReadRaw;
extern const Metric DT_SnapshotOfSegmentSplit;
extern const Metric DT_SnapshotOfSegmentMerge;
extern const Metric DT_SnapshotOfDeltaMerge;
extern const Metric DT_SnapshotOfPlaceIndex;
extern const Metric DT_SnapshotOfSegmentIngest;
extern const Metric DT_SnapshotOfBitmapFilter;
} // namespace CurrentMetrics

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes
namespace FailPoints
{
extern const char pause_when_building_fap_segments[];
} // namespace FailPoints
namespace DM
{
String SegmentSnapshot::detailInfo() const
{
    return fmt::format(
        "{{"
        "stable_rows={} "
        "persisted_rows={} persisted_dels={} persisted_cfs={} "
        "mem_rows={} mem_dels={} mem_cfs={}"
        "}}",
        stable->getRows(),
        delta->getPersistedFileSetSnapshot()->getRows(),
        delta->getPersistedFileSetSnapshot()->getDeletes(),
        delta->getPersistedFileSetSnapshot()->getColumnFileCount(),
        delta->getMemTableSetSnapshot()->getRows(),
        delta->getMemTableSetSnapshot()->getDeletes(),
        delta->getMemTableSetSnapshot()->getColumnFileCount());
}

const static size_t SEGMENT_BUFFER_SIZE = 128; // More than enough.

DMFilePtr writeIntoNewDMFile(
    DMContext & dm_context, //
    const ColumnDefinesPtr & schema_snap,
    const BlockInputStreamPtr & input_stream,
    UInt64 file_id,
    const String & parent_path)
{
    auto dmfile = DMFile::create(
        file_id,
        parent_path,
        dm_context.createChecksumConfig(),
        dm_context.global_context.getSettingsRef().dt_small_file_size_threshold,
        dm_context.global_context.getSettingsRef().dt_merged_file_max_size,
        dm_context.keyspace_id);
    auto output_stream = std::make_shared<DMFileBlockOutputStream>(dm_context.global_context, dmfile, *schema_snap);
    const auto * mvcc_stream
        = typeid_cast<const DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT> *>(input_stream.get());

    input_stream->readPrefix();
    output_stream->writePrefix();
    while (true)
    {
        size_t last_effective_num_rows = 0;
        size_t last_not_clean_rows = 0;
        size_t last_deleted_rows = 0;
        if (mvcc_stream)
        {
            last_effective_num_rows = mvcc_stream->getEffectiveNumRows();
            last_not_clean_rows = mvcc_stream->getNotCleanRows();
            last_deleted_rows = mvcc_stream->getDeletedRows();
        }
        Block block = input_stream->read();
        if (!block)
            break;
        if (!block.rows())
            continue;

        // When the input_stream is not mvcc, we assume the rows in this input_stream is most valid and make it not tend to be gc.
        size_t cur_effective_num_rows = block.rows();
        size_t cur_not_clean_rows = 1;
        // If the stream is not mvcc_stream, it will not calculate the deleted_rows.
        // Thus we set it to 1 to ensure when read this block will not use related optimization.
        size_t cur_deleted_rows = 1;
        size_t gc_hint_version = std::numeric_limits<UInt64>::max();
        if (mvcc_stream)
        {
            cur_effective_num_rows = mvcc_stream->getEffectiveNumRows();
            cur_not_clean_rows = mvcc_stream->getNotCleanRows();
            cur_deleted_rows = mvcc_stream->getDeletedRows();
            gc_hint_version = mvcc_stream->getGCHintVersion();
        }

        DMFileBlockOutputStream::BlockProperty block_property;
        block_property.effective_num_rows = cur_effective_num_rows - last_effective_num_rows;
        block_property.not_clean_rows = cur_not_clean_rows - last_not_clean_rows;
        block_property.deleted_rows = cur_deleted_rows - last_deleted_rows;
        block_property.gc_hint_version = gc_hint_version;
        output_stream->write(block, block_property);
    }

    input_stream->readSuffix();
    output_stream->writeSuffix();

    return dmfile;
}

// Create a new stable, the DMFile will write as External Page to disk, but the meta will not be written to disk.
// The caller should write the meta to disk if needed.
StableValueSpacePtr createNewStable( //
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const BlockInputStreamPtr & input_stream,
    PageIdU64 stable_id,
    WriteBatches & wbs)
{
    auto delegator = dm_context.path_pool->getStableDiskDelegator();
    auto store_path = delegator.choosePath();

    PageIdU64 dtfile_id = dm_context.storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    DMFilePtr dtfile;
    try
    {
        dtfile = writeIntoNewDMFile(dm_context, schema_snap, input_stream, dtfile_id, store_path);

        auto stable = std::make_shared<StableValueSpace>(stable_id);
        stable->setFiles({dtfile}, RowKeyRange::newAll(dm_context.is_common_handle, dm_context.rowkey_column_size));
        if (auto data_store = dm_context.global_context.getSharedContextDisagg()->remote_data_store; !data_store)
        {
            wbs.data.putExternal(dtfile_id, 0);
            delegator.addDTFile(dtfile_id, dtfile->getBytesOnDisk(), store_path);
        }
        else
        {
            auto store_id = dm_context.global_context.getTMTContext().getKVStore()->getStoreID();
            Remote::DMFileOID oid{
                .store_id = store_id,
                .keyspace_id = dm_context.keyspace_id,
                .table_id = dm_context.physical_table_id,
                .file_id = dtfile_id,
            };
            data_store->putDMFile(dtfile, oid, /*switch_to_remote*/ true);
            PS::V3::CheckpointLocation loc{
                .data_file_id = std::make_shared<String>(S3::S3Filename::fromDMFileOID(oid).toFullKey()),
                .offset_in_file = 0,
                .size_in_file = 0,
            };
            delegator.addRemoteDTFileWithGCDisabled(dtfile_id, dtfile->getBytesOnDisk());
            wbs.data.putRemoteExternal(dtfile_id, loc);
        }
        return stable;
    }
    catch (...)
    {
        if (dtfile)
        {
            dtfile->remove(dm_context.global_context.getFileProvider());
        }
        throw;
    }
}

//==========================================================================================
// Segment ser/deser
//==========================================================================================

Segment::Segment( //
    const LoggerPtr & parent_log_,
    UInt64 epoch_,
    const RowKeyRange & rowkey_range_,
    PageIdU64 segment_id_,
    PageIdU64 next_segment_id_,
    const DeltaValueSpacePtr & delta_,
    const StableValueSpacePtr & stable_)
    : epoch(epoch_)
    , rowkey_range(rowkey_range_)
    , is_common_handle(rowkey_range.is_common_handle)
    , rowkey_column_size(rowkey_range.rowkey_column_size)
    , segment_id(segment_id_)
    , next_segment_id(next_segment_id_)
    , delta(delta_)
    , stable(stable_)
    , parent_log(parent_log_)
    , log(parent_log_->getChild(fmt::format("segment_id={} epoch={}", segment_id, epoch)))
{
    if (delta != nullptr)
        delta->resetLogger(log);
    if (stable != nullptr)
        stable->resetLogger(log);
}

SegmentPtr Segment::newSegment( //
    const LoggerPtr & parent_log,
    DMContext & context,
    const ColumnDefinesPtr & schema,
    const RowKeyRange & range,
    PageIdU64 segment_id,
    PageIdU64 next_segment_id,
    PageIdU64 delta_id,
    PageIdU64 stable_id)
{
    WriteBatches wbs(*context.storage_pool, context.getWriteLimiter());

    auto delta = std::make_shared<DeltaValueSpace>(delta_id);
    auto stable
        = createNewStable(context, schema, std::make_shared<EmptySkippableBlockInputStream>(*schema), stable_id, wbs);

    auto segment
        = std::make_shared<Segment>(parent_log, INITIAL_EPOCH, range, segment_id, next_segment_id, delta, stable);

    // Write metadata.
    delta->saveMeta(wbs);
    stable->saveMeta(wbs.meta);
    segment->serialize(wbs.meta);

    wbs.writeAll();
    stable->enableDMFilesGC(context);

    return segment;
}

SegmentPtr Segment::newSegment( //
    const LoggerPtr & parent_log,
    DMContext & context,
    const ColumnDefinesPtr & schema,
    const RowKeyRange & rowkey_range,
    PageIdU64 segment_id,
    PageIdU64 next_segment_id)
{
    return newSegment(
        parent_log,
        context,
        schema,
        rowkey_range,
        segment_id,
        next_segment_id,
        context.storage_pool->newMetaPageId(),
        context.storage_pool->newMetaPageId());
}

void readSegmentMetaInfo(ReadBuffer & buf, Segment::SegmentMetaInfo & segment_info)
{
    readIntBinary(segment_info.version, buf);
    readIntBinary(segment_info.epoch, buf);

    switch (segment_info.version)
    {
    case SegmentFormat::V1:
    {
        HandleRange range;
        readIntBinary(range.start, buf);
        readIntBinary(range.end, buf);
        segment_info.range = RowKeyRange::fromHandleRange(range);
        readIntBinary(segment_info.next_segment_id, buf);
        readIntBinary(segment_info.delta_id, buf);
        readIntBinary(segment_info.stable_id, buf);
        break;
    }
    case SegmentFormat::V2:
    {
        segment_info.range = RowKeyRange::deserialize(buf);
        readIntBinary(segment_info.next_segment_id, buf);
        readIntBinary(segment_info.delta_id, buf);
        readIntBinary(segment_info.stable_id, buf);
        break;
    }
    case SegmentFormat::V3:
    {
        dtpb::SegmentMeta meta;
        String data;
        readStringBinary(data, buf);
        RUNTIME_CHECK_MSG(
            meta.ParseFromString(data),
            "Failed to parse SegmentMeta from string: {}",
            Redact::keyToHexString(data.data(), data.size()));
        segment_info.range = RowKeyRange::deserialize(meta.range());
        segment_info.next_segment_id = meta.next_segment_id();
        segment_info.delta_id = meta.delta_id();
        segment_info.stable_id = meta.stable_id();
        break;
    }
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal version: {}", segment_info.version);
    }
}

std::vector<PageIdU64> Segment::getAllSegmentIds(const DMContext & context, PageIdU64 segment_id)
{
    std::vector<PageIdU64> segment_ids = {};
    PageIdU64 current_segment_id = segment_id;
    while (current_segment_id != 0)
    {
        segment_ids.push_back(current_segment_id);
        Page page = context.storage_pool->metaReader()->read(current_segment_id); // not limit restore

        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        Segment::SegmentMetaInfo segment_info;
        readSegmentMetaInfo(buf, segment_info);
        current_segment_id = segment_info.next_segment_id;
    }
    return segment_ids;
}

SegmentPtr Segment::restoreSegment( //
    const LoggerPtr & parent_log,
    DMContext & context,
    PageIdU64 segment_id)
{
    Segment::SegmentMetaInfo segment_info;
    try
    {
        Page page = context.storage_pool->metaReader()->read(segment_id); // not limit restore

        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        readSegmentMetaInfo(buf, segment_info);

        auto delta = DeltaValueSpace::restore(context, segment_info.range, segment_info.delta_id);
        auto stable = StableValueSpace::restore(context, segment_info.stable_id);
        auto segment = std::make_shared<Segment>(
            parent_log,
            segment_info.epoch,
            segment_info.range,
            segment_id,
            segment_info.next_segment_id,
            delta,
            stable);

        return segment;
    }
    catch (DB::Exception & e)
    {
        e.addMessage(fmt::format("while restoreSegment, segment_id={}", segment_id));
        e.rethrow();
    }
    RUNTIME_CHECK_MSG(false, "unreachable");
    return {};
}

Segment::SegmentMetaInfos Segment::readAllSegmentsMetaInfoInRange( //
    DMContext & context,
    const std::shared_ptr<GeneralCancelHandle> & cancel_handle,
    const RowKeyRange & target_range,
    const CheckpointInfoPtr & checkpoint_info)
{
    RUNTIME_CHECK(checkpoint_info != nullptr);
    auto log = DB::Logger::get(fmt::format(
        "region_id={} keyspace={} table_id={}",
        checkpoint_info->region_id,
        context.keyspace_id,
        context.physical_table_id));
    Stopwatch sw;
    SCOPE_EXIT(
        { GET_METRIC(tiflash_fap_task_duration_seconds, type_write_stage_read_segment).Observe(sw.elapsedSeconds()); });

    auto end_to_segment_id_cache = checkpoint_info->checkpoint_data_holder->getEndToSegmentIdCache(
        KeyspaceTableID{context.keyspace_id, context.physical_table_id});

    // Protected by whatever lock.
    auto build_segments = [&](bool is_cache_ready, PageIdU64 current_segment_id)
        -> std::optional<std::pair<std::vector<std::pair<DM::RowKeyValue, UInt64>>, SegmentMetaInfos>> {
        // We have a cache that records all segments which map to a certain table identified by (keyspace_id, physical_table_id).
        // We can thus avoid reading from the very beginning for every different regions in this table.
        // If cache is empty, we read from DELTA_MERGE_FIRST_SEGMENT_ID to the end and build the cache.
        // Otherwise, we just read the segment that cover the range.
        LOG_DEBUG(log, "Read segment meta info, segment_id={}", current_segment_id);

        // The map is used to build cache.
        std::vector<std::pair<DM::RowKeyValue, UInt64>> end_key_and_segment_ids;
        SegmentMetaInfos segment_infos;
        while (current_segment_id != 0)
        {
            if (cancel_handle->isCanceled())
            {
                LOG_INFO(log, "FAP is canceled when building segments, built={}", end_key_and_segment_ids.size());
                // FAP task would be cleaned in FastAddPeerImplWrite. So returning empty result is OK.
                return std::nullopt;
            }
            Segment::SegmentMetaInfo segment_info;
            auto target_id = UniversalPageIdFormat::toFullPageId(
                UniversalPageIdFormat::toFullPrefix(context.keyspace_id, StorageType::Meta, context.physical_table_id),
                current_segment_id);
            auto page = checkpoint_info->temp_ps->read(target_id, nullptr, {}, false);
            if unlikely (!page.isValid())
            {
                // After #7642, DELTA_MERGE_FIRST_SEGMENT_ID may not exist, however, such checkpoint won't be selected.
                // If it were to be selected, the FAP task could fallback to regular snapshot.
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Can't find page id {}, current_segment_id={} range={}",
                    target_id,
                    current_segment_id,
                    target_range.toDebugString());
            }
            segment_info.segment_id = current_segment_id;
            ReadBufferFromMemory buf(page.data.begin(), page.data.size());
            readSegmentMetaInfo(buf, segment_info);
            if (!is_cache_ready)
            {
                end_key_and_segment_ids.emplace_back(
                    segment_info.range.getEnd().toRowKeyValue(),
                    segment_info.segment_id);
            }
            current_segment_id = segment_info.next_segment_id;
            if (!(segment_info.range.shrink(target_range).none()))
            {
                segment_infos.emplace_back(segment_info);
            }
            // if not build cache, stop as early as possible.
            if (is_cache_ready && segment_info.range.end.value->compare(*target_range.end.value) >= 0)
            {
                break;
            }
        }
        return std::make_pair(end_key_and_segment_ids, segment_infos);
    };

    {
        // If there is a table building cache, then other table may block to read the built cache.
        // If the remote reader causes much time to retrieve data, then these tasks could block here.
        // However, when the execlusive holder is canceled due to timeout, the readers could eventually get the lock.
        auto lock = end_to_segment_id_cache->writeLock();
        // - Set to `true`: The building task is done.
        // - Set to `false`: It is not build yet, or it is building.
        bool is_cache_ready = end_to_segment_id_cache->isReady(lock);
        GET_METRIC(tiflash_fap_task_duration_seconds, type_write_stage_wait_build)
            .Observe(sw.elapsedSecondsFromLastTime());

        if (!is_cache_ready)
        {
            // We are the cache builder.
            FAIL_POINT_PAUSE(FailPoints::pause_when_building_fap_segments);

            auto res = build_segments(is_cache_ready, DELTA_MERGE_FIRST_SEGMENT_ID);
            // After all segments are scanned, we try to build a cache,
            // so other FAP tasks that share the same checkpoint could reuse the cache.
            if (!res)
                return {};
            auto & [end_key_and_segment_ids, segment_infos] = *res;
            LOG_DEBUG(log, "Segment meta info cache has been built, num_segments={}", end_key_and_segment_ids.size());
            end_to_segment_id_cache->build(lock, std::move(end_key_and_segment_ids));
            return std::move(segment_infos);
        }
    }
    {
        // If we found the cache is built, which could be normal cases when the checkpoint is reused.
        auto lock = end_to_segment_id_cache->readLock();
        bool is_cache_ready = end_to_segment_id_cache->isReady(lock);
        RUNTIME_CHECK(is_cache_ready, checkpoint_info->region_id, context.keyspace_id, context.physical_table_id);
        GET_METRIC(tiflash_fap_task_result, type_reuse_chkpt_cache).Increment();
        // ... then we could seek to `current_segment_id` in cache to avoid some read.
        auto current_segment_id
            = end_to_segment_id_cache->getSegmentIdContainingKey(lock, target_range.getStart().toRowKeyValue());
        auto res = build_segments(is_cache_ready, current_segment_id);
        if (!res)
            return {};
        return std::move(res->second);
    }

    if (cancel_handle->isCanceled())
    {
        LOG_INFO(log, "FAP is canceled when building segments");
        // FAP task would be cleaned in FastAddPeerImplWrite. So returning incompelete result could be OK.
        return {};
    }
}

Segments Segment::createTargetSegmentsFromCheckpoint( //
    const LoggerPtr & parent_log,
    UInt64 region_id,
    DMContext & context,
    StoreID remote_store_id,
    const SegmentMetaInfos & meta_infos,
    const RowKeyRange & range,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs)
{
    Segments segments;
    for (const auto & segment_info : meta_infos)
    {
        LOG_DEBUG(
            parent_log,
            "Create segment begin. delta_id={} stable_id={} range={} epoch={} next_segment_id={} remote_store_id={} "
            "region_id={}",
            segment_info.delta_id,
            segment_info.stable_id,
            segment_info.range.toDebugString(),
            segment_info.epoch,
            segment_info.next_segment_id,
            remote_store_id,
            region_id);
        auto stable = StableValueSpace::createFromCheckpoint(parent_log, context, temp_ps, segment_info.stable_id, wbs);
        auto delta = DeltaValueSpace::createFromCheckpoint(
            parent_log,
            context,
            temp_ps,
            segment_info.range,
            segment_info.delta_id,
            wbs);
        auto segment = std::make_shared<Segment>(
            Logger::get(fmt::format("Checkpoint(region_id={})", region_id)),
            segment_info.epoch,
            segment_info.range.shrink(range),
            segment_info.segment_id,
            segment_info.next_segment_id,
            delta,
            stable);
        segments.push_back(segment);
        LOG_DEBUG(
            parent_log,
            "Create segment end. delta_id={} stable_id={} range={} epoch={} next_segment_id={} remote_store_id={} "
            "region_id={}",
            segment_info.delta_id,
            segment_info.stable_id,
            segment_info.range.toDebugString(),
            segment_info.epoch,
            segment_info.next_segment_id,
            remote_store_id,
            region_id);
    }
    return segments;
}

void Segment::serializeToFAPTempSegment(FastAddPeerProto::FAPTempSegmentInfo * segment_info)
{
    {
        WriteBufferFromOwnString wb;
        storeSegmentMetaInfo(wb);
        segment_info->set_segment_meta(wb.releaseStr());
    }
    segment_info->set_delta_meta(delta->serializeMeta());
    segment_info->set_stable_meta(stable->serializeMeta());
}

UInt64 Segment::storeSegmentMetaInfo(WriteBuffer & buf) const
{
    writeIntBinary(STORAGE_FORMAT_CURRENT.segment, buf);
    writeIntBinary(epoch, buf);

    if (likely(
            STORAGE_FORMAT_CURRENT.segment == SegmentFormat::V1 //
            || STORAGE_FORMAT_CURRENT.segment == SegmentFormat::V2))
    {
        rowkey_range.serialize(buf);
        writeIntBinary(next_segment_id, buf);
        writeIntBinary(delta->getId(), buf);
        writeIntBinary(stable->getId(), buf);
    }
    else if (STORAGE_FORMAT_CURRENT.segment == SegmentFormat::V3)
    {
        dtpb::SegmentMeta meta;
        auto range = rowkey_range.serialize();
        meta.mutable_range()->Swap(&range);
        meta.set_next_segment_id(next_segment_id);
        meta.set_delta_id(delta->getId());
        meta.set_stable_id(stable->getId());

        auto data = meta.SerializeAsString();
        writeStringBinary(data, buf);
    }

    return buf.count();
}

void Segment::serialize(WriteBatchWrapper & wb) const
{
    MemoryWriteBuffer buf(0, SEGMENT_BUFFER_SIZE);
    // Must be called before tryGetReadBuffer.
    auto data_size = storeSegmentMetaInfo(buf);
    wb.putPage(segment_id, 0, buf.tryGetReadBuffer(), data_size);
}

bool Segment::writeToDisk(DMContext & dm_context, const ColumnFilePtr & column_file)
{
    LOG_TRACE(log, "Segment write to disk, rows={} isBigFile={}", column_file->getRows(), column_file->isBigFile());
    return delta->appendColumnFile(dm_context, column_file);
}

bool Segment::writeToCache(DMContext & dm_context, const Block & block, size_t offset, size_t limit)
{
    LOG_TRACE(log, "Segment write to cache, rows={}", limit);
    if (unlikely(limit == 0))
        return true;
    return delta->appendToCache(dm_context, block, offset, limit);
}

bool Segment::write(DMContext & dm_context, const Block & block, bool flush_cache)
{
    LOG_TRACE(log, "Segment write to disk, rows={}", block.rows());
    WriteBatches wbs(*dm_context.storage_pool, dm_context.getWriteLimiter());

    auto column_file = ColumnFileTiny::writeColumnFile(dm_context, block, 0, block.rows(), wbs);
    wbs.writeAll();

    if (delta->appendColumnFile(dm_context, column_file))
    {
        if (flush_cache)
        {
            while (!flushCache(dm_context))
            {
                if (hasAbandoned())
                    return false;
            }
        }
        return true;
    }
    else
    {
        return false;
    }
}

bool Segment::write(DMContext & dm_context, const RowKeyRange & delete_range)
{
    auto new_range = delete_range.shrink(rowkey_range);
    if (new_range.none())
    {
        LOG_WARNING(log, "Try to write an invalid delete range, delete_range={}", delete_range.toDebugString());
        return true;
    }

    LOG_TRACE(log, "Segment write delete range, delete_range={}", delete_range.toDebugString());
    return delta->appendDeleteRange(dm_context, delete_range);
}

bool Segment::isDefinitelyEmpty(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap) const
{
    RUNTIME_CHECK(segment_snap->isForUpdate());

    // Fast path: all packs has been filtered away
    if (segment_snap->getRows() == 0)
    {
        ProfileEvents::increment(ProfileEvents::DMSegmentIsEmptyFastPath);
        return true;
    }

    ProfileEvents::increment(ProfileEvents::DMSegmentIsEmptySlowPath);

    // Build a delta stream first, and try to read some data from it.
    // As long as we read out anything, we will stop.

    auto columns_to_read = std::make_shared<ColumnDefines>();
    columns_to_read->push_back(getExtraHandleColumnDefine(is_common_handle));
    auto read_ranges = RowKeyRanges{rowkey_range};

    {
        BlockInputStreamPtr delta_stream = std::make_shared<DeltaValueInputStream>(
            dm_context,
            segment_snap->delta,
            columns_to_read,
            rowkey_range,
            ReadTag::Internal);
        delta_stream = std::make_shared<DMRowKeyFilterBlockInputStream<false>>(delta_stream, read_ranges, 0);
        delta_stream->readPrefix();
        while (true)
        {
            Block block = delta_stream->read();
            if (!block)
                break;
            if (block.rows() > 0)
                // Note: Returning false here does not mean that there must be data in the snapshot,
                // because we are not considering the delete range.
                return false;
        }
        delta_stream->readSuffix();
    }

    // The delta stream is empty. Let's then try to read from stable.
    {
        SkippableBlockInputStreams streams;
        for (const auto & file : segment_snap->stable->getDMFiles())
        {
            DMFileBlockInputStreamBuilder builder(dm_context.global_context);
            auto stream = builder
                              .setRowsThreshold(
                                  std::numeric_limits<UInt64>::max()) // TODO: May be we could have some better settings
                              .onlyReadOnePackEveryTime()
                              .build(file, *columns_to_read, read_ranges, dm_context.scan_context);
            streams.push_back(stream);
        }

        BlockInputStreamPtr stable_stream
            = std::make_shared<ConcatSkippableBlockInputStream<>>(streams, dm_context.scan_context);
        stable_stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stable_stream, read_ranges, 0);
        stable_stream->readPrefix();
        while (true)
        {
            Block block = stable_stream->read();
            if (!block)
                break;
            if (block.rows() > 0)
                // Note: Returning false here does not mean that there must be data in the snapshot,
                // because we are not considering the delete range.
                return false;
        }
        stable_stream->readSuffix();
    }

    // We cannot read out anything from the delta stream and the stable stream,
    // so we know that the snapshot is definitely empty.

    return true;
}

bool Segment::ingestDataToDelta(
    DMContext & dm_context,
    const RowKeyRange & range,
    const DMFiles & data_files,
    bool clear_data_in_range)
{
    auto new_range = range.shrink(rowkey_range);
    LOG_TRACE(log, "Segment ingest data to delta, range={} clear={}", new_range.toDebugString(), clear_data_in_range);

    ColumnFiles column_files;
    column_files.reserve(data_files.size());
    for (const auto & data_file : data_files)
    {
        auto column_file = std::make_shared<ColumnFileBig>(dm_context, data_file, rowkey_range);
        if (column_file->getRows() != 0)
            column_files.emplace_back(std::move(column_file));
    }
    return delta->ingestColumnFiles(dm_context, range, column_files, clear_data_in_range);
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Segment::IngestDataInfo Segment::prepareIngestDataWithClearData() const
{
    return IngestDataInfo{
        .option_clear_data = true,
        .is_snapshot_empty = true,
        .snapshot = nullptr,
    };
}

Segment::IngestDataInfo Segment::prepareIngestDataWithPreserveData(
    DMContext & dm_context,
    const SegmentSnapshotPtr & segment_snap) const
{
    auto is_empty = isDefinitelyEmpty(dm_context, segment_snap);
    return IngestDataInfo{
        .option_clear_data = false,
        .is_snapshot_empty = is_empty,
        .snapshot = segment_snap,
    };
}

SegmentPtr Segment::applyIngestData(
    const Segment::Lock & lock,
    DMContext & dm_context,
    const DMFilePtr & data_file,
    const IngestDataInfo & prepared_info)
{
    if (hasAbandoned())
    {
        return nullptr;
    }

    // Fast path: if we don't want to preserve data in the segment, or the segment is empty,
    // we could just replace the segment with the specified data file.
    if (prepared_info.option_clear_data || prepared_info.is_snapshot_empty)
    {
        ProfileEvents::increment(ProfileEvents::DMSegmentIngestDataByReplace);
        auto new_seg = replaceData(lock, dm_context, data_file, prepared_info.snapshot);
        RUNTIME_CHECK(new_seg != nullptr); // replaceData never returns nullptr.
        return new_seg;
    }

    ProfileEvents::increment(ProfileEvents::DMSegmentIngestDataIntoDelta);
    // Slow path: the destination is not empty, and we want to preserve the destination data,
    // so we have to ingest the data into the delta layer and trigger a delta merge later.
    auto success = ingestDataToDelta(dm_context, rowkey_range, {data_file}, /* clear_data_in_range */ false);
    // ingest to delta should always success as long as segment is not abandoned.
    RUNTIME_CHECK(success);

    // Current segment is still valid.
    return shared_from_this();
}

SegmentPtr Segment::ingestDataForTest(DMContext & dm_context, const DMFilePtr & data_file, bool clear_data)
{
    IngestDataInfo ii;
    if (clear_data)
    {
        ii = prepareIngestDataWithClearData();
    }
    else
    {
        auto segment_snap = createSnapshot(dm_context, true, CurrentMetrics::DT_SnapshotOfSegmentIngest);
        if (!segment_snap)
            return nullptr;
        ii = prepareIngestDataWithPreserveData(dm_context, segment_snap);
    }

    auto segment_lock = mustGetUpdateLock();
    auto new_segment = applyIngestData(segment_lock, dm_context, data_file, ii);
    if (new_segment.get() != this)
    {
        RUNTIME_CHECK(getRowKeyRange().getEnd() == new_segment->getRowKeyRange().getEnd(), info(), new_segment->info());
        RUNTIME_CHECK(segmentId() == new_segment->segmentId(), info(), new_segment->info());
    }

    return new_segment;
}

SegmentSnapshotPtr Segment::createSnapshot(const DMContext & dm_context, bool for_update, CurrentMetrics::Metric metric)
    const
{
    Stopwatch watch;
    SCOPE_EXIT({ dm_context.scan_context->create_snapshot_time_ns += watch.elapsed(); });
    auto delta_snap = delta->createSnapshot(dm_context, for_update, metric);
    auto stable_snap = stable->createSnapshot();
    if (!delta_snap || !stable_snap)
        return {};

    dm_context.scan_context->delta_rows += delta_snap->getRows();
    dm_context.scan_context->delta_bytes += delta_snap->getBytes();
    return std::make_shared<SegmentSnapshot>(
        std::move(delta_snap),
        std::move(stable_snap),
        Logger::get(dm_context.tracing_id));
}

BlockInputStreamPtr Segment::getInputStream(
    const ReadMode & read_mode,
    const DMContext & dm_context,
    const ColumnDefines & columns_to_read,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & read_ranges,
    const PushDownFilterPtr & filter,
    UInt64 start_ts,
    size_t expected_block_size)
{
    Stopwatch sw;
    SCOPE_EXIT({ dm_context.scan_context->build_inputstream_time_ns += sw.elapsed(); });
    auto clipped_block_rows = clipBlockRows( //
        dm_context.global_context,
        expected_block_size,
        columns_to_read,
        segment_snap->stable->stable);
    switch (read_mode)
    {
    case ReadMode::Normal:
        return getInputStreamModeNormal(
            dm_context,
            columns_to_read,
            segment_snap,
            read_ranges,
            filter ? filter->rs_operator : EMPTY_RS_OPERATOR,
            start_ts,
            clipped_block_rows);
    case ReadMode::Fast:
        return getInputStreamModeFast(
            dm_context,
            columns_to_read,
            segment_snap,
            read_ranges,
            filter ? filter->rs_operator : EMPTY_RS_OPERATOR,
            clipped_block_rows);
    case ReadMode::Raw:
        return getInputStreamModeRaw( //
            dm_context,
            columns_to_read,
            segment_snap,
            read_ranges,
            clipped_block_rows);
    case ReadMode::Bitmap:
        return getBitmapFilterInputStream(
            dm_context,
            columns_to_read,
            segment_snap,
            read_ranges,
            filter,
            start_ts,
            expected_block_size,
            clipped_block_rows);
    default:
        return nullptr;
    }
}

bool Segment::useCleanRead(const SegmentSnapshotPtr & segment_snap, const ColumnDefines & columns_to_read)
{
    return segment_snap->delta->getRows() == 0 //
        && segment_snap->delta->getDeletes() == 0 //
        && !hasColumn(columns_to_read, EXTRA_HANDLE_COLUMN_ID) //
        && !hasColumn(columns_to_read, VERSION_COLUMN_ID) //
        && !hasColumn(columns_to_read, TAG_COLUMN_ID);
}

BlockInputStreamPtr Segment::getInputStreamModeNormal(
    const DMContext & dm_context,
    const ColumnDefines & columns_to_read,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & filter,
    UInt64 start_ts,
    size_t expected_block_size,
    bool need_row_id)
{
    LOG_TRACE(segment_snap->log, "Begin segment create input stream");

    auto read_tag = need_row_id ? ReadTag::MVCC : ReadTag::Query;
    auto read_info = getReadInfo(dm_context, columns_to_read, segment_snap, read_ranges, read_tag, start_ts);

    auto real_ranges = shrinkRowKeyRanges(read_ranges);
    if (real_ranges.empty())
        return std::make_shared<EmptyBlockInputStream>(toEmptyBlock(*read_info.read_columns));

    BlockInputStreamPtr stream;
    if (dm_context.read_delta_only)
    {
        throw Exception("Unsupported for read_delta_only");
    }
    else if (dm_context.read_stable_only)
    {
        stream = segment_snap->stable->getInputStream(
            dm_context,
            *read_info.read_columns,
            real_ranges,
            filter,
            start_ts,
            expected_block_size,
            false,
            read_tag);
    }
    else if (useCleanRead(segment_snap, columns_to_read))
    {
        RUNTIME_CHECK_MSG(!need_row_id, "'need_row_id is true, should not come here'");
        // No delta, let's try some optimizations.
        stream = segment_snap->stable->getInputStream(
            dm_context,
            *read_info.read_columns,
            real_ranges,
            filter,
            start_ts,
            expected_block_size,
            true,
            read_tag);
    }
    else
    {
        stream = getPlacedStream(
            dm_context,
            *read_info.read_columns,
            real_ranges,
            filter,
            segment_snap->stable,
            read_info.getDeltaReader(need_row_id ? ReadTag::MVCC : ReadTag::Query),
            read_info.index_begin,
            read_info.index_end,
            expected_block_size,
            read_tag,
            start_ts,
            need_row_id);
    }

    stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stream, real_ranges, 0);
    stream = std::make_shared<DMVersionFilterBlockInputStream<DMVersionFilterMode::MVCC>>(
        stream,
        columns_to_read,
        start_ts,
        is_common_handle,
        dm_context.tracing_id,
        dm_context.scan_context);

    LOG_TRACE(
        segment_snap->log,
        "Finish segment create input stream, start_ts={} range_size={} ranges={}",
        start_ts,
        real_ranges.size(),
        DB::DM::toDebugString(read_ranges));
    return stream;
}

BlockInputStreamPtr Segment::getInputStreamModeNormal(
    const DMContext & dm_context,
    const ColumnDefines & columns_to_read,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & filter,
    UInt64 start_ts,
    size_t expected_block_size)
{
    auto segment_snap = createSnapshot(dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
    if (!segment_snap)
        return {};
    return getInputStreamModeNormal(
        dm_context,
        columns_to_read,
        segment_snap,
        read_ranges,
        filter,
        start_ts,
        expected_block_size);
}

BlockInputStreamPtr Segment::getInputStreamForDataExport(
    const DMContext & dm_context,
    const ColumnDefines & columns_to_read,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRange & data_range,
    size_t expected_block_size,
    bool reorganize_block) const
{
    RowKeyRanges data_ranges{data_range};
    auto read_info = getReadInfo(dm_context, columns_to_read, segment_snap, data_ranges, ReadTag::Internal);

    BlockInputStreamPtr data_stream = getPlacedStream(
        dm_context,
        *read_info.read_columns,
        data_ranges,
        EMPTY_RS_OPERATOR,
        segment_snap->stable,
        read_info.getDeltaReader(ReadTag::Internal),
        read_info.index_begin,
        read_info.index_end,
        expected_block_size,
        ReadTag::Internal);


    data_stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(data_stream, data_ranges, 0);
    if (reorganize_block)
    {
        data_stream = std::make_shared<PKSquashingBlockInputStream<false>>(
            data_stream,
            EXTRA_HANDLE_COLUMN_ID,
            is_common_handle);
    }
    data_stream = std::make_shared<DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT>>(
        data_stream,
        *read_info.read_columns,
        dm_context.min_version,
        is_common_handle);

    return data_stream;
}

/// We call getInputStreamModeFast when we read in fast mode.
/// In this case, we will read all the data in delta and stable, and then merge them without sorting.
/// Besides, we will do del_mark != 0 filtering to drop the deleted rows.
/// In conclusion, the output is unsorted, and does not do mvcc filtering.
BlockInputStreamPtr Segment::getInputStreamModeFast(
    const DMContext & dm_context,
    const ColumnDefines & columns_to_read,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & filter,
    size_t expected_block_size)
{
    auto real_ranges = shrinkRowKeyRanges(read_ranges);
    if (real_ranges.empty())
    {
        return std::make_shared<EmptyBlockInputStream>(toEmptyBlock(columns_to_read));
    }

    auto new_columns_to_read = std::make_shared<ColumnDefines>();

    // new_columns_to_read need at most columns_to_read.size() + 2, due to may extra insert into the handle column and del_mark column.
    new_columns_to_read->reserve(columns_to_read.size() + 2);

    new_columns_to_read->push_back(getExtraHandleColumnDefine(is_common_handle));
    new_columns_to_read->push_back(getTagColumnDefine());

    /// When we read in fast mode, we can try to do the following optimization:
    /// 1. Handle Column Optimization:
    ///    when the columns_to_read does not include HANDLE_COLUMN,
    ///    we can try to skip reading the handle column if the pack's handle range is fully within read range.
    ///    Thus, in this case, we set enable_handle_clean_read = true.
    /// 2. Del Column Optimization:
    ///    when the columns_to_read does not include TAG_COLUMN,
    ///    we can try to skip reading the del column if the pack has no deleted rows.
    ///    Thus, in this case, we set enable_del_clean_read = true.
    /// 3. Version Column Optimization:
    ///    if the columns_to_read does not include VERSION_COLUMN,
    ///    we don't need to read version column, thus we don't force push version column into new_columns_to_read.

    bool enable_handle_clean_read = true;
    bool enable_del_clean_read = true;

    for (const auto & c : columns_to_read)
    {
        if (c.id == EXTRA_HANDLE_COLUMN_ID)
        {
            enable_handle_clean_read = false;
        }
        else if (c.id == TAG_COLUMN_ID)
        {
            enable_del_clean_read = false;
        }
        else
        {
            new_columns_to_read->push_back(c);
        }
    }

    BlockInputStreamPtr stable_stream = segment_snap->stable->getInputStream(
        dm_context,
        *new_columns_to_read,
        real_ranges,
        filter,
        std::numeric_limits<UInt64>::max(),
        expected_block_size,
        enable_handle_clean_read,
        ReadTag::Query,
        /* is_fast_scan */ true,
        enable_del_clean_read);

    BlockInputStreamPtr delta_stream = std::make_shared<DeltaValueInputStream>(
        dm_context,
        segment_snap->delta,
        new_columns_to_read,
        this->rowkey_range,
        ReadTag::Query);

    // Do row key filtering based on data_ranges.
    delta_stream = std::make_shared<DMRowKeyFilterBlockInputStream<false>>(delta_stream, real_ranges, 0);
    stable_stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stable_stream, real_ranges, 0);

    // Filter the unneeded column and filter out the rows whose del_mark is true.
    delta_stream
        = std::make_shared<DMDeleteFilterBlockInputStream>(delta_stream, columns_to_read, dm_context.tracing_id);
    stable_stream
        = std::make_shared<DMDeleteFilterBlockInputStream>(stable_stream, columns_to_read, dm_context.tracing_id);

    BlockInputStreams streams;

    if (dm_context.read_delta_only)
    {
        streams.push_back(delta_stream);
    }
    else if (dm_context.read_stable_only)
    {
        streams.push_back(stable_stream);
    }
    else
    {
        streams.push_back(delta_stream);
        streams.push_back(stable_stream);
    }
    return std::make_shared<ConcatBlockInputStream>(streams, dm_context.tracing_id);
}

/// We call getInputStreamModeRaw in 'selraw xxxx' statement, which is always in test for debug.
/// In this case, we will read all the data without mvcc filtering and sorted merge.
BlockInputStreamPtr Segment::getInputStreamModeRaw(
    const DMContext & dm_context,
    const ColumnDefines & columns_to_read,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & data_ranges,
    size_t expected_block_size)
{
    auto new_columns_to_read = std::make_shared<ColumnDefines>();

    new_columns_to_read->push_back(getExtraHandleColumnDefine(is_common_handle));

    for (const auto & c : columns_to_read)
    {
        if (c.id != EXTRA_HANDLE_COLUMN_ID)
            new_columns_to_read->push_back(c);
    }

    BlockInputStreamPtr stable_stream = segment_snap->stable->getInputStream(
        dm_context,
        *new_columns_to_read,
        data_ranges,
        EMPTY_RS_OPERATOR,
        std::numeric_limits<UInt64>::max(),
        expected_block_size,
        /* enable_handle_clean_read */ false,
        ReadTag::Query);

    BlockInputStreamPtr delta_stream = std::make_shared<DeltaValueInputStream>(
        dm_context,
        segment_snap->delta,
        new_columns_to_read,
        this->rowkey_range,
        ReadTag::Query);

    // Do row key filtering based on data_ranges.
    delta_stream = std::make_shared<DMRowKeyFilterBlockInputStream<false>>(delta_stream, data_ranges, 0);
    stable_stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stable_stream, data_ranges, 0);

    // Filter the unneeded columns.
    delta_stream = std::make_shared<DMColumnProjectionBlockInputStream>(delta_stream, columns_to_read);
    stable_stream = std::make_shared<DMColumnProjectionBlockInputStream>(stable_stream, columns_to_read);

    BlockInputStreams streams;

    if (dm_context.read_delta_only)
    {
        streams.push_back(delta_stream);
    }
    else if (dm_context.read_stable_only)
    {
        streams.push_back(stable_stream);
    }
    else
    {
        streams.push_back(delta_stream);
        streams.push_back(stable_stream);
    }
    return std::make_shared<ConcatBlockInputStream>(streams, dm_context.tracing_id);
}

BlockInputStreamPtr Segment::getInputStreamModeRaw(const DMContext & dm_context, const ColumnDefines & columns_to_read)
{
    auto segment_snap = createSnapshot(dm_context, false, CurrentMetrics::DT_SnapshotOfReadRaw);
    if (!segment_snap)
        return {};
    return getInputStreamModeRaw(dm_context, columns_to_read, segment_snap, {rowkey_range});
}

SegmentPtr Segment::mergeDelta(DMContext & dm_context, const ColumnDefinesPtr & schema_snap) const
{
    WriteBatches wbs(*dm_context.storage_pool, dm_context.getWriteLimiter());
    auto segment_snap = createSnapshot(dm_context, true, CurrentMetrics::DT_SnapshotOfDeltaMerge);
    if (!segment_snap)
        return {};

    auto new_stable = prepareMergeDelta(dm_context, schema_snap, segment_snap, wbs);

    wbs.writeLogAndData();
    new_stable->enableDMFilesGC(dm_context);

    SYNC_FOR("before_Segment::applyMergeDelta"); // pause without holding the lock on the segment

    auto lock = mustGetUpdateLock();
    auto new_segment = applyMergeDelta(lock, dm_context, segment_snap, wbs, new_stable);

    wbs.writeAll();
    return new_segment;
}

StableValueSpacePtr Segment::prepareMergeDelta(
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const SegmentSnapshotPtr & segment_snap,
    WriteBatches & wbs) const
{
    LOG_DEBUG(
        log,
        "MergeDelta - Begin prepare, delta_column_files={} delta_rows={} delta_bytes={}",
        segment_snap->delta->getColumnFileCount(),
        segment_snap->delta->getRows(),
        segment_snap->delta->getBytes());

    EventRecorder recorder(ProfileEvents::DMDeltaMerge, ProfileEvents::DMDeltaMergeNS);

    auto data_stream = getInputStreamForDataExport(
        dm_context,
        *schema_snap,
        segment_snap,
        rowkey_range,
        dm_context.stable_pack_rows,
        /*reorginize_block*/ true);

    auto new_stable = createNewStable(dm_context, schema_snap, data_stream, segment_snap->stable->getId(), wbs);

    LOG_DEBUG(log, "MergeDelta - Finish prepare, segment={}", info());

    return new_stable;
}

SegmentPtr Segment::applyMergeDelta(
    const Segment::Lock & lock, //
    DMContext & context,
    const SegmentSnapshotPtr & segment_snap,
    WriteBatches & wbs,
    const StableValueSpacePtr & new_stable) const
{
    LOG_DEBUG(log, "MergeDelta - Begin apply");

    auto [in_memory_files, persisted_column_files]
        = delta->cloneNewlyAppendedColumnFiles(lock, context, rowkey_range, *segment_snap->delta, wbs);
    // Created references to tail pages' pages in "log" storage, we need to write them down.
    wbs.writeLogAndData();

    auto new_delta = std::make_shared<DeltaValueSpace>( //
        delta->getId(),
        persisted_column_files,
        in_memory_files);

    auto new_me = std::make_shared<Segment>( //
        parent_log,
        epoch + 1,
        rowkey_range,
        segment_id,
        next_segment_id,
        new_delta,
        new_stable);

    // avoid recheck whether to do DeltaMerge using the same gc_safe_point
    new_me->setLastCheckGCSafePoint(context.min_version);

    // Store new meta data
    new_delta->saveMeta(wbs);
    new_me->stable->saveMeta(wbs.meta);
    new_me->serialize(wbs.meta);

    // Remove old segment's delta.
    delta->recordRemoveColumnFilesPages(wbs);
    // Remove old stable's files.
    stable->recordRemovePacksPages(wbs);

    LOG_DEBUG(log, "MergeDelta - Finish apply, old_me={} new_me={}", info(), new_me->info());

    return new_me;
}

SegmentPtr Segment::replaceData(
    const Segment::Lock & lock, //
    DMContext & context,
    const DMFilePtr & data_file,
    SegmentSnapshotPtr segment_snap_opt) const
{
    LOG_DEBUG(
        log,
        "ReplaceData - Begin, snapshot_rows={} data_file={}",
        segment_snap_opt == nullptr ? "<none>" : std::to_string(segment_snap_opt->getRows()),
        data_file->path());

    ColumnFiles in_memory_files{};
    ColumnFilePersisteds persisted_files{};

    WriteBatches wbs(*context.storage_pool, context.getWriteLimiter());

    // If a snapshot is specified, we retain newly written data since the snapshot.
    // Otherwise, we just discard everything in the delta layer.
    if (segment_snap_opt != nullptr)
    {
        std::tie(in_memory_files, persisted_files)
            = delta->cloneNewlyAppendedColumnFiles(lock, context, rowkey_range, *segment_snap_opt->delta, wbs);
    }

    auto new_delta = std::make_shared<DeltaValueSpace>(delta->getId(), persisted_files, in_memory_files);
    new_delta->saveMeta(wbs);

    auto new_stable = std::make_shared<StableValueSpace>(stable->getId());
    new_stable->setFiles({data_file}, rowkey_range, &context);
    new_stable->saveMeta(wbs.meta);

    auto new_me = std::make_shared<Segment>( //
        parent_log,
        epoch + 1,
        rowkey_range,
        segment_id,
        next_segment_id,
        new_delta,
        new_stable);
    new_me->serialize(wbs.meta);

    delta->recordRemoveColumnFilesPages(wbs);
    stable->recordRemovePacksPages(wbs);

    wbs.writeAll();

    LOG_DEBUG(log, "ReplaceData - Finish, old_me={} new_me={}", info(), new_me->info());

    return new_me;
}

SegmentPtr Segment::replaceStableMetaVersion(
    const Segment::Lock &,
    DMContext & dm_context,
    const DMFiles & new_stable_files)
{
    // Ensure new stable files have the same DMFile ID and Page ID as the old stable files.
    // We only allow changing meta version when calling this function.

    if (new_stable_files.size() != stable->getDMFiles().size())
    {
        LOG_WARNING(
            log,
            "ReplaceStableMetaVersion - Failed due to stable mismatch, current_stable={} new_stable={}",
            DMFile::info(stable->getDMFiles()),
            DMFile::info(new_stable_files));
        return {};
    }
    for (size_t i = 0; i < new_stable_files.size(); i++)
    {
        if (new_stable_files[i]->fileId() != stable->getDMFiles()[i]->fileId())
        {
            LOG_WARNING(
                log,
                "ReplaceStableMetaVersion - Failed due to stable mismatch, current_stable={} "
                "new_stable={}",
                DMFile::info(stable->getDMFiles()),
                DMFile::info(new_stable_files));
            return {};
        }
    }

    WriteBatches wbs(*dm_context.storage_pool, dm_context.getWriteLimiter());

    DMFiles new_dm_files;
    new_dm_files.reserve(new_stable_files.size());
    const auto & current_stable_files = stable->getDMFiles();
    for (size_t file_idx = 0; file_idx < new_stable_files.size(); ++file_idx)
    {
        const auto & new_file = new_stable_files[file_idx];
        const auto & current_file = current_stable_files[file_idx];
        RUNTIME_CHECK(new_file->fileId() == current_file->fileId());
        if (new_file->pageId() != current_file->pageId())
        {
            // Allow pageId being different. We will restore using a correct pageId
            // because this function is supposed to only update meta version.
            auto new_dmfile = DMFile::restore(
                dm_context.global_context.getFileProvider(),
                new_file->fileId(),
                current_file->pageId(),
                new_file->parentPath(),
                DMFileMeta::ReadMode::all(),
                new_file->metaVersion());
            new_dm_files.push_back(new_dmfile);
        }
        else
        {
            new_dm_files.push_back(new_file);
        }
    }

    auto new_stable = std::make_shared<StableValueSpace>(stable->getId());
    new_stable->setFiles(new_dm_files, rowkey_range, &dm_context);
    new_stable->saveMeta(wbs.meta);

    auto new_me = std::make_shared<Segment>( //
        parent_log,
        epoch + 1,
        rowkey_range,
        segment_id,
        next_segment_id,
        delta, // Delta is untouched. Shares the same delta instance.
        new_stable);
    new_me->serialize(wbs.meta);

    wbs.writeAll();

    LOG_DEBUG(
        log,
        "ReplaceStableMetaVersion - Finish, new_stable={} old_stable={}",
        DMFile::info(new_stable_files),
        DMFile::info(stable->getDMFiles()));
    return new_me;
}

SegmentPtr Segment::dangerouslyReplaceDataFromCheckpoint(
    const Segment::Lock &, //
    DMContext & dm_context,
    const DMFilePtr & data_file,
    WriteBatches & wbs,
    const ColumnFilePersisteds & column_file_persisteds) const
{
    LOG_DEBUG(
        log,
        "ReplaceData - Begin, data_file={}, column_files_num={}",
        data_file->path(),
        column_file_persisteds.size());

    auto & storage_pool = dm_context.storage_pool;
    auto delegate = dm_context.path_pool->getStableDiskDelegator();

    // Always create a ref to the file to allow `data_file` being shared.
    auto new_page_id = storage_pool->newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);
    auto ref_file = DMFile::restore(
        dm_context.global_context.getFileProvider(),
        data_file->fileId(),
        new_page_id,
        data_file->parentPath(),
        DMFileMeta::ReadMode::all(),
        data_file->metaVersion(),
        dm_context.keyspace_id);
    wbs.data.putRefPage(new_page_id, data_file->pageId());

    auto new_stable = std::make_shared<StableValueSpace>(stable->getId());
    new_stable->setFiles({ref_file}, rowkey_range, &dm_context);
    new_stable->saveMeta(wbs.meta);

    ColumnFilePersisteds new_column_file_persisteds;
    for (const auto & column_file : column_file_persisteds)
    {
        if (auto * t = column_file->tryToTinyFile(); t)
        {
            // This column file may be ingested into multiple segments, so we cannot reuse its page id here.
            auto new_cf_id = storage_pool->newLogPageId();
            wbs.log.putRefPage(new_cf_id, t->getDataPageId());
            new_column_file_persisteds.push_back(t->cloneWith(new_cf_id));
        }
        else if (auto * d = column_file->tryToDeleteRange(); d)
        {
            new_column_file_persisteds.push_back(column_file);
        }
        else if (auto * b = column_file->tryToBigFile(); b)
        {
            auto new_data_page_id = storage_pool->newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);
            auto old_data_page_id = b->getDataPageId();
            wbs.data.putRefPage(new_data_page_id, old_data_page_id);
            auto wn_ps = dm_context.global_context.getWriteNodePageStorage();
            auto full_page_id = UniversalPageIdFormat::toFullPageId(
                UniversalPageIdFormat::toFullPrefix(
                    dm_context.keyspace_id,
                    StorageType::Data,
                    dm_context.physical_table_id),
                old_data_page_id);
            auto remote_data_location = wn_ps->getCheckpointLocation(full_page_id);
            auto data_key_view = S3::S3FilenameView::fromKey(*(remote_data_location->data_file_id)).asDataFile();
            auto file_oid = data_key_view.getDMFileOID();
            RUNTIME_CHECK(file_oid.file_id == b->getFile()->fileId(), file_oid.file_id, b->getFile()->fileId());
            auto remote_data_store = dm_context.global_context.getSharedContextDisagg()->remote_data_store;
            RUNTIME_CHECK(remote_data_store != nullptr);
            auto prepared = remote_data_store->prepareDMFile(file_oid, new_data_page_id);
            auto dmfile = prepared->restore(DMFileMeta::ReadMode::all(), b->getFile()->metaVersion());
            auto new_column_file = b->cloneWith(dm_context, dmfile, rowkey_range);
            // TODO: Do we need to acquire new page id for ColumnFileTiny index page?
            // Maybe we even do not need to clone data page: https://github.com/pingcap/tiflash/pull/9436
            new_column_file_persisteds.push_back(new_column_file);
        }
        else
        {
            RUNTIME_CHECK(false);
        }
    }

    auto new_delta = std::make_shared<DeltaValueSpace>(delta->getId(), new_column_file_persisteds);
    new_delta->saveMeta(wbs);

    auto new_me = std::make_shared<Segment>( //
        parent_log,
        epoch + 1,
        rowkey_range,
        segment_id,
        next_segment_id,
        new_delta,
        new_stable);
    new_me->serialize(wbs.meta);

    delta->recordRemoveColumnFilesPages(wbs);
    stable->recordRemovePacksPages(wbs);

    LOG_DEBUG(log, "ReplaceData - Finish, old_me={} new_me={}", info(), new_me->info());

    return new_me;
}

SegmentPair Segment::split(
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    std::optional<RowKeyValue> opt_split_at,
    SplitMode opt_split_mode) const
{
    WriteBatches wbs(*dm_context.storage_pool, dm_context.getWriteLimiter());
    auto segment_snap = createSnapshot(dm_context, true, CurrentMetrics::DT_SnapshotOfSegmentSplit);
    if (!segment_snap)
        return {};

    auto split_info_opt = prepareSplit(dm_context, schema_snap, segment_snap, opt_split_at, opt_split_mode, wbs);
    if (!split_info_opt.has_value())
        return {};

    auto & split_info = split_info_opt.value();

    wbs.writeLogAndData();
    split_info.my_stable->enableDMFilesGC(dm_context);
    split_info.other_stable->enableDMFilesGC(dm_context);

    SYNC_FOR("before_Segment::applySplit"); // pause without holding the lock on the segment

    auto lock = mustGetUpdateLock();
    auto segment_pair = applySplit(lock, dm_context, segment_snap, wbs, split_info);

    wbs.writeAll();

    return segment_pair;
}

std::optional<RowKeyValue> Segment::getSplitPointFast(DMContext & dm_context, const StableSnapshotPtr & stable_snap)
    const
{
    // FIXME: this method does not consider invalid packs in stable dmfiles.

    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);
    auto stable_rows = stable_snap->getRows();
    if (unlikely(!stable_rows))
        return {};

    size_t split_row_index = stable_rows / 2;

    const auto & dmfiles = stable_snap->getDMFiles();

    DMFilePtr read_file;
    size_t file_index = 0;
    auto read_pack = std::make_shared<IdSet>();
    size_t read_row_in_pack = 0;

    size_t cur_rows = 0;
    for (size_t index = 0; index < dmfiles.size(); index++)
    {
        const auto & file = dmfiles[index];
        size_t rows_in_file = file->getRows();
        cur_rows += rows_in_file;
        if (cur_rows > split_row_index)
        {
            cur_rows -= rows_in_file;
            const auto & pack_stats = file->getPackStats();
            for (size_t pack_id = 0; pack_id < pack_stats.size(); ++pack_id)
            {
                cur_rows += pack_stats[pack_id].rows;
                if (cur_rows > split_row_index)
                {
                    cur_rows -= pack_stats[pack_id].rows;

                    read_file = file;
                    file_index = index;
                    read_pack->insert(pack_id);
                    read_row_in_pack = split_row_index - cur_rows;

                    break;
                }
            }
            break;
        }
    }
    if (unlikely(!read_file))
        throw Exception("Logical error: failed to find split point");

    DMFileBlockInputStreamBuilder builder(dm_context.global_context);
    auto stream = builder.setColumnCache(stable_snap->getColumnCaches()[file_index])
                      .setReadPacks(read_pack)
                      .setTracingID(fmt::format("{}-getSplitPointFast", dm_context.tracing_id))
                      .build(
                          read_file,
                          /*read_columns=*/{getExtraHandleColumnDefine(is_common_handle)},
                          /*rowkey_ranges=*/{RowKeyRange::newAll(is_common_handle, rowkey_column_size)},
                          dm_context.scan_context);

    stream->readPrefix();
    auto block = stream->read();
    if (!block)
        throw Exception("Unexpected empty block");
    stream->readSuffix();

    RowKeyColumnContainer rowkey_column(block.getByPosition(0).column, is_common_handle);
    RowKeyValue split_point(rowkey_column.getRowKeyValue(read_row_in_pack));


    if (!rowkey_range.check(split_point.toRowKeyValueRef())
        || RowKeyRange(rowkey_range.start, split_point, is_common_handle, rowkey_column_size).none()
        || RowKeyRange(split_point, rowkey_range.end, is_common_handle, rowkey_column_size).none())
    {
        LOG_WARNING(
            log,
            "Split - unexpected split_point: {}, should be in range {}, cur_rows: {}, read_row_in_pack: {}, "
            "file_index: {}",
            split_point.toRowKeyValueRef().toDebugString(),
            rowkey_range.toDebugString(),
            cur_rows,
            read_row_in_pack,
            file_index);
        return {};
    }

    return {split_point};
}

std::optional<RowKeyValue> Segment::getSplitPointSlow(
    DMContext & dm_context,
    const ReadInfo & read_info,
    const SegmentSnapshotPtr & segment_snap) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);

    const auto & pk_col = getExtraHandleColumnDefine(is_common_handle);
    auto pk_col_defs = std::make_shared<ColumnDefines>(ColumnDefines{pk_col});
    // We need to create a new delta_reader here, because the one in read_info is used to read columns other than PK column.
    auto delta_reader = read_info.getDeltaReader(pk_col_defs, ReadTag::Internal);

    size_t exact_rows = 0;

    RowKeyRanges rowkey_ranges{rowkey_range};
    {
        BlockInputStreamPtr stream = getPlacedStream(
            dm_context,
            *pk_col_defs,
            rowkey_ranges,
            EMPTY_RS_OPERATOR,
            segment_snap->stable,
            delta_reader,
            read_info.index_begin,
            read_info.index_end,
            dm_context.stable_pack_rows,
            ReadTag::Internal);

        stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stream, rowkey_ranges, 0);

        stream->readPrefix();
        Block block;
        while ((block = stream->read()))
            exact_rows += block.rows();
        stream->readSuffix();
    }

    if (exact_rows == 0)
    {
        LOG_WARNING(log, "Segment has no rows, should not split, segment={}", info());
        return {};
    }

    BlockInputStreamPtr stream = getPlacedStream(
        dm_context,
        *pk_col_defs,
        rowkey_ranges,
        EMPTY_RS_OPERATOR,
        segment_snap->stable,
        delta_reader,
        read_info.index_begin,
        read_info.index_end,
        dm_context.stable_pack_rows,
        ReadTag::Internal);

    stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stream, rowkey_ranges, 0);

    size_t split_row_index = exact_rows / 2;
    RowKeyValue split_point;
    size_t count = 0;

    stream->readPrefix();
    while (true)
    {
        Block block = stream->read();
        if (!block)
            break;
        count += block.rows();
        if (count > split_row_index)
        {
            size_t offset_in_block = block.rows() - (count - split_row_index);
            RowKeyColumnContainer rowkey_column(block.getByName(pk_col.name).column, is_common_handle);
            split_point = RowKeyValue(rowkey_column.getRowKeyValue(offset_in_block));
            break;
        }
    }
    stream->readSuffix();

    if (!rowkey_range.check(split_point.toRowKeyValueRef())
        || RowKeyRange(rowkey_range.start, split_point, is_common_handle, rowkey_column_size).none()
        || RowKeyRange(split_point, rowkey_range.end, is_common_handle, rowkey_column_size).none())
    {
        LOG_WARNING(
            log,
            "unexpected split_handle: {}, should be in range {}, exact_rows: {}, cur count: {}, split_row_index: {}",
            split_point.toRowKeyValueRef().toDebugString(),
            rowkey_range.toDebugString(),
            exact_rows,
            count,
            split_row_index);
        return {};
    }

    return {split_point};
}

bool isSplitPointValid(const RowKeyRange & segment_range, const RowKeyValueRef & split_point)
{
    return segment_range.check(split_point) && split_point != segment_range.getStart();
}

std::optional<Segment::SplitInfo> Segment::prepareSplit(
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const SegmentSnapshotPtr & segment_snap,
    std::optional<RowKeyValue> opt_split_at,
    Segment::SplitMode split_mode,
    WriteBatches & wbs) const
{
    SYNC_FOR("before_Segment::prepareSplit");

    if (opt_split_at.has_value())
    {
        if (!isSplitPointValid(rowkey_range, opt_split_at->toRowKeyValueRef()))
        {
            LOG_WARNING(
                log,
                "Split - Split skipped because the specified split point is invalid, split_point={}",
                opt_split_at.value().toDebugString());
            return std::nullopt;
        }
    }

    SplitMode try_split_mode = split_mode;
    // We will only try either LogicalSplit or PhysicalSplit.
    if (split_mode == SplitMode::Auto)
    {
        if (opt_split_at.has_value())
        {
            if (dm_context.enable_logical_split)
                try_split_mode = SplitMode::Logical;
            else
                try_split_mode = SplitMode::Physical;
        }
        else
        {
            // When split point is not specified, there are some preconditions in order to use logical split.
            if (!dm_context.enable_logical_split //
                || segment_snap->stable->getDMFilesPacks() <= 3 //
                || segment_snap->delta->getRows() > segment_snap->stable->getRows())
            {
                try_split_mode = SplitMode::Physical;
            }
            else
            {
                try_split_mode = SplitMode::Logical;
            }
        }
    }

    switch (try_split_mode)
    {
    case SplitMode::Logical:
    {
        auto [split_info_or_null, status]
            = prepareSplitLogical(dm_context, schema_snap, segment_snap, opt_split_at, wbs);
        if (status == PrepareSplitLogicalStatus::FailCalculateSplitPoint && split_mode == SplitMode::Auto)
            // Fallback to use physical split if possible.
            return prepareSplitPhysical(dm_context, schema_snap, segment_snap, std::nullopt, wbs);
        else
            return split_info_or_null;
    }
    case SplitMode::Physical:
        return prepareSplitPhysical(dm_context, schema_snap, segment_snap, opt_split_at, wbs);
    default:
        RUNTIME_CHECK(false, static_cast<Int32>(try_split_mode));
    }
}

std::pair<std::optional<Segment::SplitInfo>, Segment::PrepareSplitLogicalStatus> //
Segment::prepareSplitLogical( //
    DMContext & dm_context,
    const ColumnDefinesPtr & /*schema_snap*/,
    const SegmentSnapshotPtr & segment_snap,
    std::optional<RowKeyValue> opt_split_point,
    WriteBatches & wbs) const
{
    LOG_DEBUG(
        log,
        "Split - SplitLogical - Begin prepare, opt_split_point={}",
        opt_split_point.has_value() ? opt_split_point->toDebugString() : "(null)");

    if (!opt_split_point.has_value())
    {
        opt_split_point = getSplitPointFast(dm_context, segment_snap->stable);
        if (!opt_split_point.has_value() || !isSplitPointValid(rowkey_range, opt_split_point->toRowKeyValueRef()))
        {
            LOG_INFO(
                log,
                "Split - SplitLogical - Fail to calculate out a valid split point, calculated_split_point={} "
                "segment={}",
                (opt_split_point.has_value() ? opt_split_point->toDebugString() : "(null)"),
                info());
            return {std::nullopt, PrepareSplitLogicalStatus::FailCalculateSplitPoint};
        }
    }

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & storage_pool = dm_context.storage_pool;

    RowKeyRange my_range(rowkey_range.start, opt_split_point.value(), is_common_handle, rowkey_column_size);
    RowKeyRange other_range(opt_split_point.value(), rowkey_range.end, is_common_handle, rowkey_column_size);

    if (my_range.none() || other_range.none())
    {
        LOG_WARNING(
            log,
            "Split - SplitLogical - Unexpected range, aborted, my_range: {}, other_range: {}",
            my_range.toDebugString(),
            other_range.toDebugString());
        return {std::nullopt, PrepareSplitLogicalStatus::FailOther};
    }

    GenPageId log_gen_page_id = [&]() {
        return storage_pool->newLogPageId();
    };

    DMFiles my_stable_files;
    DMFiles other_stable_files;

    auto delegate = dm_context.path_pool->getStableDiskDelegator();
    for (const auto & dmfile : segment_snap->stable->getDMFiles())
    {
        auto ori_page_id = dmfile->pageId();
        auto file_id = dmfile->fileId();
        auto file_parent_path = dmfile->parentPath();
        if (!dm_context.global_context.getSharedContextDisagg()->remote_data_store)
        {
            RUNTIME_CHECK(file_parent_path == delegate.getDTFilePath(file_id));
        }

        auto my_dmfile_page_id = storage_pool->newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);
        auto other_dmfile_page_id = storage_pool->newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);

        // Note that the file id may has already been mark as deleted. We must
        // create a reference to the page id itself instead of create a reference
        // to the file id.
        wbs.data.putRefPage(my_dmfile_page_id, ori_page_id);
        wbs.data.putRefPage(other_dmfile_page_id, ori_page_id);
        wbs.removed_data.delPage(ori_page_id);

        auto my_dmfile = DMFile::restore(
            dm_context.global_context.getFileProvider(),
            file_id,
            /* page_id= */ my_dmfile_page_id,
            file_parent_path,
            DMFileMeta::ReadMode::all(),
            dmfile->metaVersion(),
            dm_context.keyspace_id);
        auto other_dmfile = DMFile::restore(
            dm_context.global_context.getFileProvider(),
            file_id,
            /* page_id= */ other_dmfile_page_id,
            file_parent_path,
            DMFileMeta::ReadMode::all(),
            dmfile->metaVersion(),
            dm_context.keyspace_id);
        my_stable_files.push_back(my_dmfile);
        other_stable_files.push_back(other_dmfile);
    }

    auto other_stable_id = storage_pool->newMetaPageId();

    auto my_stable = std::make_shared<StableValueSpace>(segment_snap->stable->getId());
    auto other_stable = std::make_shared<StableValueSpace>(other_stable_id);

    my_stable->setFiles(my_stable_files, my_range, &dm_context);
    other_stable->setFiles(other_stable_files, other_range, &dm_context);

    LOG_DEBUG(
        log,
        "Split - SplitLogical - Finish prepare, segment={} split_point={}",
        info(),
        opt_split_point->toDebugString());

    return {
        SplitInfo{
            .is_logical = true,
            .split_point = opt_split_point.value(),
            .my_stable = my_stable,
            .other_stable = other_stable},
        PrepareSplitLogicalStatus::Success};
}

std::optional<Segment::SplitInfo> Segment::prepareSplitPhysical( //
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const SegmentSnapshotPtr & segment_snap,
    std::optional<RowKeyValue> opt_split_point,
    WriteBatches & wbs) const
{
    LOG_DEBUG(
        log,
        "Split - SplitPhysical - Begin prepare, opt_split_point={}",
        opt_split_point.has_value() ? opt_split_point->toDebugString() : "(null)");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto read_info = getReadInfo(
        dm_context,
        *schema_snap,
        segment_snap,
        {RowKeyRange::newAll(is_common_handle, rowkey_column_size)},
        ReadTag::Internal);

    if (!opt_split_point.has_value())
        opt_split_point = getSplitPointSlow(dm_context, read_info, segment_snap);
    if (!opt_split_point.has_value())
        return {};

    const auto & split_point = opt_split_point.value();

    RowKeyRange my_range(rowkey_range.start, split_point, is_common_handle, rowkey_column_size);
    RowKeyRange other_range(split_point, rowkey_range.end, is_common_handle, rowkey_column_size);

    if (my_range.none() || other_range.none())
    {
        LOG_WARNING(
            log,
            "Split - SplitPhysical - Unexpected range, aborted, my_range: {}, other_range: {}",
            my_range.toDebugString(),
            other_range.toDebugString());
        return std::nullopt;
    }

    StableValueSpacePtr my_new_stable;
    StableValueSpacePtr other_stable;

    {
        auto my_delta_reader = read_info.getDeltaReader(schema_snap, ReadTag::Internal);

        RowKeyRanges my_ranges{my_range};
        BlockInputStreamPtr my_data = getPlacedStream(
            dm_context,
            *read_info.read_columns,
            my_ranges,
            EMPTY_RS_OPERATOR,
            segment_snap->stable,
            my_delta_reader,
            read_info.index_begin,
            read_info.index_end,
            dm_context.stable_pack_rows,
            ReadTag::Internal);


        my_data = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(my_data, my_ranges, 0);
        my_data
            = std::make_shared<PKSquashingBlockInputStream<false>>(my_data, EXTRA_HANDLE_COLUMN_ID, is_common_handle);
        my_data = std::make_shared<DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT>>(
            my_data,
            *read_info.read_columns,
            dm_context.min_version,
            is_common_handle);
        auto my_stable_id = segment_snap->stable->getId();
        my_new_stable = createNewStable(dm_context, schema_snap, my_data, my_stable_id, wbs);
    }

    LOG_DEBUG(log, "Split - SplitPhysical - Finish prepare my_new_stable");

    {
        // Write new segment's data
        auto other_delta_reader = read_info.getDeltaReader(schema_snap, ReadTag::Internal);

        RowKeyRanges other_ranges{other_range};
        BlockInputStreamPtr other_data = getPlacedStream(
            dm_context,
            *read_info.read_columns,
            other_ranges,
            EMPTY_RS_OPERATOR,
            segment_snap->stable,
            other_delta_reader,
            read_info.index_begin,
            read_info.index_end,
            dm_context.stable_pack_rows,
            ReadTag::Internal);


        other_data = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(other_data, other_ranges, 0);
        other_data = std::make_shared<PKSquashingBlockInputStream<false>>(
            other_data,
            EXTRA_HANDLE_COLUMN_ID,
            is_common_handle);
        other_data = std::make_shared<DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT>>(
            other_data,
            *read_info.read_columns,
            dm_context.min_version,
            is_common_handle);
        auto other_stable_id = dm_context.storage_pool->newMetaPageId();
        other_stable = createNewStable(dm_context, schema_snap, other_data, other_stable_id, wbs);
    }

    LOG_DEBUG(log, "Split - SplitPhysical - Finish prepare other_stable");

    // Remove old stable's files.
    for (const auto & file : stable->getDMFiles())
    {
        // Here we should remove the ref id instead of file_id.
        // Because a dmfile could be used by several segments, and only after all ref_ids are removed, then the file_id removed.
        wbs.removed_data.delPage(file->pageId());
    }

    LOG_DEBUG(
        log,
        "Split - SplitPhysical - Finish prepare, segment={} split_point={}",
        info(),
        split_point.toDebugString());

    return SplitInfo{
        .is_logical = false,
        .split_point = split_point,
        .my_stable = my_new_stable,
        .other_stable = other_stable,
    };
}

SegmentPair Segment::applySplit( //
    const Segment::Lock & lock,
    DMContext & dm_context,
    const SegmentSnapshotPtr & segment_snap,
    WriteBatches & wbs,
    SplitInfo & split_info) const
{
    LOG_DEBUG(log, "Split - {} - Begin apply", split_info.is_logical ? "SplitLogical" : "SplitPhysical");

    RowKeyRange my_range(rowkey_range.start, split_info.split_point, is_common_handle, rowkey_column_size);
    RowKeyRange other_range(split_info.split_point, rowkey_range.end, is_common_handle, rowkey_column_size);

    // In logical split, the newly created two segment shares the same delta column files,
    // because stable content is unmodified.
    auto [my_in_memory_files, my_persisted_files] = split_info.is_logical
        ? delta->cloneAllColumnFiles(lock, dm_context, my_range, wbs)
        : delta->cloneNewlyAppendedColumnFiles(lock, dm_context, my_range, *segment_snap->delta, wbs);
    auto [other_in_memory_files, other_persisted_files] = split_info.is_logical
        ? delta->cloneAllColumnFiles(lock, dm_context, other_range, wbs)
        : delta->cloneNewlyAppendedColumnFiles(lock, dm_context, other_range, *segment_snap->delta, wbs);

    // Created references to tail pages' pages in "log" storage, we need to write them down.
    wbs.writeLogAndData();

    auto other_segment_id = dm_context.storage_pool->newMetaPageId();
    auto other_delta_id = dm_context.storage_pool->newMetaPageId();

    auto my_delta = std::make_shared<DeltaValueSpace>( //
        delta->getId(),
        my_persisted_files,
        my_in_memory_files);
    auto other_delta = std::make_shared<DeltaValueSpace>( //
        other_delta_id,
        other_persisted_files,
        other_in_memory_files);

    auto new_me = std::make_shared<Segment>( //
        parent_log,
        this->epoch + 1,
        my_range,
        this->segment_id,
        other_segment_id,
        my_delta,
        split_info.my_stable);
    auto other = std::make_shared<Segment>( //
        parent_log,
        INITIAL_EPOCH,
        other_range,
        other_segment_id,
        this->next_segment_id,
        other_delta,
        split_info.other_stable);

    new_me->delta->saveMeta(wbs);
    new_me->stable->saveMeta(wbs.meta);
    new_me->serialize(wbs.meta);

    other->delta->saveMeta(wbs);
    other->stable->saveMeta(wbs.meta);
    other->serialize(wbs.meta);

    // Remove old segment's delta.
    delta->recordRemoveColumnFilesPages(wbs);
    // Remove old stable's files.
    stable->recordRemovePacksPages(wbs);

    LOG_DEBUG(
        log,
        "Split - {} - Finish apply, old_me={} new_me={} new_other={}",
        split_info.is_logical ? "SplitLogical" : "SplitPhysical",
        info(),
        new_me->info(),
        other->info());

    return {new_me, other};
}

SegmentPtr Segment::merge(
    DMContext & dm_context,
    const ColumnDefinesPtr & schema_snap,
    const std::vector<SegmentPtr> & ordered_segments)
{
    WriteBatches wbs(*dm_context.storage_pool, dm_context.getWriteLimiter());

    std::vector<SegmentSnapshotPtr> ordered_snapshots;
    for (const auto & seg : ordered_segments)
    {
        auto snap = seg->createSnapshot(dm_context, /* for_update */ true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
        if (!snap)
        {
            LOG_DEBUG(seg->log, "Merge - Give up segmentMerge because snapshot failed, seg={}", seg->simpleInfo());
            return {};
        }
        ordered_snapshots.emplace_back(snap);
    }

    auto merged_stable = prepareMerge(dm_context, schema_snap, ordered_segments, ordered_snapshots, wbs);

    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC(dm_context);

    SYNC_FOR("before_Segment::applyMerge"); // pause without holding the lock on segments to be merged

    std::vector<Segment::Lock> locks;
    locks.reserve(ordered_segments.size());
    for (const auto & seg : ordered_segments)
        locks.emplace_back(seg->mustGetUpdateLock());

    auto merged = applyMerge(locks, dm_context, ordered_segments, ordered_snapshots, wbs, merged_stable);

    wbs.writeAll();
    return merged;
}

StableValueSpacePtr Segment::prepareMerge(
    DMContext & dm_context, //
    const ColumnDefinesPtr & schema_snap,
    const std::vector<SegmentPtr> & ordered_segments,
    const std::vector<SegmentSnapshotPtr> & ordered_snapshots,
    WriteBatches & wbs)
{
    RUNTIME_CHECK(ordered_segments.size() >= 2, ordered_snapshots.size());
    RUNTIME_CHECK(
        ordered_segments.size() == ordered_snapshots.size(),
        ordered_segments.size(),
        ordered_snapshots.size());

    const auto & log = ordered_segments[0]->log;
    LOG_DEBUG(log, "Merge - Begin prepare, segments_to_merge={}", simpleInfo(ordered_segments));

    for (size_t i = 1; i < ordered_segments.size(); i++)
    {
        RUNTIME_CHECK(
            ordered_segments[i - 1]->rowkey_range.getEnd() == ordered_segments[i]->rowkey_range.getStart(),
            i,
            ordered_segments[i - 1]->info(),
            ordered_segments[i]->info());
        RUNTIME_CHECK(
            ordered_segments[i - 1]->next_segment_id == ordered_segments[i]->segment_id,
            i,
            ordered_segments[i - 1]->info(),
            ordered_segments[i]->info());
    }

    auto get_stream = [&](const SegmentPtr & segment, const SegmentSnapshotPtr & segment_snap) {
        auto read_info = segment->getReadInfo(
            dm_context,
            *schema_snap,
            segment_snap,
            {RowKeyRange::newAll(segment->is_common_handle, segment->rowkey_column_size)},
            ReadTag::Internal);
        RowKeyRanges rowkey_ranges{segment->rowkey_range};
        BlockInputStreamPtr stream = getPlacedStream(
            dm_context,
            *read_info.read_columns,
            rowkey_ranges,
            EMPTY_RS_OPERATOR,
            segment_snap->stable,
            read_info.getDeltaReader(ReadTag::Internal),
            read_info.index_begin,
            read_info.index_end,
            dm_context.stable_pack_rows,
            ReadTag::Internal);

        stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stream, rowkey_ranges, 0);
        stream = std::make_shared<PKSquashingBlockInputStream<false>>(
            stream,
            EXTRA_HANDLE_COLUMN_ID,
            dm_context.is_common_handle);
        stream = std::make_shared<DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT>>(
            stream,
            *read_info.read_columns,
            dm_context.min_version,
            dm_context.is_common_handle);

        return stream;
    };

    std::vector<BlockInputStreamPtr> input_streams;
    input_streams.reserve(ordered_segments.size());
    for (size_t i = 0; i < ordered_segments.size(); i++)
        input_streams.emplace_back(get_stream(ordered_segments[i], ordered_snapshots[i]));

    BlockInputStreamPtr merged_stream = std::make_shared<ConcatBlockInputStream>(input_streams, /*req_id=*/"");
    // for the purpose to calculate StableProperty of the new segment
    merged_stream = std::make_shared<DMVersionFilterBlockInputStream<DMVersionFilterMode::COMPACT>>(
        merged_stream,
        *schema_snap,
        dm_context.min_version,
        dm_context.is_common_handle);

    auto merged_stable_id = ordered_segments[0]->stable->getId();
    auto merged_stable = createNewStable(dm_context, schema_snap, merged_stream, merged_stable_id, wbs);

    LOG_DEBUG(log, "Merge - Finish prepare, segments_to_merge={}", info(ordered_segments));

    return merged_stable;
}

SegmentPtr Segment::applyMerge(
    const std::vector<Segment::Lock> & locks, //
    DMContext & dm_context,
    const std::vector<SegmentPtr> & ordered_segments,
    const std::vector<SegmentSnapshotPtr> & ordered_snapshots,
    WriteBatches & wbs,
    const StableValueSpacePtr & merged_stable)
{
    RUNTIME_CHECK(ordered_segments.size() >= 2, ordered_snapshots.size());
    RUNTIME_CHECK(
        ordered_segments.size() == ordered_snapshots.size(),
        ordered_segments.size(),
        ordered_snapshots.size());

    const auto & first_seg = ordered_segments.front();
    const auto & last_seg = ordered_segments.back();
    const auto & log = first_seg->log;
    LOG_DEBUG(log, "Merge - Begin apply, segments_to_merge={}", simpleInfo(ordered_segments));

    RowKeyRange merged_range(
        first_seg->rowkey_range.start,
        last_seg->rowkey_range.end,
        first_seg->is_common_handle,
        first_seg->rowkey_column_size);

    ColumnFilePersisteds merged_persisted_column_files;
    ColumnFiles merged_in_memory_files;
    for (size_t i = 0; i < ordered_segments.size(); i++)
    {
        const auto [in_memory_files, persisted_files] = ordered_segments[i]->delta->cloneNewlyAppendedColumnFiles(
            locks[i],
            dm_context,
            merged_range,
            *ordered_snapshots[i]->delta,
            wbs);
        merged_persisted_column_files.insert(
            merged_persisted_column_files.end(),
            persisted_files.begin(),
            persisted_files.end());
        merged_in_memory_files.insert(merged_in_memory_files.end(), in_memory_files.begin(), in_memory_files.end());
    }

    // Created references to tail pages' pages in "log" storage, we need to write them down.
    wbs.writeLogAndData();

    auto merged_delta = std::make_shared<DeltaValueSpace>( //
        first_seg->delta->getId(),
        merged_persisted_column_files,
        merged_in_memory_files);

    auto merged = std::make_shared<Segment>( //
        first_seg->parent_log,
        first_seg->epoch + 1,
        merged_range,
        first_seg->segment_id,
        last_seg->next_segment_id,
        merged_delta,
        merged_stable);

    // Store new meta data
    merged->delta->saveMeta(wbs);
    merged->stable->saveMeta(wbs.meta);
    merged->serialize(wbs.meta);

    for (size_t i = 0; i < ordered_segments.size(); i++)
    {
        const auto & seg = ordered_segments[i];
        seg->delta->recordRemoveColumnFilesPages(wbs);
        seg->stable->recordRemovePacksPages(wbs);
        if (i > 0) // The first seg's id is preserved, so don't del id.
        {
            wbs.removed_meta.delPage(seg->segmentId());
            wbs.removed_meta.delPage(seg->delta->getId());
            wbs.removed_meta.delPage(seg->stable->getId());
        }
    }

    LOG_DEBUG(log, "Merge - Finish apply, merged={} merged_from_segments={}", merged->info(), info(ordered_segments));

    return merged;
}

SegmentPtr Segment::dropNextSegment(WriteBatches & wbs, const RowKeyRange & next_segment_range)
{
    assert(rowkey_range.end == next_segment_range.start);
    // merge the rowkey range of the next segment to this segment
    auto new_rowkey_range = RowKeyRange(
        rowkey_range.start,
        next_segment_range.end,
        rowkey_range.is_common_handle,
        rowkey_range.rowkey_column_size);
    auto new_segment = std::make_shared<Segment>( //
        parent_log,
        epoch + 1,
        new_rowkey_range,
        segment_id,
        0,
        delta,
        stable);
    new_segment->serialize(wbs.meta);
    wbs.writeMeta();
    LOG_INFO(log, "Finish segment drop its next segment, segment={}", info());
    return new_segment;
}

void Segment::check(DMContext &, const String &) const {}

bool Segment::flushCache(DMContext & dm_context)
{
    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_DeltaFlush};
    GET_METRIC(tiflash_storage_subtask_count, type_delta_flush).Increment();
    Stopwatch watch;
    SCOPE_EXIT(
        { GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_flush).Observe(watch.elapsedSeconds()); });

    return delta->flush(dm_context);
}

bool Segment::compactDelta(DMContext & dm_context)
{
    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_DeltaCompact};
    GET_METRIC(tiflash_storage_subtask_count, type_delta_compact).Increment();
    Stopwatch watch;
    SCOPE_EXIT(
        { GET_METRIC(tiflash_storage_subtask_duration_seconds, type_delta_compact).Observe(watch.elapsedSeconds()); });

    return delta->compact(dm_context);
}

void Segment::placeDeltaIndex(DMContext & dm_context) const
{
    // Update delta-index with persisted packs. TODO: can use a read snapshot here?
    auto segment_snap = createSnapshot(dm_context, /*for_update=*/true, CurrentMetrics::DT_SnapshotOfPlaceIndex);
    if (!segment_snap)
        return;
    placeDeltaIndex(dm_context, segment_snap);
}

void Segment::placeDeltaIndex(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap) const
{
    getReadInfo(
        dm_context,
        /*read_columns=*/{getExtraHandleColumnDefine(is_common_handle)},
        segment_snap,
        {RowKeyRange::newAll(is_common_handle, rowkey_column_size)},
        ReadTag::Internal);
}

String Segment::simpleInfo() const
{
    return fmt::format(
        "<segment_id={} epoch={} range={}{}>",
        segment_id,
        epoch,
        rowkey_range.toDebugString(),
        hasAbandoned() ? " abandoned=true" : "");
}

String Segment::info() const
{
    RUNTIME_CHECK(stable && delta);
    return fmt::format(
        "<segment_id={} epoch={} range={}{} next_segment_id={} "
        "delta_rows={} delta_bytes={} delta_deletes={} "
        "stable_file={} stable_rows={} stable_bytes={} "
        "dmf_rows={} dmf_bytes={} dmf_packs={}>",
        segment_id,
        epoch,
        rowkey_range.toDebugString(),
        hasAbandoned() ? " abandoned=true" : "",
        next_segment_id,

        delta->getRows(),
        delta->getBytes(),
        delta->getDeletes(),

        stable->getDMFilesString(),
        stable->getRows(),
        stable->getBytes(),

        stable->getDMFilesRows(),
        stable->getDMFilesBytes(),
        stable->getDMFilesPacks());
}

String Segment::simpleInfo(const std::vector<SegmentPtr> & segments)
{
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("[{} segments: ", segments.size());
    fmt_buf.joinStr(
        segments.cbegin(),
        segments.cend(),
        [&](const SegmentPtr & seg, FmtBuffer & fb) { fb.append(seg->simpleInfo()); },
        ", ");
    fmt_buf.fmtAppend("]");
    return fmt_buf.toString();
}

String Segment::info(const std::vector<SegmentPtr> & segments)
{
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("[{} segments: ", segments.size());
    fmt_buf.joinStr(
        segments.cbegin(),
        segments.cend(),
        [&](const SegmentPtr & seg, FmtBuffer & fb) { fb.append(seg->info()); },
        ", ");
    fmt_buf.fmtAppend("]");
    return fmt_buf.toString();
}

void Segment::drop(const FileProviderPtr & file_provider, WriteBatches & wbs)
{
    delta->recordRemoveColumnFilesPages(wbs);
    stable->recordRemovePacksPages(wbs);

    wbs.removed_meta.delPage(segment_id);
    wbs.removed_meta.delPage(delta->getId());
    wbs.removed_meta.delPage(stable->getId());
    wbs.writeAll();
    stable->drop(file_provider);
}

void Segment::dropAsFAPTemp(const FileProviderPtr & file_provider, WriteBatches & wbs)
{
    // The segment_id, delta_id, stable_id are invalid, just cleanup the persisted page_id in
    // delta layer and stable layer
    delta->recordRemoveColumnFilesPages(wbs);
    stable->recordRemovePacksPages(wbs);
    wbs.writeAll();
    stable->drop(file_provider);
}

Segment::ReadInfo Segment::getReadInfo(
    const DMContext & dm_context,
    const ColumnDefines & read_columns,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & read_ranges,
    ReadTag read_tag,
    UInt64 start_ts) const
{
    LOG_DEBUG(segment_snap->log, "Begin segment getReadInfo {}", simpleInfo());

    auto new_read_columns = arrangeReadColumns(getExtraHandleColumnDefine(is_common_handle), read_columns);
    auto pk_ver_col_defs = std::make_shared<ColumnDefines>(
        ColumnDefines{getExtraHandleColumnDefine(dm_context.is_common_handle), getVersionColumnDefine()});
    // Create a reader that reads pk and version columns to update deltaindex.
    // It related to MVCC, so always set a `ReadTag::MVCC` for it.
    auto delta_reader = std::make_shared<DeltaValueReader>(
        dm_context,
        segment_snap->delta,
        pk_ver_col_defs,
        this->rowkey_range,
        ReadTag::MVCC);

    auto [my_delta_index, fully_indexed] = ensurePlace(dm_context, segment_snap, delta_reader, read_ranges, start_ts);
    auto compacted_index = my_delta_index->getDeltaTree()->getCompactedEntries();
    // Hold compacted_index reference, to prevent it from deallocated.
    delta_reader->setDeltaIndex(compacted_index);

    LOG_DEBUG(
        segment_snap->log,
        "Finish segment getReadInfo, my_delta_index={} fully_indexed={} read_ranges={} "
        "snap={} {}",
        my_delta_index->toString(),
        fully_indexed,
        DB::DM::toDebugString(read_ranges),
        segment_snap->detailInfo(),
        simpleInfo());

    if (fully_indexed)
    {
        // Try update shared index, if my_delta_index is more advanced.
        bool ok = segment_snap->delta->getSharedDeltaIndex()->updateIfAdvanced(*my_delta_index);
        if (ok)
        {
            LOG_DEBUG(
                segment_snap->log,
                "Segment updated delta index, my_delta_index={} {}",
                my_delta_index->toString(),
                simpleInfo());
            // Update cache size.
            if (auto cache = dm_context.global_context.getSharedContextDisagg()->rn_delta_index_cache; cache)
                cache->setDeltaIndex(segment_snap->delta->getSharedDeltaIndex());
        }
    }

    // Refresh the reference in DeltaIndexManager, so that the index can be properly managed.
    if (auto manager = dm_context.global_context.getDeltaIndexManager(); manager)
        manager->refreshRef(segment_snap->delta->getSharedDeltaIndex());

    return ReadInfo(
        delta_reader->createNewReader(new_read_columns, read_tag),
        compacted_index->begin(),
        compacted_index->end(),
        new_read_columns);
}

ColumnDefinesPtr Segment::arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read)
{
    // We always put handle, version and tag column at the beginning of columns.
    ColumnDefines new_columns_to_read;

    new_columns_to_read.push_back(handle);
    new_columns_to_read.push_back(getVersionColumnDefine());
    new_columns_to_read.push_back(getTagColumnDefine());

    for (const auto & c : columns_to_read)
    {
        if (c.id != handle.id && c.id != VERSION_COLUMN_ID && c.id != TAG_COLUMN_ID)
            new_columns_to_read.push_back(c);
    }

    return std::make_shared<ColumnDefines>(std::move(new_columns_to_read));
}

template <bool skippable_place>
SkippableBlockInputStreamPtr Segment::getPlacedStream(
    const DMContext & dm_context,
    const ColumnDefines & read_columns,
    const RowKeyRanges & rowkey_ranges,
    const RSOperatorPtr & filter,
    const StableSnapshotPtr & stable_snap,
    const DeltaValueReaderPtr & delta_reader,
    const DeltaIndexIterator & delta_index_begin,
    const DeltaIndexIterator & delta_index_end,
    size_t expected_block_size,
    ReadTag read_tag,
    UInt64 start_ts,
    bool need_row_id)
{
    if (unlikely(rowkey_ranges.empty()))
        throw Exception("rowkey ranges shouldn't be empty", ErrorCodes::LOGICAL_ERROR);

    SkippableBlockInputStreamPtr stable_input_stream = stable_snap->getInputStream(
        dm_context,
        read_columns,
        rowkey_ranges,
        filter,
        start_ts,
        expected_block_size,
        /* enable_handle_clean_read */ false,
        read_tag,
        /* is_fast_scan */ false,
        /* enable_del_clean_read */ false);
    RowKeyRange rowkey_range = rowkey_ranges.size() == 1
        ? rowkey_ranges[0]
        : mergeRanges(rowkey_ranges, rowkey_ranges[0].is_common_handle, rowkey_ranges[0].rowkey_column_size);
    if (!need_row_id)
    {
        return std::make_shared<DeltaMergeBlockInputStream<skippable_place, /*need_row_id*/ false>>( //
            stable_input_stream,
            delta_reader,
            delta_index_begin,
            delta_index_end,
            rowkey_range,
            expected_block_size,
            stable_snap->getDMFilesRows(),
            dm_context.tracing_id);
    }
    else
    {
        return std::make_shared<DeltaMergeBlockInputStream<skippable_place, /*need_row_id*/ true>>( //
            stable_input_stream,
            delta_reader,
            delta_index_begin,
            delta_index_end,
            rowkey_range,
            expected_block_size,
            stable_snap->getDMFilesRows(),
            dm_context.tracing_id);
    }
}

std::pair<DeltaIndexPtr, bool> Segment::ensurePlace(
    const DMContext & dm_context,
    const SegmentSnapshotPtr & segment_snap,
    const DeltaValueReaderPtr & delta_reader,
    const RowKeyRanges & read_ranges,
    UInt64 start_ts) const
{
    const auto & stable_snap = segment_snap->stable;
    auto delta_snap = delta_reader->getDeltaSnap();
    // Try to clone from the sahred delta index, if it fails to reuse the shared delta index,
    // it will return an empty delta index and we should place it in the following branch.
    auto my_delta_index = delta_snap->getSharedDeltaIndex()->tryClone(delta_snap->getRows(), delta_snap->getDeletes());
    auto my_delta_tree = my_delta_index->getDeltaTree();

    const bool relevant_place = dm_context.enable_relevant_place;
    const bool skippable_place = dm_context.enable_skippable_place;

    // Note that, when enable_relevant_place is false , we cannot use the range of this segment.
    // Because some block / delete ranges could contain some data / range that are not belong to current segment.
    // If we use the range of this segment as relevant_range, fully_indexed will always be false in those cases.
    RowKeyRange relevant_range = relevant_place ? mergeRanges(read_ranges, is_common_handle, rowkey_column_size)
                                                : RowKeyRange::newAll(is_common_handle, rowkey_column_size);

    auto [my_placed_rows, my_placed_deletes] = my_delta_index->getPlacedStatus();

    // Let's do a fast check, determine whether we need to do place or not.
    if (!delta_reader->shouldPlace( //
            dm_context,
            my_placed_rows,
            my_placed_deletes,
            rowkey_range,
            relevant_range,
            start_ts))
    {
        // We can reuse the shared-delta-index
        return {my_delta_index, false};
    }

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_PlaceIndexUpdate};
    GET_METRIC(tiflash_storage_subtask_count, type_place_index_update).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(tiflash_storage_subtask_duration_seconds, type_place_index_update).Observe(watch.elapsedSeconds());
    });

    EventRecorder recorder(ProfileEvents::DMPlace, ProfileEvents::DMPlaceNS);

    auto items = delta_reader->getPlaceItems(
        my_placed_rows,
        my_placed_deletes,
        delta_snap->getRows(),
        delta_snap->getDeletes());

    bool fully_indexed = true;
    for (auto & v : items)
    {
        if (v.isBlock())
        {
            auto block = v.getBlock();
            auto offset = v.getBlockOffset();
            auto rows = block.rows();

            RUNTIME_CHECK_MSG(
                my_placed_rows == offset,
                "Place block offset not match, my_placed_rows={} offset={}",
                my_placed_rows,
                offset);

            if (skippable_place)
                fully_indexed &= placeUpsert<true>(
                    dm_context,
                    stable_snap,
                    delta_reader,
                    offset,
                    std::move(block),
                    *my_delta_tree,
                    relevant_range,
                    relevant_place);
            else
                fully_indexed &= placeUpsert<false>(
                    dm_context,
                    stable_snap,
                    delta_reader,
                    offset,
                    std::move(block),
                    *my_delta_tree,
                    relevant_range,
                    relevant_place);

            my_placed_rows += rows;
        }
        else
        {
            if (skippable_place)
                fully_indexed &= placeDelete<true>(
                    dm_context,
                    stable_snap,
                    delta_reader,
                    v.getDeleteRange(),
                    *my_delta_tree,
                    relevant_range,
                    relevant_place);
            else
                fully_indexed &= placeDelete<false>(
                    dm_context,
                    stable_snap,
                    delta_reader,
                    v.getDeleteRange(),
                    *my_delta_tree,
                    relevant_range,
                    relevant_place);

            ++my_placed_deletes;
        }
    }

    RUNTIME_CHECK_MSG(
        my_placed_rows == delta_snap->getRows() && my_placed_deletes == delta_snap->getDeletes(),
        "Placed status not match! Expected place rows:{}, deletes:{}, but actually placed rows:{}, deletes:{}",
        delta_snap->getRows(),
        delta_snap->getDeletes(),
        my_placed_rows,
        my_placed_deletes);

    my_delta_index->update(my_delta_tree, my_placed_rows, my_placed_deletes);

    LOG_DEBUG(
        segment_snap->log,
        "Finish segment ensurePlace, read_ranges={} placed_items={} shared_delta_index={} my_delta_index={} {}",
        DB::DM::toDebugString(read_ranges),
        items.size(),
        delta_snap->getSharedDeltaIndex()->toString(),
        my_delta_index->toString(),
        simpleInfo());

    return {my_delta_index, fully_indexed};
}

template <bool skippable_place>
bool Segment::placeUpsert(
    const DMContext & dm_context,
    const StableSnapshotPtr & stable_snap,
    const DeltaValueReaderPtr & delta_reader,
    size_t delta_value_space_offset,
    Block && block,
    DeltaTree & update_delta_tree,
    const RowKeyRange & relevant_range,
    bool relevant_place) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceUpsert, ProfileEvents::DMPlaceUpsertNS);

    IColumn::Permutation perm;

    const auto & handle = getExtraHandleColumnDefine(is_common_handle);
    bool do_sort = sortBlockByPk(handle, block, perm);
    RowKeyValueRef first_rowkey
        = RowKeyColumnContainer(block.getByPosition(0).column, is_common_handle).getRowKeyValue(0);
    RowKeyValueRef range_start = relevant_range.getStart();

    auto place_handle_range = skippable_place
        ? RowKeyRange::startFrom(std::max(first_rowkey, range_start), is_common_handle, rowkey_column_size)
        : RowKeyRange::newAll(is_common_handle, rowkey_column_size);

    auto compacted_index = update_delta_tree.getCompactedEntries();

    auto merged_stream = getPlacedStream<skippable_place>( //
        dm_context,
        {handle, getVersionColumnDefine()},
        {place_handle_range},
        EMPTY_RS_OPERATOR,
        stable_snap,
        delta_reader,
        compacted_index->begin(),
        compacted_index->end(),
        dm_context.stable_pack_rows,
        ReadTag::MVCC);

    if (do_sort)
        return DM::placeInsert<true>(
            merged_stream,
            block,
            relevant_range,
            relevant_place,
            update_delta_tree,
            delta_value_space_offset,
            perm,
            getPkSort(handle));
    else
        return DM::placeInsert<false>(
            merged_stream,
            block,
            relevant_range,
            relevant_place,
            update_delta_tree,
            delta_value_space_offset,
            perm,
            getPkSort(handle));
}

template <bool skippable_place>
bool Segment::placeDelete(
    const DMContext & dm_context,
    const StableSnapshotPtr & stable_snap,
    const DeltaValueReaderPtr & delta_reader,
    const RowKeyRange & delete_range,
    DeltaTree & update_delta_tree,
    const RowKeyRange & relevant_range,
    bool relevant_place) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceDeleteRange, ProfileEvents::DMPlaceDeleteRangeNS);

    const auto & handle = getExtraHandleColumnDefine(is_common_handle);

    RowKeyRanges delete_ranges{delete_range};
    Blocks delete_data;
    {
        auto compacted_index = update_delta_tree.getCompactedEntries();

        BlockInputStreamPtr delete_stream = getPlacedStream( //
            dm_context,
            {handle, getVersionColumnDefine()},
            delete_ranges,
            EMPTY_RS_OPERATOR,
            stable_snap,
            delta_reader,
            compacted_index->begin(),
            compacted_index->end(),
            dm_context.stable_pack_rows,
            ReadTag::MVCC);

        delete_stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(delete_stream, delete_ranges, 0);

        // Try to merge into big block. 128 MB should be enough.
        SquashingBlockInputStream squashed_delete_stream(delete_stream, 0, 128 * (1UL << 20), /*req_id=*/"");

        while (true)
        {
            Block block = squashed_delete_stream.read();
            if (!block)
                break;
            delete_data.emplace_back(std::move(block));
        }
    }

    bool fully_indexed = true;
    // Note that we can not do read and place at the same time.
    for (const auto & block : delete_data)
    {
        RowKeyValueRef first_rowkey
            = RowKeyColumnContainer(block.getByPosition(0).column, is_common_handle).getRowKeyValue(0);
        auto place_handle_range = skippable_place
            ? RowKeyRange::startFrom(first_rowkey, is_common_handle, rowkey_column_size)
            : RowKeyRange::newAll(is_common_handle, rowkey_column_size);

        auto compacted_index = update_delta_tree.getCompactedEntries();

        auto merged_stream = getPlacedStream<skippable_place>( //
            dm_context,
            {handle, getVersionColumnDefine()},
            {place_handle_range},
            EMPTY_RS_OPERATOR,
            stable_snap,
            delta_reader,
            compacted_index->begin(),
            compacted_index->end(),
            dm_context.stable_pack_rows,
            ReadTag::MVCC);
        fully_indexed &= DM::placeDelete(
            merged_stream,
            block,
            relevant_range,
            relevant_place,
            update_delta_tree,
            getPkSort(handle));
    }
    return fully_indexed;
}

BitmapFilterPtr Segment::buildBitmapFilter(
    const DMContext & dm_context,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & filter,
    UInt64 start_ts,
    size_t expected_block_size)
{
    RUNTIME_CHECK_MSG(!dm_context.read_delta_only, "Read delta only is unsupported");
    if (dm_context.read_stable_only || (segment_snap->delta->getRows() == 0 && segment_snap->delta->getDeletes() == 0))
    {
        return buildBitmapFilterStableOnly(
            dm_context,
            segment_snap,
            read_ranges,
            filter,
            start_ts,
            expected_block_size);
    }
    else
    {
        return buildBitmapFilterNormal(dm_context, segment_snap, read_ranges, filter, start_ts, expected_block_size);
    }
}

BitmapFilterPtr Segment::buildBitmapFilterNormal(
    const DMContext & dm_context,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & filter,
    UInt64 start_ts,
    size_t expected_block_size)
{
    Stopwatch sw_total;
    ColumnDefines columns_to_read{
        getExtraHandleColumnDefine(is_common_handle),
    };
    // Generate the bitmap according to the MVCC filter result
    auto stream = getInputStreamModeNormal(
        dm_context,
        columns_to_read,
        segment_snap,
        read_ranges,
        filter,
        start_ts,
        expected_block_size,
        /*need_row_id*/ true);
    // `total_rows` is the rows read for building bitmap
    auto total_rows = segment_snap->delta->getRows() + segment_snap->stable->getDMFilesRows();
    auto bitmap_filter = std::make_shared<BitmapFilter>(total_rows, /*default_value*/ false);
    bitmap_filter->set(stream);
    bitmap_filter->runOptimize();

    const auto elapse_ns = sw_total.elapsed();
    dm_context.scan_context->build_bitmap_time_ns += elapse_ns;
    LOG_DEBUG(
        segment_snap->log,
        "buildBitmapFilterNormal total_rows={} cost={:.3f}ms",
        total_rows,
        elapse_ns / 1'000'000.0);
    return bitmap_filter;
}

namespace
{

struct Range
{
    UInt64 offset;
    UInt64 rows;

    Range(UInt64 offset_, UInt64 rows_)
        : offset(offset_)
        , rows(rows_)
    {}
};

std::pair<std::vector<Range>, std::vector<IdSetPtr>> parseDMFilePackInfo(
    const DMFiles & dmfiles,
    const DMContext & dm_context,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & filter,
    UInt64 start_ts)
{
    // Packs that all rows compliant with MVCC filter and RowKey filter requirements.
    // For building bitmap filter, we don't need to read these packs,
    // just set corresponding positions in the bitmap to true.
    // So we record the offset and rows of these packs and merge continuous ranges.
    std::vector<Range> skipped_ranges;
    // Packs that some rows compliant with MVCC filter and RowKey filter requirements.
    // We need to read these packs and do RowKey filter and MVCC filter for them.
    std::vector<IdSetPtr> some_packs_sets;
    some_packs_sets.reserve(dmfiles.size());

    // The offset of the first row in the current range.
    size_t offset = 0;
    // The number of rows in the current range.
    size_t rows = 0;
    UInt32 preceded_rows = 0;

    for (const auto & dmfile : dmfiles)
    {
        DMFilePackFilter pack_filter = DMFilePackFilter::loadFrom(
            dmfile,
            dm_context.global_context.getMinMaxIndexCache(),
            /*set_cache_if_miss*/ true,
            read_ranges,
            filter,
            /*read_pack*/ {},
            dm_context.global_context.getFileProvider(),
            dm_context.global_context.getReadLimiter(),
            dm_context.scan_context,
            dm_context.tracing_id,
            ReadTag::MVCC);
        const auto & pack_res = pack_filter.getPackResConst();
        const auto & handle_res = pack_filter.getHandleRes();
        const auto & pack_stats = dmfile->getPackStats();

        auto some_packs_set = std::make_shared<IdSet>();

        for (size_t pack_id = 0; pack_id < pack_stats.size(); ++pack_id)
        {
            const auto & pack_stat = pack_stats[pack_id];
            preceded_rows += pack_stat.rows;
            if (!pack_res[pack_id].isUse())
            {
                continue;
            }

            if (handle_res[pack_id] == RSResult::Some || pack_stat.not_clean > 0
                || pack_filter.getMaxVersion(pack_id) > start_ts)
            {
                // We need to read this pack to do RowKey or MVCC filter.
                some_packs_set->insert(pack_id);
                continue;
            }

            // When this pack is next to the previous pack, we merge them.
            // Otherwise, we record the previous continuous packs and start a new one.
            if (offset + rows == preceded_rows - pack_stat.rows)
            {
                rows += pack_stat.rows;
            }
            else
            {
                skipped_ranges.emplace_back(offset, rows);
                offset = preceded_rows - pack_stat.rows;
                rows = pack_stat.rows;
            }
        }

        some_packs_sets.push_back(some_packs_set);
    }
    if (rows > 0)
        skipped_ranges.emplace_back(offset, rows);

    return {skipped_ranges, some_packs_sets};
}

} // namespace

BitmapFilterPtr Segment::buildBitmapFilterStableOnly(
    const DMContext & dm_context,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & filter,
    UInt64 start_ts,
    size_t expected_block_size)
{
    Stopwatch sw;
    const auto & dmfiles = segment_snap->stable->getDMFiles();
    RUNTIME_CHECK(!dmfiles.empty());

    auto commit_elapse = [&sw, &dm_context]() -> double {
        const auto elapse_ns = sw.elapsed();
        dm_context.scan_context->build_bitmap_time_ns += elapse_ns;
        return elapse_ns / 1'000'000.0;
    };

    auto [skipped_ranges, some_packs_sets] = parseDMFilePackInfo(dmfiles, dm_context, read_ranges, filter, start_ts);

    if (skipped_ranges.size() == 1 && skipped_ranges[0].offset == 0
        && skipped_ranges[0].rows == segment_snap->stable->getDMFilesRows())
    {
        auto elapse_ms = commit_elapse();
        LOG_DEBUG(
            segment_snap->log,
            "buildBitmapFilterStableOnly all match, total_rows={}, cost={:.3f}ms",
            segment_snap->stable->getDMFilesRows(),
            elapse_ms);
        return std::make_shared<BitmapFilter>(segment_snap->stable->getDMFilesRows(), /*default_value*/ true);
    }

    auto bitmap_filter
        = std::make_shared<BitmapFilter>(segment_snap->stable->getDMFilesRows(), /*default_value*/ false);
    for (const auto & range : skipped_ranges)
    {
        bitmap_filter->set(range.offset, range.rows);
    }

    bool has_some_packs = false;
    for (const auto & some_packs_set : some_packs_sets)
    {
        if (!some_packs_set->empty())
        {
            has_some_packs = true;
            break;
        }
    }
    if (!has_some_packs)
    {
        auto elapse_ms = commit_elapse();
        LOG_DEBUG(
            segment_snap->log,
            "buildBitmapFilterStableOnly not have some packs, total_rows={}, cost={:.3f}ms",
            segment_snap->stable->getDMFilesRows(),
            elapse_ms);
        return bitmap_filter;
    }

    const ColumnDefines columns_to_read{
        getExtraHandleColumnDefine(is_common_handle),
        getVersionColumnDefine(),
        getTagColumnDefine(),
    };
    BlockInputStreamPtr stream = segment_snap->stable->getInputStream(
        dm_context,
        columns_to_read,
        read_ranges,
        filter,
        start_ts,
        expected_block_size,
        /*enable_handle_clean_read*/ false,
        ReadTag::MVCC,
        /*is_fast_scan*/ false,
        /*enable_del_clean_read*/ false,
        /*read_packs*/ some_packs_sets,
        /*need_row_id*/ true);
    stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stream, read_ranges, 0);
    const ColumnDefines read_columns{
        getExtraHandleColumnDefine(is_common_handle),
    };
    stream = std::make_shared<DMVersionFilterBlockInputStream<DMVersionFilterMode::MVCC>>(
        stream,
        read_columns,
        start_ts,
        is_common_handle,
        dm_context.tracing_id);
    bitmap_filter->set(stream);

    auto elapse_ms = commit_elapse();
    LOG_DEBUG(
        segment_snap->log,
        "buildBitmapFilterStableOnly read_packs={} total_rows={} cost={:.3f}ms",
        some_packs_sets.size(),
        segment_snap->stable->getDMFilesRows(),
        elapse_ms);
    return bitmap_filter;
}

SkippableBlockInputStreamPtr Segment::getConcatSkippableBlockInputStream(
    BitmapFilterPtr bitmap_filter,
    const SegmentSnapshotPtr & segment_snap,
    const DMContext & dm_context,
    const ColumnDefines & columns_to_read,
    const RowKeyRanges & read_ranges,
    const RSOperatorPtr & filter,
    UInt64 start_ts,
    size_t expected_block_size,
    ReadTag read_tag)
{
    static constexpr bool NeedRowID = false;
    // set `is_fast_scan` to true to try to enable clean read
    auto enable_handle_clean_read = !hasColumn(columns_to_read, EXTRA_HANDLE_COLUMN_ID);
    constexpr auto is_fast_scan = true;
    auto enable_del_clean_read = !hasColumn(columns_to_read, TAG_COLUMN_ID);

    SkippableBlockInputStreamPtr stable_stream = segment_snap->stable->getInputStream(
        dm_context,
        columns_to_read,
        read_ranges,
        filter,
        start_ts,
        expected_block_size,
        enable_handle_clean_read,
        read_tag,
        is_fast_scan,
        enable_del_clean_read,
        /* read_packs */ {},
        NeedRowID,
        bitmap_filter);

    auto columns_to_read_ptr = std::make_shared<ColumnDefines>(columns_to_read);

    auto memtable = segment_snap->delta->getMemTableSetSnapshot();
    auto persisted_files = segment_snap->delta->getPersistedFileSetSnapshot();
    SkippableBlockInputStreamPtr mem_table_stream = std::make_shared<ColumnFileSetInputStream>(
        dm_context,
        memtable,
        columns_to_read_ptr,
        this->rowkey_range,
        read_tag);
    auto ann_query_info = getANNQueryInfo(filter);
    SkippableBlockInputStreamPtr persisted_files_stream = ColumnFileSetWithVectorIndexInputStream::tryBuild(
        dm_context,
        persisted_files,
        columns_to_read_ptr,
        this->rowkey_range,
        persisted_files->getDataProvider(),
        ann_query_info,
        bitmap_filter,
        segment_snap->stable->getDMFilesRows(),
        read_tag);

    auto stream = std::dynamic_pointer_cast<ConcatSkippableBlockInputStream<NeedRowID>>(stable_stream);
    assert(stream != nullptr);
    stream->appendChild(persisted_files_stream, persisted_files->getRows());
    stream->appendChild(mem_table_stream, memtable->getRows());
    return ConcatVectorIndexBlockInputStream::build(stream, ann_query_info);
}

BlockInputStreamPtr Segment::getLateMaterializationStream(
    BitmapFilterPtr & bitmap_filter,
    const DMContext & dm_context,
    const ColumnDefines & columns_to_read,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & data_ranges,
    const PushDownFilterPtr & filter,
    UInt64 start_ts,
    size_t expected_block_size)
{
    const auto & filter_columns = filter->filter_columns;
    BlockInputStreamPtr filter_column_stream = getConcatSkippableBlockInputStream(
        bitmap_filter,
        segment_snap,
        dm_context,
        *filter_columns,
        data_ranges,
        filter->rs_operator,
        start_ts,
        expected_block_size,
        ReadTag::LMFilter);

    if (unlikely(filter_columns->size() == columns_to_read.size()))
    {
        LOG_ERROR(
            segment_snap->log,
            "Late materialization filter columns size equal to read columns size, which is not expected, "
            "filter_columns_size={}",
            filter_columns->size());
        BlockInputStreamPtr stream
            = std::make_shared<BitmapFilterBlockInputStream>(*filter_columns, filter_column_stream, bitmap_filter);
        if (filter->extra_cast)
        {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, filter->extra_cast, dm_context.tracing_id);
            stream->setExtraInfo("cast after tableScan");
        }
        stream = std::make_shared<FilterBlockInputStream>(
            stream,
            filter->before_where,
            filter->filter_column_name,
            dm_context.tracing_id);
        stream->setExtraInfo("push down filter");
        stream
            = std::make_shared<ExpressionBlockInputStream>(stream, filter->project_after_where, dm_context.tracing_id);
        stream->setExtraInfo("project after where");
        return stream;
    }

    // construct extra cast stream if needed
    if (filter->extra_cast)
    {
        filter_column_stream = std::make_shared<ExpressionBlockInputStream>(
            filter_column_stream,
            filter->extra_cast,
            dm_context.tracing_id);
        filter_column_stream->setExtraInfo("cast after tableScan");
    }

    // construct filter stream
    filter_column_stream = std::make_shared<FilterBlockInputStream>(
        filter_column_stream,
        filter->before_where,
        filter->filter_column_name,
        dm_context.tracing_id);
    filter_column_stream->setExtraInfo("push down filter");

    auto rest_columns_to_read = std::make_shared<ColumnDefines>(columns_to_read);
    // remove columns of pushed down filter
    for (const auto & col : *filter_columns)
    {
        rest_columns_to_read->erase(
            std::remove_if(
                rest_columns_to_read->begin(),
                rest_columns_to_read->end(),
                [&](const ColumnDefine & c) { return c.id == col.id; }),
            rest_columns_to_read->end());
    }

    // construct stream for the rest columns
    auto rest_column_stream = getConcatSkippableBlockInputStream(
        bitmap_filter,
        segment_snap,
        dm_context,
        *rest_columns_to_read,
        data_ranges,
        filter->rs_operator,
        start_ts,
        expected_block_size,
        ReadTag::Query);

    // construct late materialization stream
    return std::make_shared<LateMaterializationBlockInputStream>(
        columns_to_read,
        filter->filter_column_name,
        filter_column_stream,
        rest_column_stream,
        bitmap_filter,
        dm_context.tracing_id);
}

RowKeyRanges Segment::shrinkRowKeyRanges(const RowKeyRanges & read_ranges) const
{
    RowKeyRanges real_ranges;
    for (const auto & read_range : read_ranges)
    {
        auto real_range = rowkey_range.shrink(read_range);
        if (!real_range.none())
            real_ranges.emplace_back(std::move(real_range));
    }
    return real_ranges;
}

static bool hasCacheableColumn(const ColumnDefines & columns)
{
    return std::find_if(columns.begin(), columns.end(), DMFileReader::isCacheableColumn) != columns.end();
}

BlockInputStreamPtr Segment::getBitmapFilterInputStream(
    const DMContext & dm_context,
    const ColumnDefines & columns_to_read,
    const SegmentSnapshotPtr & segment_snap,
    const RowKeyRanges & read_ranges,
    const PushDownFilterPtr & filter,
    UInt64 start_ts,
    size_t build_bitmap_filter_block_rows,
    size_t read_data_block_rows)
{
    auto real_ranges = shrinkRowKeyRanges(read_ranges);
    if (real_ranges.empty())
    {
        return std::make_shared<EmptyBlockInputStream>(toEmptyBlock(columns_to_read));
    }
    /*
    auto bitmap_filter = buildBitmapFilter(
        dm_context,
        segment_snap,
        real_ranges,
        filter ? filter->rs_operator : EMPTY_RS_OPERATOR,
        start_ts,
        build_bitmap_filter_block_rows);
    */
    UNUSED(build_bitmap_filter_block_rows);
    RUNTIME_CHECK(!is_common_handle);
    //RUNTIME_CHECK(filter == nullptr);
    auto bitmap_filter = buildBitmapFilter<Int64>(dm_context, *segment_snap, read_ranges, start_ts, version_chain);

    // If we don't need to read the cacheable columns, release column cache as soon as possible.
    if (!hasCacheableColumn(columns_to_read))
    {
        segment_snap->stable->clearColumnCaches();
    }

    if (filter && filter->before_where)
    {
        // if has filter conditions pushed down, use late materialization
        return getLateMaterializationStream(
            bitmap_filter,
            dm_context,
            columns_to_read,
            segment_snap,
            real_ranges,
            filter,
            start_ts,
            read_data_block_rows);
    }

    auto skippable_stream = getConcatSkippableBlockInputStream(
        bitmap_filter,
        segment_snap,
        dm_context,
        columns_to_read,
        real_ranges,
        filter ? filter->rs_operator : EMPTY_RS_OPERATOR,
        start_ts,
        read_data_block_rows,
        ReadTag::Query);
    auto stream = std::make_shared<BitmapFilterBlockInputStream>(columns_to_read, skippable_stream, bitmap_filter);
    if (auto * vector_index_stream = dynamic_cast<ConcatVectorIndexBlockInputStream *>(skippable_stream.get());
        vector_index_stream)
    {
        // For vector search, there are more likely to return small blocks from different
        // sub-streams. Squash blocks to reduce the number of blocks thus improve the
        // performance of upper layer.
        return std::make_shared<SquashingBlockInputStream>(
            stream,
            /*min_block_size_rows=*/read_data_block_rows,
            /*min_block_size_bytes=*/0,
            dm_context.tracing_id);
    }
    return stream;
}

// clipBlockRows try to limit the block size not exceed settings.max_block_bytes.
size_t Segment::clipBlockRows(
    const Context & context,
    size_t expected_block_rows,
    const ColumnDefines & read_columns,
    const StableValueSpacePtr & stable)
{
    size_t max_block_bytes = context.getSettingsRef().max_block_bytes;
    size_t pack_rows = context.getSettingsRef().dt_segment_stable_pack_rows; // At least one pack.
    return clipBlockRows(max_block_bytes, pack_rows, expected_block_rows, read_columns, stable);
}

size_t Segment::clipBlockRows(
    size_t max_block_bytes,
    size_t pack_rows,
    size_t expected_block_rows,
    const ColumnDefines & read_columns,
    const StableValueSpacePtr & stable)
{
    // Disable block bytes limit.
    if (stable == nullptr || unlikely(max_block_bytes <= 0))
    {
        return expected_block_rows;
    }
    else
    {
        auto row_bytes = std::max(1, stable->avgRowBytes(read_columns)); // Avoid row_bytes to be 0.
        auto rows = max_block_bytes / row_bytes;
        rows = std::max(
            rows / pack_rows * pack_rows,
            pack_rows); // Align down with pack rows and at least read one pack.
        return std::min(expected_block_rows, rows);
    }
}

} // namespace DM
} // namespace DB
