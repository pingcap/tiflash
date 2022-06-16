#include <Common/TiFlashMetrics.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/EmptyBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Poco/Logger.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMDecoratorStreams.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/DeltaMerge.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaPlace.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <numeric>

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
} // namespace CurrentMetrics

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
} // namespace ErrorCodes

namespace DM
{

const static size_t SEGMENT_BUFFER_SIZE = 128; // More than enough.

DMFilePtr writeIntoNewDMFile(DMContext &                    dm_context, //
                             const ColumnDefinesPtr &       schema_snap,
                             const BlockInputStreamPtr &    input_stream,
                             UInt64                         file_id,
                             const String &                 parent_path,
                             DMFileBlockOutputStream::Flags flags)
{
    auto   dmfile        = DMFile::create(file_id, parent_path, flags.isSingleFile());
    auto   output_stream = std::make_shared<DMFileBlockOutputStream>(dm_context.db_context, dmfile, *schema_snap, flags);
    auto * mvcc_stream   = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(input_stream.get());

    input_stream->readPrefix();
    output_stream->writePrefix();
    while (true)
    {
        size_t last_effective_num_rows = 0;
        size_t last_not_clean_rows     = 0;
        if (mvcc_stream)
        {
            last_effective_num_rows = mvcc_stream->getEffectiveNumRows();
            last_not_clean_rows     = mvcc_stream->getNotCleanRows();
        }

        Block block = input_stream->read();
        if (!block)
            break;
        if (!block.rows())
            continue;

        // When the input_stream is not mvcc, we assume the rows in this input_stream is most valid and make it not tend to be gc.
        size_t cur_effective_num_rows = block.rows();
        size_t cur_not_clean_rows     = 1;
        size_t gc_hint_version        = UINT64_MAX;
        if (mvcc_stream)
        {
            cur_effective_num_rows = mvcc_stream->getEffectiveNumRows();
            cur_not_clean_rows     = mvcc_stream->getNotCleanRows();
            gc_hint_version        = mvcc_stream->getGCHintVersion();
        }

        DMFileBlockOutputStream::BlockProperty block_property;
        block_property.effective_num_rows = cur_effective_num_rows - last_effective_num_rows;
        block_property.not_clean_rows     = cur_not_clean_rows - last_not_clean_rows;
        block_property.gc_hint_version    = gc_hint_version;
        output_stream->write(block, block_property);
    }

    input_stream->readSuffix();
    output_stream->writeSuffix();

    return dmfile;
}

StableValueSpacePtr createNewStable(DMContext &                 context,
                                    const ColumnDefinesPtr &    schema_snap,
                                    const BlockInputStreamPtr & input_stream,
                                    PageId                      stable_id,
                                    WriteBatches &              wbs,
                                    bool                        need_rate_limit)
{
    auto delegator  = context.path_pool.getStableDiskDelegator();
    auto store_path = delegator.choosePath();

    DMFileBlockOutputStream::Flags flags;
    flags.setRateLimit(need_rate_limit);
    flags.setSingleFile(context.db_context.getSettingsRef().dt_enable_single_file_mode_dmfile);

    PageId dtfile_id = context.storage_pool.newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    auto   dtfile    = writeIntoNewDMFile(context, schema_snap, input_stream, dtfile_id, store_path, flags);
    auto   stable    = std::make_shared<StableValueSpace>(stable_id);
    stable->setFiles({dtfile}, RowKeyRange::newAll(context.is_common_handle, context.rowkey_column_size));
    stable->saveMeta(wbs.meta);
    wbs.data.putExternal(dtfile_id, 0);
    delegator.addDTFile(dtfile_id, dtfile->getBytesOnDisk(), store_path);

    return stable;
}

//==========================================================================================
// Segment ser/deser
//==========================================================================================

Segment::Segment(UInt64                      epoch_, //
                 const RowKeyRange &         rowkey_range_,
                 PageId                      segment_id_,
                 PageId                      next_segment_id_,
                 const DeltaValueSpacePtr &  delta_,
                 const StableValueSpacePtr & stable_)
    : epoch(epoch_),
      rowkey_range(rowkey_range_),
      is_common_handle(rowkey_range.is_common_handle),
      rowkey_column_size(rowkey_range.rowkey_column_size),
      segment_id(segment_id_),
      next_segment_id(next_segment_id_),
      delta(delta_),
      stable(stable_),
      log(&Logger::get("Segment"))
{
}

SegmentPtr Segment::newSegment(DMContext &              context,
                               const ColumnDefinesPtr & schema,
                               const RowKeyRange &      range,
                               PageId                   segment_id,
                               PageId                   next_segment_id,
                               PageId                   delta_id,
                               PageId                   stable_id)
{
    WriteBatches wbs(context.storage_pool);

    auto delta  = std::make_shared<DeltaValueSpace>(delta_id);
    auto stable = createNewStable(context, schema, std::make_shared<EmptySkippableBlockInputStream>(*schema), stable_id, wbs, false);

    auto segment = std::make_shared<Segment>(INITIAL_EPOCH, range, segment_id, next_segment_id, delta, stable);

    // Write metadata.
    delta->saveMeta(wbs);
    stable->saveMeta(wbs.meta);
    segment->serialize(wbs.meta);

    wbs.writeAll();
    stable->enableDMFilesGC();

    return segment;
}

SegmentPtr Segment::newSegment(
    DMContext & context, const ColumnDefinesPtr & schema, const RowKeyRange & rowkey_range, PageId segment_id, PageId next_segment_id)
{
    return newSegment(context,
                      schema,
                      rowkey_range,
                      segment_id,
                      next_segment_id,
                      context.storage_pool.newMetaPageId(),
                      context.storage_pool.newMetaPageId());
}

SegmentPtr Segment::restoreSegment(DMContext & context, PageId segment_id)
{
    Page page = context.storage_pool.meta().read(segment_id);

    ReadBufferFromMemory   buf(page.data.begin(), page.data.size());
    SegmentFormat::Version version;

    readIntBinary(version, buf);
    UInt64      epoch;
    RowKeyRange rowkey_range;
    PageId      next_segment_id, delta_id, stable_id;

    readIntBinary(epoch, buf);

    switch (version)
    {
    case SegmentFormat::V1: {
        HandleRange range;
        readIntBinary(range.start, buf);
        readIntBinary(range.end, buf);
        rowkey_range = RowKeyRange::fromHandleRange(range);
        break;
    }
    case SegmentFormat::V2: {
        rowkey_range = RowKeyRange::deserialize(buf);
        break;
    }
    default:
        throw Exception("Illegal version" + DB::toString(version), ErrorCodes::LOGICAL_ERROR);
    }

    readIntBinary(next_segment_id, buf);
    readIntBinary(delta_id, buf);
    readIntBinary(stable_id, buf);

    auto delta   = DeltaValueSpace::restore(context, rowkey_range, delta_id);
    auto stable  = StableValueSpace::restore(context, stable_id);
    auto segment = std::make_shared<Segment>(epoch, rowkey_range, segment_id, next_segment_id, delta, stable);

    return segment;
}

void Segment::serialize(WriteBatch & wb)
{
    MemoryWriteBuffer buf(0, SEGMENT_BUFFER_SIZE);
    writeIntBinary(STORAGE_FORMAT_CURRENT.segment, buf);
    writeIntBinary(epoch, buf);
    rowkey_range.serialize(buf);
    writeIntBinary(next_segment_id, buf);
    writeIntBinary(delta->getId(), buf);
    writeIntBinary(stable->getId(), buf);

    auto data_size = buf.count(); // Must be called before tryGetReadBuffer.
    wb.putPage(segment_id, 0, buf.tryGetReadBuffer(), data_size);
}

bool Segment::writeToDisk(DMContext & dm_context, const DeltaPackPtr & pack)
{
    LOG_TRACE(log, "Segment [" << segment_id << "] write to disk rows: " << pack->getRows() << ", isFile" << pack->isFile());
    return delta->appendPack(dm_context, pack);
}

bool Segment::writeToCache(DMContext & dm_context, const Block & block, size_t offset, size_t limit)
{
    LOG_TRACE(log, "Segment [" << segment_id << "] write to cache rows: " << limit);
    if (unlikely(limit == 0))
        return true;
    return delta->appendToCache(dm_context, block, offset, limit);
}

bool Segment::write(DMContext & dm_context, const Block & block, bool flush_cache)
{
    LOG_TRACE(log, "Segment [" << segment_id << "] write to disk rows: " << block.rows());
    WriteBatches wbs(dm_context.storage_pool);

    auto pack = DeltaPackBlock::writePack(dm_context, block, 0, block.rows(), wbs);
    wbs.writeAll();

    if (delta->appendPack(dm_context, pack))
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
        LOG_WARNING(log, "Try to write an invalid delete range " << delete_range.toDebugString() << " into " << simpleInfo());
        return true;
    }

    LOG_TRACE(log, "Segment [" << segment_id << "] write delete range: " << delete_range.toDebugString());
    return delta->appendDeleteRange(dm_context, delete_range);
}

bool Segment::ingestPacks(DMContext & dm_context, const RowKeyRange & range, const DeltaPacks & packs, bool clear_data_in_range)
{
    auto new_range = range.shrink(rowkey_range);
    LOG_TRACE(log, "Segment [" << segment_id << "] write region snapshot: " << new_range.toDebugString());

    return delta->ingestPacks(dm_context, range, packs, clear_data_in_range);
}

SegmentSnapshotPtr Segment::createSnapshot(const DMContext & dm_context, bool for_update, CurrentMetrics::Metric metric) const
{
    // If the snapshot is created for read, then the snapshot will contain all packs (cached and persisted) for read.
    // If the snapshot is created for update, then the snapshot will only contain the persisted packs.
    auto delta_snap  = delta->createSnapshot(dm_context, for_update, metric);
    auto stable_snap = stable->createSnapshot();
    if (!delta_snap || !stable_snap)
        return {};
    return std::make_shared<SegmentSnapshot>(std::move(delta_snap), std::move(stable_snap));
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &          dm_context,
                                            const ColumnDefines &      columns_to_read,
                                            const SegmentSnapshotPtr & segment_snap,
                                            const RowKeyRanges &       read_ranges,
                                            const RSOperatorPtr &      filter,
                                            UInt64                     max_version,
                                            size_t                     expected_block_size)
{
    LOG_TRACE(log, "Segment [" << segment_id << "] [epoch=" << epoch << "] create InputStream");

    auto read_info = getReadInfo(dm_context, columns_to_read, segment_snap, read_ranges, max_version);

    auto create_stream = [&](const RowKeyRange & read_range) -> BlockInputStreamPtr {
        BlockInputStreamPtr stream;
        if (dm_context.read_delta_only)
        {
            throw Exception("Unsupported");
        }
        else if (dm_context.read_stable_only)
        {
            stream = segment_snap->stable->getInputStream(
                dm_context, *read_info.read_columns, read_range, filter, max_version, expected_block_size, false);
        }
        else if (segment_snap->delta->getRows() == 0 && segment_snap->delta->getDeletes() == 0 //
                 && !hasColumn(columns_to_read, EXTRA_HANDLE_COLUMN_ID)                        //
                 && !hasColumn(columns_to_read, VERSION_COLUMN_ID)                             //
                 && !hasColumn(columns_to_read, TAG_COLUMN_ID))
        {
            // No delta, let's try some optimizations.
            stream = segment_snap->stable->getInputStream(
                dm_context, *read_info.read_columns, read_range, filter, max_version, expected_block_size, true);
        }
        else
        {
            stream = getPlacedStream(dm_context,
                                     *read_info.read_columns,
                                     read_range,
                                     filter,
                                     segment_snap->stable,
                                     read_info.getDeltaReader(),
                                     read_info.index_begin,
                                     read_info.index_end,
                                     expected_block_size,
                                     max_version);
        }

        stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stream, read_range, 0);
        stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_MVCC>>(
            stream, columns_to_read, max_version, is_common_handle, dm_context.query_id);

        return stream;
    };

    BlockInputStreamPtr stream;
    if (read_ranges.size() == 1)
    {
        LOG_TRACE(log,
                  "Segment [" << segment_id << "] is read by max_version: " << max_version << ", 1"
                              << " range: " << DB::DM::toDebugString(read_ranges));
        RowKeyRange real_range = rowkey_range.shrink(read_ranges[0]);
        if (real_range.none())
            stream = std::make_shared<EmptyBlockInputStream>(toEmptyBlock(*read_info.read_columns));
        else
            stream = create_stream(real_range);
    }
    else
    {
        BlockInputStreams streams;
        for (auto & read_range : read_ranges)
        {
            RowKeyRange real_range = rowkey_range.shrink(read_range);
            if (!real_range.none())
                streams.push_back(create_stream(real_range));
        }

        LOG_TRACE(log,
                  "Segment [" << segment_id << "] is read by max_version: " << max_version << ", " << streams.size()
                              << " ranges: " << DB::DM::toDebugString(read_ranges));

        if (streams.empty())
            stream = std::make_shared<EmptyBlockInputStream>(toEmptyBlock(*read_info.read_columns));
        else
            stream = std::make_shared<ConcatBlockInputStream>(streams);
    }
    return stream;
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &     dm_context,
                                            const ColumnDefines & columns_to_read,
                                            const RowKeyRanges &  read_ranges,
                                            const RSOperatorPtr & filter,
                                            UInt64                max_version,
                                            size_t                expected_block_size)
{
    auto segment_snap = createSnapshot(dm_context, false, CurrentMetrics::DT_SnapshotOfRead);
    if (!segment_snap)
        return {};
    return getInputStream(dm_context, columns_to_read, segment_snap, read_ranges, filter, max_version, expected_block_size);
}

BlockInputStreamPtr Segment::getInputStreamForDataExport(const DMContext &          dm_context,
                                                         const ColumnDefines &      columns_to_read,
                                                         const SegmentSnapshotPtr & segment_snap,
                                                         const RowKeyRange &        data_range,
                                                         size_t                     expected_block_size,
                                                         bool                       reorgnize_block) const
{
    auto read_info = getReadInfo(dm_context, columns_to_read, segment_snap, {data_range});

    BlockInputStreamPtr data_stream = getPlacedStream(dm_context,
                                                      *read_info.read_columns,
                                                      data_range,
                                                      EMPTY_FILTER,
                                                      segment_snap->stable,
                                                      read_info.getDeltaReader(),
                                                      read_info.index_begin,
                                                      read_info.index_end,
                                                      expected_block_size);


    data_stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(data_stream, data_range, 0);
    if (reorgnize_block)
    {
        data_stream = std::make_shared<PKSquashingBlockInputStream<false>>(data_stream, EXTRA_HANDLE_COLUMN_ID, is_common_handle);
    }
    data_stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
        data_stream, *read_info.read_columns, dm_context.min_version, is_common_handle);

    return data_stream;
}

BlockInputStreamPtr Segment::getInputStreamRaw(const DMContext &          dm_context,
                                               const ColumnDefines &      columns_to_read,
                                               const SegmentSnapshotPtr & segment_snap,
                                               bool                       do_range_filter,
                                               size_t                     expected_block_size)
{
    auto new_columns_to_read = std::make_shared<ColumnDefines>();

    if (!do_range_filter)
    {
        (*new_columns_to_read) = columns_to_read;
    }
    else
    {
        new_columns_to_read->push_back(getExtraHandleColumnDefine(is_common_handle));

        for (const auto & c : columns_to_read)
        {
            if (c.id != EXTRA_HANDLE_COLUMN_ID)
                new_columns_to_read->push_back(c);
        }
    }


    BlockInputStreamPtr delta_stream = std::make_shared<DeltaValueInputStream>(dm_context, //
                                                                               segment_snap->delta,
                                                                               new_columns_to_read,
                                                                               this->rowkey_range);

    BlockInputStreamPtr stable_stream = segment_snap->stable->getInputStream(
        dm_context, *new_columns_to_read, rowkey_range, EMPTY_FILTER, MAX_UINT64, expected_block_size, false);

    if (do_range_filter)
    {
        delta_stream = std::make_shared<DMRowKeyFilterBlockInputStream<false>>(delta_stream, rowkey_range, 0);
        delta_stream = std::make_shared<DMColumnFilterBlockInputStream>(delta_stream, columns_to_read);

        stable_stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stable_stream, rowkey_range, 0);
        stable_stream = std::make_shared<DMColumnFilterBlockInputStream>(stable_stream, columns_to_read);
    }

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
    return std::make_shared<ConcatBlockInputStream>(streams);
}

BlockInputStreamPtr Segment::getInputStreamRaw(const DMContext & dm_context, const ColumnDefines & columns_to_read)
{
    auto segment_snap = createSnapshot(dm_context, false, CurrentMetrics::DT_SnapshotOfReadRaw);
    if (!segment_snap)
        return {};
    return getInputStreamRaw(dm_context, columns_to_read, segment_snap, true);
}

SegmentPtr Segment::mergeDelta(DMContext & dm_context, const ColumnDefinesPtr & schema_snap) const
{
    WriteBatches wbs(dm_context.storage_pool);
    auto         segment_snap = createSnapshot(dm_context, true, CurrentMetrics::DT_SnapshotOfDeltaMerge);
    if (!segment_snap)
        return {};

    auto new_stable = prepareMergeDelta(dm_context, schema_snap, segment_snap, wbs, false);

    wbs.writeLogAndData();
    new_stable->enableDMFilesGC();

    auto lock        = mustGetUpdateLock();
    auto new_segment = applyMergeDelta(dm_context, segment_snap, wbs, new_stable);

    wbs.writeAll();
    return new_segment;
}

StableValueSpacePtr Segment::prepareMergeDelta(DMContext &                dm_context,
                                               const ColumnDefinesPtr &   schema_snap,
                                               const SegmentSnapshotPtr & segment_snap,
                                               WriteBatches &             wbs,
                                               bool                       need_rate_limit) const
{
    LOG_INFO(log,
             "Segment [" << DB::toString(segment_id)
                         << "] prepare merge delta start. delta packs: " << DB::toString(segment_snap->delta->getPackCount())
                         << ", delta total rows: " << DB::toString(segment_snap->delta->getRows()));

    EventRecorder recorder(ProfileEvents::DMDeltaMerge, ProfileEvents::DMDeltaMergeNS);

    auto data_stream = getInputStreamForDataExport(
        dm_context, *schema_snap, segment_snap, rowkey_range, dm_context.stable_pack_rows, /*reorginize_block*/ true);

    auto new_stable = createNewStable(dm_context, schema_snap, data_stream, segment_snap->stable->getId(), wbs, need_rate_limit);

    LOG_INFO(log, "Segment [" << DB::toString(segment_id) << "] prepare merge delta done.");

    return new_stable;
}

SegmentPtr Segment::applyMergeDelta(DMContext &                 context,
                                    const SegmentSnapshotPtr &  segment_snap,
                                    WriteBatches &              wbs,
                                    const StableValueSpacePtr & new_stable) const
{
    LOG_INFO(log, "Before apply merge delta: " << info());

    auto later_packs = delta->checkHeadAndCloneTail(context, rowkey_range, segment_snap->delta->getPacks(), wbs);
    // Created references to tail pages' pages in "log" storage, we need to write them down.
    wbs.writeLogAndData();

    auto new_delta = std::make_shared<DeltaValueSpace>(delta->getId(), later_packs);
    new_delta->saveMeta(wbs);

    auto new_me = std::make_shared<Segment>(epoch + 1, //
                                            rowkey_range,
                                            segment_id,
                                            next_segment_id,
                                            new_delta,
                                            new_stable);

    // avoid recheck whether to do DeltaMerge using the same gc_safe_point
    new_me->setLastCheckGCSafePoint(context.min_version);

    // Store new meta data
    new_me->serialize(wbs.meta);

    // Remove old segment's delta.
    delta->recordRemovePacksPages(wbs);
    // Remove old stable's files.
    stable->recordRemovePacksPages(wbs);

    LOG_INFO(log, "After apply merge delta new segment: " << new_me->info());

    return new_me;
}

SegmentPair Segment::split(DMContext & dm_context, const ColumnDefinesPtr & schema_snap) const
{
    WriteBatches wbs(dm_context.storage_pool);
    auto         segment_snap = createSnapshot(dm_context, true, CurrentMetrics::DT_SnapshotOfSegmentSplit);
    if (!segment_snap)
        return {};

    auto split_info_opt = prepareSplit(dm_context, schema_snap, segment_snap, wbs, false);
    if (!split_info_opt.has_value())
        return {};

    auto & split_info = split_info_opt.value();

    wbs.writeLogAndData();
    split_info.my_stable->enableDMFilesGC();
    split_info.other_stable->enableDMFilesGC();

    auto lock         = mustGetUpdateLock();
    auto segment_pair = applySplit(dm_context, segment_snap, wbs, split_info);

    wbs.writeAll();

    return segment_pair;
}

std::optional<RowKeyValue> Segment::getSplitPointFast(DMContext & dm_context, const StableSnapshotPtr & stable_snap) const
{
    // FIXME: this method does not consider invalid packs in stable dmfiles.

    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);
    auto          stable_rows = stable_snap->getRows();
    if (unlikely(!stable_rows))
        throw Exception("No stable rows");

    size_t split_row_index = stable_rows / 2;

    auto & dmfiles = stable_snap->getDMFiles();

    DMFilePtr read_file;
    size_t    file_index       = 0;
    auto      read_pack        = std::make_shared<IdSet>();
    size_t    read_row_in_pack = 0;

    size_t cur_rows = 0;
    for (size_t index = 0; index < dmfiles.size(); index++)
    {
        auto & file         = dmfiles[index];
        size_t rows_in_file = file->getRows();
        cur_rows += rows_in_file;
        if (cur_rows > split_row_index)
        {
            cur_rows -= rows_in_file;
            auto & pack_stats = file->getPackStats();
            for (size_t pack_id = 0; pack_id < pack_stats.size(); ++pack_id)
            {
                cur_rows += pack_stats[pack_id].rows;
                if (cur_rows > split_row_index)
                {
                    cur_rows -= pack_stats[pack_id].rows;

                    read_file  = file;
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

    DMFileBlockInputStream stream(dm_context.db_context,
                                  MAX_UINT64,
                                  false,
                                  dm_context.hash_salt,
                                  read_file,
                                  {getExtraHandleColumnDefine(is_common_handle)},
                                  RowKeyRange::newAll(is_common_handle, rowkey_column_size),
                                  EMPTY_FILTER,
                                  stable_snap->getColumnCaches()[file_index],
                                  read_pack);

    stream.readPrefix();
    auto block = stream.read();
    if (!block)
        throw Exception("Unexpected empty block");
    stream.readSuffix();

    RowKeyColumnContainer rowkey_column(block.getByPosition(0).column, is_common_handle);
    RowKeyValue           split_point(rowkey_column.getRowKeyValue(read_row_in_pack));


    if (!rowkey_range.check(split_point.toRowKeyValueRef())
        || RowKeyRange(rowkey_range.start, split_point, is_common_handle, rowkey_column_size).none()
        || RowKeyRange(split_point, rowkey_range.end, is_common_handle, rowkey_column_size).none())
    {
        LOG_WARNING(log,
                    __FUNCTION__ << " unexpected split_handle: " << split_point.toRowKeyValueRef().toDebugString()
                                 << ", should be in range " << rowkey_range.toDebugString() << ", cur_rows: " << cur_rows
                                 << ", read_row_in_pack: " << read_row_in_pack << ", file_index: " << file_index);
        return {};
    }

    return {split_point};
}

std::optional<RowKeyValue>
Segment::getSplitPointSlow(DMContext & dm_context, const ReadInfo & read_info, const SegmentSnapshotPtr & segment_snap) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);

    auto & pk_col      = getExtraHandleColumnDefine(is_common_handle);
    auto   pk_col_defs = std::make_shared<ColumnDefines>(ColumnDefines{pk_col});
    // We need to create a new delta_reader here, because the one in read_info is used to read columns other than PK column.
    auto delta_reader = read_info.getDeltaReader(pk_col_defs);

    size_t exact_rows = 0;

    {
        BlockInputStreamPtr stream = getPlacedStream(dm_context,
                                                     *pk_col_defs,
                                                     rowkey_range,
                                                     EMPTY_FILTER,
                                                     segment_snap->stable,
                                                     delta_reader,
                                                     read_info.index_begin,
                                                     read_info.index_end,
                                                     dm_context.stable_pack_rows);

        stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stream, rowkey_range, 0);

        stream->readPrefix();
        Block block;
        while ((block = stream->read()))
            exact_rows += block.rows();
        stream->readSuffix();
    }

    if (exact_rows == 0)
    {
        LOG_WARNING(log, __FUNCTION__ << " Segment " << info() << " has no rows, should not split.");
        return {};
    }

    BlockInputStreamPtr stream = getPlacedStream(dm_context,
                                                 *pk_col_defs,
                                                 rowkey_range,
                                                 EMPTY_FILTER,
                                                 segment_snap->stable,
                                                 delta_reader,
                                                 read_info.index_begin,
                                                 read_info.index_end,
                                                 dm_context.stable_pack_rows);

    stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stream, rowkey_range, 0);

    size_t      split_row_index = exact_rows / 2;
    RowKeyValue split_point;
    size_t      count = 0;

    stream->readPrefix();
    while (true)
    {
        Block block = stream->read();
        if (!block)
            break;
        count += block.rows();
        if (count > split_row_index)
        {
            size_t                offset_in_block = block.rows() - (count - split_row_index);
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
        LOG_WARNING(log,
                    __FUNCTION__ << " unexpected split_handle: " << split_point.toRowKeyValueRef().toDebugString()
                                 << ", should be in range " << rowkey_range.toDebugString() << ", exact_rows: " << DB::toString(exact_rows)
                                 << ", cur count: " << DB::toString(count) << ", split_row_index: " << split_row_index);
        return {};
    }

    return {split_point};
}

std::optional<Segment::SplitInfo> Segment::prepareSplit(DMContext &                dm_context,
                                                        const ColumnDefinesPtr &   schema_snap,
                                                        const SegmentSnapshotPtr & segment_snap,
                                                        WriteBatches &             wbs,
                                                        bool                       need_rate_limit) const
{
    if (!dm_context.enable_logical_split         //
        || segment_snap->stable->getPacks() <= 3 //
        || segment_snap->delta->getRows() > segment_snap->stable->getRows())
    {
        return prepareSplitPhysical(dm_context, schema_snap, segment_snap, wbs, need_rate_limit);
    }
    else
    {
        auto split_point_opt = getSplitPointFast(dm_context, segment_snap->stable);

        bool bad_split_point = !split_point_opt.has_value() || !rowkey_range.check(split_point_opt->toRowKeyValueRef())
            || compare(split_point_opt->toRowKeyValueRef(), rowkey_range.getStart()) == 0;
        if (bad_split_point)
        {
            LOG_INFO(
                log,
                "Got bad split point [" << (split_point_opt.has_value() ? split_point_opt->toRowKeyValueRef().toDebugString() : "no value")
                                        << "] for segment " << info() << ", fall back to split physical.");
            return prepareSplitPhysical(dm_context, schema_snap, segment_snap, wbs, need_rate_limit);
        }
        else
            return prepareSplitLogical(dm_context, schema_snap, segment_snap, split_point_opt.value(), wbs);
    }
}

std::optional<Segment::SplitInfo> Segment::prepareSplitLogical(DMContext & dm_context,
                                                               const ColumnDefinesPtr & /*schema_snap*/,
                                                               const SegmentSnapshotPtr & segment_snap,
                                                               RowKeyValue &              split_point,
                                                               WriteBatches &             wbs) const
{
    LOG_INFO(log, "Segment [" << segment_id << "] prepare split logical start");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & storage_pool = dm_context.storage_pool;

    RowKeyRange my_range(rowkey_range.start, split_point, is_common_handle, rowkey_column_size);
    RowKeyRange other_range(split_point, rowkey_range.end, is_common_handle, rowkey_column_size);

    if (my_range.none() || other_range.none())
    {
        LOG_WARNING(log,
                    __FUNCTION__ << ": unexpected range! my_range: " << my_range.toDebugString()
                                 << ", other_range: " << other_range.toDebugString() << ", aborted");
        return {};
    }

    GenPageId log_gen_page_id = std::bind(&StoragePool::newLogPageId, &storage_pool);

    DMFiles my_stable_files;
    DMFiles other_stable_files;

    auto delegate = dm_context.path_pool.getStableDiskDelegator();
    for (auto & dmfile : segment_snap->stable->getDMFiles())
    {
        auto ori_ref_id       = dmfile->refId();
        auto file_id          = dmfile->fileId();
        auto file_parent_path = delegate.getDTFilePath(file_id);

        auto my_dmfile_id    = storage_pool.newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);
        auto other_dmfile_id = storage_pool.newDataPageIdForDTFile(delegate, __PRETTY_FUNCTION__);

        wbs.data.putRefPage(my_dmfile_id, file_id);
        wbs.data.putRefPage(other_dmfile_id, file_id);
        wbs.removed_data.delPage(ori_ref_id);

        auto my_dmfile = DMFile::restore(dm_context.db_context.getFileProvider(), file_id, /* ref_id= */ my_dmfile_id, file_parent_path);
        auto other_dmfile
            = DMFile::restore(dm_context.db_context.getFileProvider(), file_id, /* ref_id= */ other_dmfile_id, file_parent_path);

        my_stable_files.push_back(my_dmfile);
        other_stable_files.push_back(other_dmfile);
    }

    auto other_stable_id = storage_pool.newMetaPageId();

    auto my_stable    = std::make_shared<StableValueSpace>(segment_snap->stable->getId());
    auto other_stable = std::make_shared<StableValueSpace>(other_stable_id);

    my_stable->setFiles(my_stable_files, my_range, &dm_context);
    other_stable->setFiles(other_stable_files, other_range, &dm_context);

    LOG_INFO(log, "Segment [" << segment_id << "] prepare split logical done");

    return {SplitInfo{true, split_point, my_stable, other_stable}};
}

std::optional<Segment::SplitInfo> Segment::prepareSplitPhysical(DMContext &                dm_context,
                                                                const ColumnDefinesPtr &   schema_snap,
                                                                const SegmentSnapshotPtr & segment_snap,
                                                                WriteBatches &             wbs,
                                                                bool                       need_rate_limit) const
{
    LOG_INFO(log, "Segment [" << segment_id << "] prepare split physical start");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto read_info       = getReadInfo(dm_context, *schema_snap, segment_snap, {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
    auto split_point_opt = getSplitPointSlow(dm_context, read_info, segment_snap);
    if (!split_point_opt.has_value())
        return {};

    const auto & split_point = split_point_opt.value();

    RowKeyRange my_range(rowkey_range.start, split_point, is_common_handle, rowkey_column_size);
    RowKeyRange other_range(split_point, rowkey_range.end, is_common_handle, rowkey_column_size);

    if (my_range.none() || other_range.none())
    {
        LOG_WARNING(log,
                    __FUNCTION__ << ": unexpected range! my_range: " << my_range.toDebugString()
                                 << ", other_range: " << other_range.toDebugString() << ", aborted");
        return {};
    }

    StableValueSpacePtr my_new_stable;
    StableValueSpacePtr other_stable;

    {
        // Write my data
        LOG_DEBUG(log, "Created my placed stream");

        auto my_delta_reader = read_info.getDeltaReader(schema_snap);

        BlockInputStreamPtr my_data = getPlacedStream(dm_context,
                                                      *read_info.read_columns,
                                                      my_range,
                                                      EMPTY_FILTER,
                                                      segment_snap->stable,
                                                      my_delta_reader,
                                                      read_info.index_begin,
                                                      read_info.index_end,
                                                      dm_context.stable_pack_rows);


        my_data = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(my_data, my_range, 0);
        my_data = std::make_shared<PKSquashingBlockInputStream<false>>(my_data, EXTRA_HANDLE_COLUMN_ID, is_common_handle);
        my_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            my_data, *read_info.read_columns, dm_context.min_version, is_common_handle);
        auto my_stable_id = segment_snap->stable->getId();
        my_new_stable     = createNewStable(dm_context, schema_snap, my_data, my_stable_id, wbs, need_rate_limit);
    }

    LOG_INFO(log, "prepare my_new_stable done");

    {
        // Write new segment's data
        LOG_DEBUG(log, "Created other placed stream");

        auto other_delta_reader = read_info.getDeltaReader(schema_snap);

        BlockInputStreamPtr other_data = getPlacedStream(dm_context,
                                                         *read_info.read_columns,
                                                         other_range,
                                                         EMPTY_FILTER,
                                                         segment_snap->stable,
                                                         other_delta_reader,
                                                         read_info.index_begin,
                                                         read_info.index_end,
                                                         dm_context.stable_pack_rows);


        other_data = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(other_data, other_range, 0);
        other_data = std::make_shared<PKSquashingBlockInputStream<false>>(other_data, EXTRA_HANDLE_COLUMN_ID, is_common_handle);
        other_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            other_data, *read_info.read_columns, dm_context.min_version, is_common_handle);
        auto other_stable_id = dm_context.storage_pool.newMetaPageId();
        other_stable         = createNewStable(dm_context, schema_snap, other_data, other_stable_id, wbs, need_rate_limit);
    }

    LOG_INFO(log, "prepare other_stable done");

    // Remove old stable's files.
    for (auto & file : stable->getDMFiles())
    {
        // Here we should remove the ref id instead of file_id.
        // Because a dmfile could be used by several segments, and only after all ref_ids are removed, then the file_id removed.
        wbs.removed_data.delPage(file->refId());
    }

    LOG_INFO(log, "Segment [" << segment_id << "] prepare split physical done");

    return {SplitInfo{false, split_point, my_new_stable, other_stable}};
}

SegmentPair Segment::applySplit(DMContext &                dm_context, //
                                const SegmentSnapshotPtr & segment_snap,
                                WriteBatches &             wbs,
                                SplitInfo &                split_info) const
{
    LOG_INFO(log, "Segment [" << segment_id << "] apply split");

    RowKeyRange  my_range(rowkey_range.start, split_info.split_point, is_common_handle, rowkey_column_size);
    RowKeyRange  other_range(split_info.split_point, rowkey_range.end, is_common_handle, rowkey_column_size);
    DeltaPacks   empty_packs;
    DeltaPacks * head_packs = split_info.is_logical ? &empty_packs : &segment_snap->delta->getPacks();

    auto my_delta_packs    = delta->checkHeadAndCloneTail(dm_context, my_range, *head_packs, wbs);
    auto other_delta_packs = delta->checkHeadAndCloneTail(dm_context, other_range, *head_packs, wbs);

    // Created references to tail pages' pages in "log" storage, we need to write them down.
    wbs.writeLogAndData();

    auto other_segment_id = dm_context.storage_pool.newMetaPageId();
    auto other_delta_id   = dm_context.storage_pool.newMetaPageId();

    auto my_delta    = std::make_shared<DeltaValueSpace>(delta->getId(), my_delta_packs);
    auto other_delta = std::make_shared<DeltaValueSpace>(other_delta_id, other_delta_packs);

    auto new_me = std::make_shared<Segment>(this->epoch + 1, //
                                            my_range,
                                            this->segment_id,
                                            other_segment_id,
                                            my_delta,
                                            split_info.my_stable);

    auto other = std::make_shared<Segment>(INITIAL_EPOCH, //
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
    delta->recordRemovePacksPages(wbs);
    // Remove old stable's files.
    stable->recordRemovePacksPages(wbs);

    LOG_INFO(log, "Segment " << info() << " split into " << new_me->info() << " and " << other->info());

    return {new_me, other};
}

SegmentPtr Segment::merge(DMContext & dm_context, const ColumnDefinesPtr & schema_snap, const SegmentPtr & left, const SegmentPtr & right)
{
    WriteBatches wbs(dm_context.storage_pool);
    /// This segment may contain some rows that not belong to this segment range which is left by previous split operation.
    /// And only saved data in this segment will be filtered by the segment range in the merge process,
    /// unsaved data will be directly copied to the new segment.
    /// So we flush here to make sure that all potential data left by previous split operation is saved.
    while (!left->flushCache(dm_context))
    {
        // keep flush until success if not abandoned
        if (left->hasAbandoned())
        {
            LOG_DEBUG(left->log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return {};
        }
    }
    while (!right->flushCache(dm_context))
    {
        // keep flush until success if not abandoned
        if (right->hasAbandoned())
        {
            LOG_DEBUG(right->log, "Give up merge segments left [" << left->segmentId() << "], right [" << right->segmentId() << "]");
            return {};
        }
    }

    auto left_snap  = left->createSnapshot(dm_context, true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
    auto right_snap = right->createSnapshot(dm_context, true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
    if (!left_snap || !right_snap)
        return {};

    auto merged_stable = prepareMerge(dm_context, schema_snap, left, left_snap, right, right_snap, wbs, false);

    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC();

    auto left_lock  = left->mustGetUpdateLock();
    auto right_lock = right->mustGetUpdateLock();

    auto merged = applyMerge(dm_context, left, left_snap, right, right_snap, wbs, merged_stable);

    wbs.writeAll();
    return merged;
}

/// Segments may contain some rows that not belong to its range which is left by previous split operation.
/// And only saved data in the segment will be filtered by the segment range in the merge process,
/// unsaved data will be directly copied to the new segment.
/// So remember to do a flush for the segments before merge.
StableValueSpacePtr Segment::prepareMerge(DMContext &                dm_context, //
                                          const ColumnDefinesPtr &   schema_snap,
                                          const SegmentPtr &         left,
                                          const SegmentSnapshotPtr & left_snap,
                                          const SegmentPtr &         right,
                                          const SegmentSnapshotPtr & right_snap,
                                          WriteBatches &             wbs,
                                          bool                       need_rate_limit)
{
    LOG_INFO(left->log, "Segment [" << left->segmentId() << "] and [" << right->segmentId() << "] prepare merge start");

    if (unlikely(compare(left->rowkey_range.getEnd(), right->rowkey_range.getStart()) != 0 || left->next_segment_id != right->segment_id))
        throw Exception("The ranges of merge segments are not consecutive: first end: " + left->rowkey_range.getEnd().toDebugString()
                        + ", second start: " + right->rowkey_range.getStart().toDebugString());

    auto getStream = [&](const SegmentPtr & segment, const SegmentSnapshotPtr & segment_snap) {
        auto read_info = segment->getReadInfo(
            dm_context, *schema_snap, segment_snap, {RowKeyRange::newAll(left->is_common_handle, left->rowkey_column_size)});
        BlockInputStreamPtr stream = getPlacedStream(dm_context,
                                                     *read_info.read_columns,
                                                     segment->rowkey_range,
                                                     EMPTY_FILTER,
                                                     segment_snap->stable,
                                                     read_info.getDeltaReader(),
                                                     read_info.index_begin,
                                                     read_info.index_end,
                                                     dm_context.stable_pack_rows);

        stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(stream, segment->rowkey_range, 0);
        stream = std::make_shared<PKSquashingBlockInputStream<false>>(stream, EXTRA_HANDLE_COLUMN_ID, dm_context.is_common_handle);
        stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            stream, *read_info.read_columns, dm_context.min_version, dm_context.is_common_handle);

        return stream;
    };

    auto left_stream  = getStream(left, left_snap);
    auto right_stream = getStream(right, right_snap);

    BlockInputStreamPtr merged_stream = std::make_shared<ConcatBlockInputStream>(BlockInputStreams{left_stream, right_stream});
    // for the purpose to calculate StableProperty of the new segment
    merged_stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
        merged_stream, *schema_snap, dm_context.min_version, dm_context.is_common_handle);

    auto merged_stable_id = left->stable->getId();
    auto merged_stable    = createNewStable(dm_context, schema_snap, merged_stream, merged_stable_id, wbs, need_rate_limit);

    LOG_INFO(left->log, "Segment [" << left->segmentId() << "] and [" << right->segmentId() << "] prepare merge done");

    return merged_stable;
}

SegmentPtr Segment::applyMerge(DMContext &                 dm_context, //
                               const SegmentPtr &          left,
                               const SegmentSnapshotPtr &  left_snap,
                               const SegmentPtr &          right,
                               const SegmentSnapshotPtr &  right_snap,
                               WriteBatches &              wbs,
                               const StableValueSpacePtr & merged_stable)
{
    LOG_INFO(left->log, "Segment [" << left->segmentId() << "] and [" << right->segmentId() << "] apply merge");

    RowKeyRange merged_range(left->rowkey_range.start, right->rowkey_range.end, left->is_common_handle, left->rowkey_column_size);

    auto left_tail_packs  = left->delta->checkHeadAndCloneTail(dm_context, merged_range, left_snap->delta->getPacks(), wbs);
    auto right_tail_packs = right->delta->checkHeadAndCloneTail(dm_context, merged_range, right_snap->delta->getPacks(), wbs);

    // Created references to tail pages' pages in "log" storage, we need to write them down.
    wbs.writeLogAndData();

    /// Make sure saved packs are appended before unsaved packs.
    DeltaPacks merged_packs;

    auto L_first_unsaved
        = std::find_if(left_tail_packs.begin(), left_tail_packs.end(), [](const DeltaPackPtr & p) { return !p->isSaved(); });
    auto R_first_unsaved
        = std::find_if(right_tail_packs.begin(), right_tail_packs.end(), [](const DeltaPackPtr & p) { return !p->isSaved(); });

    merged_packs.insert(merged_packs.end(), left_tail_packs.begin(), L_first_unsaved);
    merged_packs.insert(merged_packs.end(), right_tail_packs.begin(), R_first_unsaved);

    merged_packs.insert(merged_packs.end(), L_first_unsaved, left_tail_packs.end());
    merged_packs.insert(merged_packs.end(), R_first_unsaved, right_tail_packs.end());

    auto merged_delta = std::make_shared<DeltaValueSpace>(left->delta->getId(), merged_packs);

    auto merged = std::make_shared<Segment>(left->epoch + 1, //
                                            merged_range,
                                            left->segment_id,
                                            right->next_segment_id,
                                            merged_delta,
                                            merged_stable);

    // Store new meta data
    merged->delta->saveMeta(wbs);
    merged->stable->saveMeta(wbs.meta);
    merged->serialize(wbs.meta);

    left->delta->recordRemovePacksPages(wbs);
    left->stable->recordRemovePacksPages(wbs);

    right->delta->recordRemovePacksPages(wbs);
    right->stable->recordRemovePacksPages(wbs);

    wbs.removed_meta.delPage(right->segmentId());
    wbs.removed_meta.delPage(right->delta->getId());
    wbs.removed_meta.delPage(right->stable->getId());

    LOG_INFO(left->log, "Segment [" << left->info() << "] and [" << right->info() << "] merged into " << merged->info());

    return merged;
}

void Segment::check(DMContext &, const String &) const {}

bool Segment::flushCache(DMContext & dm_context)
{
    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_DeltaFlush};
    GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_delta_flush).Increment();
    Stopwatch watch;
    SCOPE_EXIT(
        { GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_delta_flush).Observe(watch.elapsedSeconds()); });

    return delta->flush(dm_context);
}

bool Segment::compactDelta(DMContext & dm_context)
{
    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_DeltaCompact};
    GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_delta_compact).Increment();
    Stopwatch watch;
    SCOPE_EXIT(
        { GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_delta_compact).Observe(watch.elapsedSeconds()); });

    return delta->compact(dm_context);
}

void Segment::placeDeltaIndex(DMContext & dm_context)
{
    // Update delta-index with persisted packs.
    auto segment_snap = createSnapshot(dm_context, /*for_update=*/true, CurrentMetrics::DT_SnapshotOfPlaceIndex);
    if (!segment_snap)
        return;
    getReadInfo(dm_context,
                /*read_columns=*/{getExtraHandleColumnDefine(is_common_handle)},
                segment_snap,
                {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
}

String Segment::simpleInfo() const
{
    return "{" + DB::toString(segment_id) + ":" + rowkey_range.toDebugString() + "}";
}

String Segment::info() const
{
    std::stringstream s;
    s << "{[id:" << segment_id << "], [next:" << next_segment_id << "], [epoch:" << epoch << "], [range:" << rowkey_range.toDebugString()
      << "], [rowkey_range:" << rowkey_range.toDebugString() << "], [delta rows:" << delta->getRows()
      << "], [delete ranges:" << delta->getDeletes() << "], [stable(" << stable->getDMFilesString() << "):" << stable->getRows() << "]}";
    return s.str();
}

Segment::ReadInfo Segment::getReadInfo(const DMContext &          dm_context,
                                       const ColumnDefines &      read_columns,
                                       const SegmentSnapshotPtr & segment_snap,
                                       const RowKeyRanges &       read_ranges,
                                       UInt64                     max_version) const
{
    LOG_DEBUG(log, __FUNCTION__ << " start");

    auto new_read_columns = arrangeReadColumns(getExtraHandleColumnDefine(is_common_handle), read_columns);
    auto pk_ver_col_defs
        = std::make_shared<ColumnDefines>(ColumnDefines{getExtraHandleColumnDefine(dm_context.is_common_handle), getVersionColumnDefine()});
    // Create a reader only for pk and version columns.
    auto delta_reader = std::make_shared<DeltaValueReader>(dm_context, segment_snap->delta, pk_ver_col_defs, this->rowkey_range);

    auto [my_delta_index, fully_indexed] = ensurePlace(dm_context, segment_snap->stable, delta_reader, read_ranges, max_version);
    auto compacted_index                 = my_delta_index->getDeltaTree()->getCompactedEntries();


    // Hold compacted_index reference, to prevent it from deallocated.
    delta_reader->setDeltaIndex(compacted_index);

    LOG_DEBUG(log, __FUNCTION__ << " end");

    if (fully_indexed)
    {
        // Try update shared index, if my_delta_index is more advanced.
        bool ok = segment_snap->delta->getSharedDeltaIndex()->updateIfAdvanced(*my_delta_index);
        if (ok)
            LOG_DEBUG(log, simpleInfo() << " Updated delta index");
    }

    // Refresh the reference in DeltaIndexManager, so that the index can be properly managed.
    if (auto manager = dm_context.db_context.getDeltaIndexManager(); manager)
        manager->refreshRef(segment_snap->delta->getSharedDeltaIndex());

    return ReadInfo(delta_reader->createNewReader(new_read_columns), compacted_index->begin(), compacted_index->end(), new_read_columns);
}

ColumnDefinesPtr Segment::arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read)
{
    // We always put handle, version and tag column at the beginning of columns.
    ColumnDefines new_columns_to_read;

    new_columns_to_read.push_back(handle);
    new_columns_to_read.push_back(getVersionColumnDefine());
    new_columns_to_read.push_back(getTagColumnDefine());

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        auto & c = columns_to_read[i];
        if (c.id != handle.id && c.id != VERSION_COLUMN_ID && c.id != TAG_COLUMN_ID)
            new_columns_to_read.push_back(c);
    }

    return std::make_shared<ColumnDefines>(std::move(new_columns_to_read));
}

template <bool skippable_place, class IndexIterator>
SkippableBlockInputStreamPtr Segment::getPlacedStream(const DMContext &           dm_context,
                                                      const ColumnDefines &       read_columns,
                                                      const RowKeyRange &         rowkey_range,
                                                      const RSOperatorPtr &       filter,
                                                      const StableSnapshotPtr &   stable_snap,
                                                      const DeltaValueReaderPtr & delta_reader,
                                                      const IndexIterator &       delta_index_begin,
                                                      const IndexIterator &       delta_index_end,
                                                      size_t                      expected_block_size,
                                                      UInt64                      max_version)
{
    SkippableBlockInputStreamPtr stable_input_stream
        = stable_snap->getInputStream(dm_context, read_columns, rowkey_range, filter, max_version, expected_block_size, false);
    return std::make_shared<DeltaMergeBlockInputStream<DeltaValueReader, IndexIterator, skippable_place>>( //
        stable_input_stream,
        delta_reader,
        delta_index_begin,
        delta_index_end,
        rowkey_range,
        expected_block_size);
}

std::pair<DeltaIndexPtr, bool> Segment::ensurePlace(const DMContext &           dm_context,
                                                    const StableSnapshotPtr &   stable_snap,
                                                    const DeltaValueReaderPtr & delta_reader,
                                                    const RowKeyRanges &        read_ranges,
                                                    UInt64                      max_version) const
{
    auto delta_snap = delta_reader->getDeltaSnap();
    // Clone a new delta index.
    auto my_delta_index = delta_snap->getSharedDeltaIndex()->tryClone(delta_snap->getRows(), delta_snap->getDeletes());
    auto my_delta_tree  = my_delta_index->getDeltaTree();

    // Note that, when enable_relevant_place is false , we cannot use the range of this segment.
    // Because some block / delete ranges could contain some data / range that are not belong to current segment.
    // If we use the range of this segment as relevant_range, fully_indexed will always be false in those cases.
    RowKeyRange relevant_range = dm_context.enable_relevant_place ? mergeRanges(read_ranges, is_common_handle, rowkey_column_size)
                                                                  : RowKeyRange::newAll(is_common_handle, rowkey_column_size);

    auto [my_placed_rows, my_placed_deletes] = my_delta_index->getPlacedStatus();

    // Let's do a fast check, determine whether we need to do place or not.
    if (!delta_reader->shouldPlace(dm_context, my_delta_index, rowkey_range, relevant_range, max_version))
        return {my_delta_index, false};

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_PlaceIndexUpdate};
    GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_place_index_update).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_place_index_update).Observe(watch.elapsedSeconds());
    });

    EventRecorder recorder(ProfileEvents::DMPlace, ProfileEvents::DMPlaceNS);

    auto items = delta_reader->getPlaceItems(my_placed_rows, my_placed_deletes, delta_snap->getRows(), delta_snap->getDeletes());

    bool fully_indexed = true;
    for (auto & v : items)
    {
        if (v.isBlock())
        {
            auto block  = v.getBlock();
            auto offset = v.getBlockOffset();
            auto rows   = block.rows();

            if (unlikely(my_placed_rows != offset))
                throw Exception("Place block offset not match", ErrorCodes::LOGICAL_ERROR);

            if (dm_context.enable_skippable_place)
                fully_indexed
                    &= placeUpsert<true>(dm_context, stable_snap, delta_reader, offset, std::move(block), *my_delta_tree, relevant_range);
            else
                fully_indexed
                    &= placeUpsert<false>(dm_context, stable_snap, delta_reader, offset, std::move(block), *my_delta_tree, relevant_range);

            my_placed_rows += rows;
        }
        else
        {
            if (dm_context.enable_skippable_place)
                fully_indexed
                    &= placeDelete<true>(dm_context, stable_snap, delta_reader, v.getDeleteRange(), *my_delta_tree, relevant_range);
            else
                fully_indexed
                    &= placeDelete<false>(dm_context, stable_snap, delta_reader, v.getDeleteRange(), *my_delta_tree, relevant_range);

            ++my_placed_deletes;
        }
    }

    if (unlikely(my_placed_rows != delta_snap->getRows() || my_placed_deletes != delta_snap->getDeletes()))
        throw Exception("Placed status not match! Expected place rows:" + DB::toString(delta_snap->getRows())
                        + ", deletes:" + DB::toString(delta_snap->getDeletes())
                        + ", but actually placed rows:" + DB::toString(my_placed_rows) + ", deletes:" + DB::toString(my_placed_deletes));

    my_delta_index->update(my_delta_tree, my_placed_rows, my_placed_deletes);

    LOG_DEBUG(log,
              __FUNCTION__ << simpleInfo() << " read_ranges:" << DB::DM::toDebugString(read_ranges) << ", place item count:" << items.size()
                           << ", shared delta index: " << delta_snap->getSharedDeltaIndex()->toString()
                           << ", my delta index: " << my_delta_index->toString());

    return {my_delta_index, fully_indexed};
}

template <bool skippable_place>
bool Segment::placeUpsert(const DMContext &           dm_context,
                          const StableSnapshotPtr &   stable_snap,
                          const DeltaValueReaderPtr & delta_reader,
                          size_t                      delta_value_space_offset,
                          Block &&                    block,
                          DeltaTree &                 update_delta_tree,
                          const RowKeyRange &         relevant_range) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceUpsert, ProfileEvents::DMPlaceUpsertNS);

    IColumn::Permutation perm;

    auto &         handle       = getExtraHandleColumnDefine(is_common_handle);
    bool           do_sort      = sortBlockByPk(handle, block, perm);
    RowKeyValueRef first_rowkey = RowKeyColumnContainer(block.getByPosition(0).column, is_common_handle).getRowKeyValue(0);
    RowKeyValueRef range_start  = relevant_range.getStart();

    auto place_handle_range = skippable_place ? RowKeyRange::startFrom(max(first_rowkey, range_start), is_common_handle, rowkey_column_size)
                                              : RowKeyRange::newAll(is_common_handle, rowkey_column_size);

    auto compacted_index = update_delta_tree.getCompactedEntries();

    auto merged_stream = getPlacedStream<skippable_place>( //
        dm_context,
        {handle, getVersionColumnDefine()},
        place_handle_range,
        EMPTY_FILTER,
        stable_snap,
        delta_reader,
        compacted_index->begin(),
        compacted_index->end(),
        dm_context.stable_pack_rows);

    if (do_sort)
        return DM::placeInsert<true>(
            merged_stream, block, relevant_range, update_delta_tree, delta_value_space_offset, perm, getPkSort(handle));
    else
        return DM::placeInsert<false>(
            merged_stream, block, relevant_range, update_delta_tree, delta_value_space_offset, perm, getPkSort(handle));
}

template <bool skippable_place>
bool Segment::placeDelete(const DMContext &           dm_context,
                          const StableSnapshotPtr &   stable_snap,
                          const DeltaValueReaderPtr & delta_reader,
                          const RowKeyRange &         delete_range,
                          DeltaTree &                 update_delta_tree,
                          const RowKeyRange &         relevant_range) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceDeleteRange, ProfileEvents::DMPlaceDeleteRangeNS);

    auto & handle = getExtraHandleColumnDefine(is_common_handle);

    Blocks delete_data;
    {
        auto compacted_index = update_delta_tree.getCompactedEntries();

        BlockInputStreamPtr delete_stream = getPlacedStream( //
            dm_context,
            {handle, getVersionColumnDefine()},
            delete_range,
            EMPTY_FILTER,
            stable_snap,
            delta_reader,
            compacted_index->begin(),
            compacted_index->end(),
            dm_context.stable_pack_rows);

        delete_stream = std::make_shared<DMRowKeyFilterBlockInputStream<true>>(delete_stream, delete_range, 0);

        // Try to merge into big block. 128 MB should be enough.
        SquashingBlockInputStream squashed_delete_stream(delete_stream, 0, 128 * (1UL << 20));

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
        RowKeyValueRef first_rowkey       = RowKeyColumnContainer(block.getByPosition(0).column, is_common_handle).getRowKeyValue(0);
        auto           place_handle_range = skippable_place ? RowKeyRange::startFrom(first_rowkey, is_common_handle, rowkey_column_size)
                                                            : RowKeyRange::newAll(is_common_handle, rowkey_column_size);

        auto compacted_index = update_delta_tree.getCompactedEntries();

        auto merged_stream = getPlacedStream<skippable_place>( //
            dm_context,
            {handle, getVersionColumnDefine()},
            place_handle_range,
            EMPTY_FILTER,
            stable_snap,
            delta_reader,
            compacted_index->begin(),
            compacted_index->end(),
            dm_context.stable_pack_rows);
        fully_indexed &= DM::placeDelete(merged_stream, block, relevant_range, update_delta_tree, getPkSort(handle));
    }
    return fully_indexed;
}

} // namespace DM
} // namespace DB
