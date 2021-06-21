#include <Common/TiFlashMetrics.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/EmptyBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMDecoratorStreams.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMerge.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaPlace.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/ReorganizeBlockInputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

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

const Segment::Version Segment::CURRENT_VERSION = 1;
const static size_t    SEGMENT_BUFFER_SIZE      = 128; // More than enough.

DMFilePtr writeIntoNewDMFile(DMContext &                 dm_context, //
                             const ColumnDefinesPtr &    schema_snap,
                             const BlockInputStreamPtr & input_stream,
                             UInt64                      file_id,
                             const String &              parent_path)
{
    auto   dmfile        = DMFile::create(file_id, parent_path);
    auto   output_stream = std::make_shared<DMFileBlockOutputStream>(dm_context.db_context, dmfile, *schema_snap);
    auto * mvcc_stream   = typeid_cast<const DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT> *>(input_stream.get());

    input_stream->readPrefix();
    output_stream->writePrefix();
    while (true)
    {
        size_t last_not_clean_rows = 0;
        if (mvcc_stream)
            last_not_clean_rows = mvcc_stream->getNotCleanRows();

        Block block = input_stream->read();
        if (!block)
            break;
        if (!block.rows())
            continue;

        size_t cur_not_clean_rows = 1;
        if (mvcc_stream)
            cur_not_clean_rows = mvcc_stream->getNotCleanRows();

        output_stream->write(block, cur_not_clean_rows - last_not_clean_rows);
    }

    input_stream->readSuffix();
    output_stream->writeSuffix();

    return dmfile;
}

StableValueSpacePtr createNewStable(DMContext &                 context,
                                    const ColumnDefinesPtr &    schema_snap,
                                    const BlockInputStreamPtr & input_stream,
                                    PageId                      stable_id,
                                    WriteBatches &              wbs)
{
    auto delegate   = context.path_pool.getStableDiskDelegator();
    auto store_path = delegate.choosePath();

    PageId dmfile_id = context.storage_pool.newDataPageId();
    auto   dmfile    = writeIntoNewDMFile(context, schema_snap, input_stream, dmfile_id, store_path);
    auto   stable    = std::make_shared<StableValueSpace>(stable_id);
    stable->setFiles({dmfile});
    stable->saveMeta(wbs.meta);
    wbs.data.putExternal(dmfile_id, 0);
    delegate.addDTFile(dmfile_id, dmfile->getBytesOnDisk(), store_path);

    return stable;
}

//==========================================================================================
// Segment ser/deser
//==========================================================================================

Segment::Segment(UInt64                      epoch_, //
                 const HandleRange &         range_,
                 PageId                      segment_id_,
                 PageId                      next_segment_id_,
                 const DeltaValueSpacePtr &  delta_,
                 const StableValueSpacePtr & stable_)
    : epoch(epoch_),
      range(range_),
      segment_id(segment_id_),
      next_segment_id(next_segment_id_),
      delta(delta_),
      stable(stable_),
      log(&Logger::get("Segment"))
{
}

SegmentPtr Segment::newSegment(DMContext &              context,
                               const ColumnDefinesPtr & schema,
                               const HandleRange &      range,
                               PageId                   segment_id,
                               PageId                   next_segment_id,
                               PageId                   delta_id,
                               PageId                   stable_id)
{
    WriteBatches wbs(context.storage_pool);

    auto delta  = std::make_shared<DeltaValueSpace>(delta_id);
    auto stable = createNewStable(context, schema, std::make_shared<EmptySkippableBlockInputStream>(*schema), stable_id, wbs);

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
    DMContext & context, const ColumnDefinesPtr & schema, const HandleRange & range, PageId segment_id, PageId next_segment_id)
{
    return newSegment(
        context, schema, range, segment_id, next_segment_id, context.storage_pool.newMetaPageId(), context.storage_pool.newMetaPageId());
}

SegmentPtr Segment::restoreSegment(DMContext & context, PageId segment_id)
{
    Page page = context.storage_pool.meta().read(segment_id);

    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    Version              version;
    readIntBinary(version, buf);
    if (version != CURRENT_VERSION)
        throw Exception("version not match", ErrorCodes::LOGICAL_ERROR);
    UInt64      epoch;
    HandleRange range;
    PageId      next_segment_id, delta_id, stable_id;

    readIntBinary(epoch, buf);
    readIntBinary(range.start, buf);
    readIntBinary(range.end, buf);
    readIntBinary(next_segment_id, buf);
    readIntBinary(delta_id, buf);
    readIntBinary(stable_id, buf);

    auto delta = std::make_shared<DeltaValueSpace>(delta_id);
    delta->restore(context);
    auto stable  = StableValueSpace::restore(context, stable_id);
    auto segment = std::make_shared<Segment>(epoch, range, segment_id, next_segment_id, delta, stable);

    return segment;
}

void Segment::serialize(WriteBatch & wb)
{
    MemoryWriteBuffer buf(0, SEGMENT_BUFFER_SIZE);
    writeIntBinary(CURRENT_VERSION, buf);
    writeIntBinary(epoch, buf);
    writeIntBinary(range.start, buf);
    writeIntBinary(range.end, buf);
    writeIntBinary(next_segment_id, buf);
    writeIntBinary(delta->getId(), buf);
    writeIntBinary(stable->getId(), buf);

    auto data_size = buf.count(); // Must be called before tryGetReadBuffer.
    wb.putPage(segment_id, 0, buf.tryGetReadBuffer(), data_size);
}

bool Segment::writeToDisk(DMContext & dm_context, const PackPtr & pack)
{
    LOG_TRACE(log, "Segment [" << segment_id << "] write to disk rows: " << pack->rows);
    return delta->appendToDisk(dm_context, pack);
}

bool Segment::writeToCache(DMContext & dm_context, const Block & block, size_t offset, size_t limit)
{
    LOG_TRACE(log, "Segment [" << segment_id << "] write to cache rows: " << limit);
    if (unlikely(limit == 0))
        return true;
    return delta->appendToCache(dm_context, block, offset, limit);
}

bool Segment::write(DMContext & dm_context, const Block & block)
{
    LOG_TRACE(log, "Segment [" << segment_id << "] write to disk rows: " << block.rows());
    WriteBatches wbs(dm_context.storage_pool);
    PackPtr      pack = DeltaValueSpace::writePack(dm_context, block, 0, block.rows(), wbs);
    wbs.writeAll();

    if (delta->appendToDisk(dm_context, pack))
    {
        flushCache(dm_context);
        return true;
    }
    else
    {
        return false;
    }
}

bool Segment::write(DMContext & dm_context, const HandleRange & delete_range)
{
    auto new_range = delete_range.shrink(range);
    if (new_range.none())
    {
        LOG_WARNING(log, "Try to write an invalid delete range " << delete_range.toDebugString() << " into " << simpleInfo());
        return true;
    }

    LOG_TRACE(log, "Segment [" << segment_id << "] write delete range: " << delete_range.toDebugString());
    return delta->appendDeleteRange(dm_context, delete_range);
}

SegmentSnapshotPtr Segment::createSnapshot(const DMContext & dm_context, bool for_update) const
{
    // If the snapshot is created for read, then the snapshot will contain all packs (cached and persisted) for read.
    // If the snapshot is created for update, then the snapshot will only contain the persisted packs.
    auto delta_snap  = delta->createSnapshot(dm_context, for_update);
    auto stable_snap = stable->createSnapshot();
    if (!delta_snap || !stable_snap)
        return {};
    return std::make_shared<SegmentSnapshot>(std::move(delta_snap), std::move(stable_snap));
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &          dm_context,
                                            const ColumnDefines &      columns_to_read,
                                            const SegmentSnapshotPtr & segment_snap,
                                            const HandleRanges &       read_ranges,
                                            const RSOperatorPtr &      filter,
                                            UInt64                     max_version,
                                            size_t                     expected_block_size)
{
    LOG_TRACE(log, "Segment [" << segment_id << "] [epoch=" << epoch << "] create InputStream");

    auto read_info = getReadInfo(dm_context, columns_to_read, segment_snap, read_ranges, max_version);

    auto create_stream = [&](const HandleRange & read_range) -> BlockInputStreamPtr {
        BlockInputStreamPtr stream;
        if (dm_context.read_delta_only)
        {
            throw Exception("Unsupported");
        }
        else if (dm_context.read_stable_only)
        {
            stream = segment_snap->stable->getInputStream(
                dm_context, read_info.read_columns, read_range, filter, max_version, expected_block_size, false);
        }
        else if (segment_snap->delta->rows == 0 && segment_snap->delta->deletes == 0 //
                 && !hasColumn(columns_to_read, EXTRA_HANDLE_COLUMN_ID)              //
                 && !hasColumn(columns_to_read, VERSION_COLUMN_ID)                   //
                 && !hasColumn(columns_to_read, TAG_COLUMN_ID))
        {
            // No delta, let's try some optimizations.
            stream = segment_snap->stable->getInputStream(
                dm_context, read_info.read_columns, read_range, filter, max_version, expected_block_size, true);
        }
        else
        {
            stream = getPlacedStream(dm_context,
                                     read_info.read_columns,
                                     read_range,
                                     filter,
                                     segment_snap->stable,
                                     segment_snap->delta,
                                     read_info.index_begin,
                                     read_info.index_end,
                                     expected_block_size,
                                     max_version);
        }

        stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, read_range, 0);
        stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_MVCC>>(stream, columns_to_read, max_version);

        return stream;
    };

    BlockInputStreamPtr stream;
    if (read_ranges.size() == 1)
    {
        LOG_TRACE(log,
                  "Segment [" << DB::toString(segment_id) << "] is read by max_version: " << max_version << ", 1"
                              << " range: " << DB::DM::toDebugString(read_ranges));
        auto real_range = range.shrink(read_ranges[0]);
        if (real_range.none())
            stream = std::make_shared<EmptyBlockInputStream>(toEmptyBlock(read_info.read_columns));
        else
            stream = create_stream(real_range);
    }
    else
    {
        BlockInputStreams streams;
        for (auto & read_range : read_ranges)
        {
            HandleRange real_range = range.shrink(read_range);
            if (!real_range.none())
                streams.push_back(create_stream(real_range));
        }

        LOG_TRACE(log,
                  "Segment [" << DB::toString(segment_id) << "] is read by max_version: " << max_version << ", "
                              << DB::toString(streams.size()) << " ranges: " << DB::DM::toDebugString(read_ranges));

        if (streams.empty())
            stream = std::make_shared<EmptyBlockInputStream>(toEmptyBlock(read_info.read_columns));
        else
            stream = std::make_shared<ConcatBlockInputStream>(streams);
    }
    return stream;
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &     dm_context,
                                            const ColumnDefines & columns_to_read,
                                            const HandleRanges &  read_ranges,
                                            const RSOperatorPtr & filter,
                                            UInt64                max_version,
                                            size_t                expected_block_size)
{
    auto segment_snap = createSnapshot(dm_context);
    if (!segment_snap)
        return {};
    return getInputStream(dm_context, columns_to_read, segment_snap, read_ranges, filter, max_version, expected_block_size);
}

BlockInputStreamPtr Segment::getInputStreamRaw(const DMContext &          dm_context,
                                               const ColumnDefines &      columns_to_read,
                                               const SegmentSnapshotPtr & segment_snap,
                                               bool                       do_range_filter,
                                               size_t                     expected_block_size)
{
    ColumnDefines new_columns_to_read;

    if (!do_range_filter)
    {
        new_columns_to_read = columns_to_read;
    }
    else
    {
        new_columns_to_read.push_back(getExtraHandleColumnDefine());

        for (const auto & c : columns_to_read)
        {
            if (c.id != EXTRA_HANDLE_COLUMN_ID)
                new_columns_to_read.push_back(c);
        }
    }

    BlockInputStreamPtr delta_stream = segment_snap->delta->prepareForStream(dm_context, new_columns_to_read);

    BlockInputStreamPtr stable_stream = segment_snap->stable->getInputStream(
        dm_context, new_columns_to_read, range, EMPTY_FILTER, MAX_UINT64, expected_block_size, false);

    if (do_range_filter)
    {
        delta_stream = std::make_shared<DMHandleFilterBlockInputStream<false>>(delta_stream, range, 0);
        delta_stream = std::make_shared<DMColumnFilterBlockInputStream>(delta_stream, columns_to_read);

        stable_stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(stable_stream, range, 0);
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
    auto segment_snap = createSnapshot(dm_context);
    if (!segment_snap)
        return {};
    return getInputStreamRaw(dm_context, columns_to_read, segment_snap, true);
}

SegmentPtr Segment::mergeDelta(DMContext & dm_context, const ColumnDefinesPtr & schema_snap) const
{
    WriteBatches wbs(dm_context.storage_pool);
    auto         segment_snap = createSnapshot(dm_context, true);
    if (!segment_snap)
        return {};

    auto new_stable = prepareMergeDelta(dm_context, schema_snap, segment_snap, wbs);

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
                                               WriteBatches &             wbs) const
{
    LOG_INFO(log,
             "Segment [" << DB::toString(segment_id)
                         << "] prepare merge delta start. delta packs: " << DB::toString(segment_snap->delta->getPackCount())
                         << ", delta total rows: " << DB::toString(segment_snap->delta->getRows()));

    EventRecorder recorder(ProfileEvents::DMDeltaMerge, ProfileEvents::DMDeltaMergeNS);

    auto read_info = getReadInfo(dm_context, *schema_snap, segment_snap);

    BlockInputStreamPtr data_stream = getPlacedStream(dm_context,
                                                      read_info.read_columns,
                                                      range,
                                                      EMPTY_FILTER,
                                                      segment_snap->stable,
                                                      segment_snap->delta,
                                                      read_info.index_begin,
                                                      read_info.index_end,
                                                      dm_context.stable_pack_rows);

    data_stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(data_stream, range, 0);
    data_stream = std::make_shared<ReorganizeBlockInputStream>(data_stream, EXTRA_HANDLE_COLUMN_NAME);
    data_stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
        data_stream, read_info.read_columns, dm_context.min_version);

    auto new_stable = createNewStable(dm_context, schema_snap, data_stream, segment_snap->stable->getId(), wbs);

    LOG_INFO(log, "Segment [" << DB::toString(segment_id) << "] prepare merge delta done.");

    return new_stable;
}

SegmentPtr Segment::applyMergeDelta(DMContext &                 context,
                                    const SegmentSnapshotPtr &  segment_snap,
                                    WriteBatches &              wbs,
                                    const StableValueSpacePtr & new_stable) const
{
    LOG_INFO(log, "Before apply merge delta: " << info());

    auto later_packs = delta->checkHeadAndCloneTail(context, range, segment_snap->delta->packs, wbs);
    // Created references to tail pages' pages in "log" storage, we need to write them down.
    wbs.writeLogAndData();

    auto new_delta = std::make_shared<DeltaValueSpace>(delta->getId(), later_packs);
    new_delta->saveMeta(wbs);

    auto new_me = std::make_shared<Segment>(epoch + 1, //
                                            range,
                                            segment_id,
                                            next_segment_id,
                                            new_delta,
                                            new_stable);

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
    auto         segment_snap = createSnapshot(dm_context, true);
    if (!segment_snap)
        return {};

    auto split_info_opt = prepareSplit(dm_context, schema_snap, segment_snap, wbs);
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

std::optional<Handle> Segment::getSplitPointFast(DMContext & dm_context, const StableSnapshotPtr & stable_snap) const
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
                                  read_file,
                                  {getExtraHandleColumnDefine()},
                                  HandleRange::newAll(),
                                  EMPTY_FILTER,
                                  stable_snap->getColumnCaches()[file_index],
                                  read_pack);

    stream.readPrefix();
    auto block = stream.read();
    if (!block)
        throw Exception("Unexpected empty block");
    stream.readSuffix();

<<<<<<< HEAD
    return {block.getByPosition(0).column->getInt(read_row_in_pack)};
=======
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
>>>>>>> 7a63da895... Abort the current split and forbid later split under illegal split point, instead of exception (#2214)
}

std::optional<Handle>
Segment::getSplitPointSlow(DMContext & dm_context, const ReadInfo & read_info, const SegmentSnapshotPtr & segment_snap) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);

    auto & handle     = getExtraHandleColumnDefine();
    size_t exact_rows = 0;

    {
        BlockInputStreamPtr stream = getPlacedStream(dm_context,
                                                     {handle},
                                                     range,
                                                     EMPTY_FILTER,
                                                     segment_snap->stable,
                                                     segment_snap->delta,
                                                     read_info.index_begin,
                                                     read_info.index_end,
                                                     dm_context.stable_pack_rows);

        stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, range, 0);

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
                                                 {handle},
                                                 range,
                                                 EMPTY_FILTER,
                                                 segment_snap->stable,
                                                 segment_snap->delta,
                                                 read_info.index_begin,
                                                 read_info.index_end,
                                                 dm_context.stable_pack_rows);

    stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, range, 0);

    size_t split_row_index = exact_rows / 2;
    Handle split_handle    = 0;
    size_t count           = 0;

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
            split_handle           = block.getByName(handle.name).column->getInt(offset_in_block);
            break;
        }
    }
    stream->readSuffix();

<<<<<<< HEAD
    if (!range.check(split_handle))
        throw Exception("getSplitPointSlow unexpected split_handle: " + Redact::handleToDebugString(split_handle) + ", should be in range "
                        + range.toDebugString() + ", exact_rows: " + DB::toString(exact_rows) + ", cur count:" + DB::toString(count));
=======
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
>>>>>>> 7a63da895... Abort the current split and forbid later split under illegal split point, instead of exception (#2214)

    return {split_handle};
}

std::optional<Segment::SplitInfo> Segment::prepareSplit(DMContext &                dm_context,
                                                        const ColumnDefinesPtr &   schema_snap,
                                                        const SegmentSnapshotPtr & segment_snap,
                                                        WriteBatches &             wbs) const
{
    if (!dm_context.enable_logical_split         //
        || segment_snap->stable->getPacks() <= 3 //
        || segment_snap->delta->getRows() > segment_snap->stable->getRows())
    {
<<<<<<< HEAD
        return prepareSplitPhysical(dm_context, schema_snap, segment_snap, wbs);
=======
        return prepareSplitPhysical(dm_context, schema_snap, segment_snap, wbs, need_rate_limit);
>>>>>>> 7a63da895... Abort the current split and forbid later split under illegal split point, instead of exception (#2214)
    }
    else
    {
        auto split_point_opt = getSplitPointFast(dm_context, segment_snap->stable);
        bool bad_split_point
            = !split_point_opt.has_value() || !range.check(split_point_opt.value()) || split_point_opt.value() == range.start;
        if (bad_split_point)
        {
            LOG_INFO(
                log,
                "Got bad split point [" << (split_point_opt.has_value() ? Redact::handleToDebugString(split_point_opt.value()) : "no value")
                                        << "] for segment " << info() << ", fall back to split physical.");
            return prepareSplitPhysical(dm_context, schema_snap, segment_snap, wbs);
        }
        else
            return prepareSplitLogical(dm_context, schema_snap, segment_snap, split_point_opt.value(), wbs);
    }
}

std::optional<Segment::SplitInfo> Segment::prepareSplitLogical(DMContext & dm_context,
                                                               const ColumnDefinesPtr & /*schema_snap*/,
                                                               const SegmentSnapshotPtr & segment_snap,
                                                               Handle                     split_point,
                                                               WriteBatches &             wbs) const
{
    LOG_INFO(log, "Segment [" << segment_id << "] prepare split logical start");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & storage_pool = dm_context.storage_pool;

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

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
        auto file_id          = segment_snap->delta->storage_snap->data_reader.getNormalPageId(ori_ref_id);
        auto file_parent_path = delegate.getDTFilePath(file_id);

        auto my_dmfile_id    = storage_pool.newDataPageId();
        auto other_dmfile_id = storage_pool.newDataPageId();

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

    my_stable->setFiles(my_stable_files, &dm_context, my_range);
    other_stable->setFiles(other_stable_files, &dm_context, other_range);

    LOG_INFO(log, "Segment [" << segment_id << "] prepare split logical done");

    return {SplitInfo{true, split_point, my_stable, other_stable}};
}

std::optional<Segment::SplitInfo> Segment::prepareSplitPhysical(DMContext &                dm_context,
                                                                const ColumnDefinesPtr &   schema_snap,
                                                                const SegmentSnapshotPtr & segment_snap,
                                                                WriteBatches &             wbs) const
{
    LOG_INFO(log, "Segment [" << segment_id << "] prepare split physical start");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto read_info       = getReadInfo(dm_context, *schema_snap, segment_snap);
    auto split_point_opt = getSplitPointSlow(dm_context, read_info, segment_snap);
    if (!split_point_opt.has_value())
        return {};

    auto split_point = split_point_opt.value();

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

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
        BlockInputStreamPtr my_data = getPlacedStream(dm_context,
                                                      read_info.read_columns,
                                                      my_range,
                                                      EMPTY_FILTER,
                                                      segment_snap->stable,
                                                      segment_snap->delta,
                                                      read_info.index_begin,
                                                      read_info.index_end,
                                                      dm_context.stable_pack_rows);

        LOG_DEBUG(log, "Created my placed stream");

        my_data = std::make_shared<DMHandleFilterBlockInputStream<true>>(my_data, my_range, 0);
        my_data = std::make_shared<ReorganizeBlockInputStream>(my_data, EXTRA_HANDLE_COLUMN_NAME);
        my_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            my_data, read_info.read_columns, dm_context.min_version);
        auto my_stable_id = segment_snap->stable->getId();
        my_new_stable     = createNewStable(dm_context, schema_snap, my_data, my_stable_id, wbs);
    }

    LOG_INFO(log, "prepare my_new_stable done");

    {
        // Write new segment's data
        BlockInputStreamPtr other_data = getPlacedStream(dm_context,
                                                         read_info.read_columns,
                                                         other_range,
                                                         EMPTY_FILTER,
                                                         segment_snap->stable,
                                                         segment_snap->delta,
                                                         read_info.index_begin,
                                                         read_info.index_end,
                                                         dm_context.stable_pack_rows);

        LOG_DEBUG(log, "Created other placed stream");

        other_data = std::make_shared<DMHandleFilterBlockInputStream<true>>(other_data, other_range, 0);
        other_data = std::make_shared<ReorganizeBlockInputStream>(other_data, EXTRA_HANDLE_COLUMN_NAME);
        other_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            other_data, read_info.read_columns, dm_context.min_version);
        auto other_stable_id = dm_context.storage_pool.newMetaPageId();
        other_stable         = createNewStable(dm_context, schema_snap, other_data, other_stable_id, wbs);
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

    HandleRange my_range    = {range.start, split_info.split_point};
    HandleRange other_range = {split_info.split_point, range.end};

    Packs   empty_packs;
    Packs * head_packs = split_info.is_logical ? &empty_packs : &segment_snap->delta->packs;

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

    auto left_snap  = left->createSnapshot(dm_context, true);
    auto right_snap = right->createSnapshot(dm_context, true);
    if (!left_snap || !right_snap)
        return {};

    auto merged_stable = prepareMerge(dm_context, schema_snap, left, left_snap, right, right_snap, wbs);

    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC();

    auto left_lock  = left->mustGetUpdateLock();
    auto right_lock = right->mustGetUpdateLock();

    auto merged = applyMerge(dm_context, left, left_snap, right, right_snap, wbs, merged_stable);

    wbs.writeAll();
    return merged;
}

StableValueSpacePtr Segment::prepareMerge(DMContext &                dm_context, //
                                          const ColumnDefinesPtr &   schema_snap,
                                          const SegmentPtr &         left,
                                          const SegmentSnapshotPtr & left_snap,
                                          const SegmentPtr &         right,
                                          const SegmentSnapshotPtr & right_snap,
                                          WriteBatches &             wbs)
{
    LOG_INFO(left->log, "Segment [" << left->segmentId() << "] and [" << right->segmentId() << "] prepare merge start");

    if (unlikely(left->range.end != right->range.start || left->next_segment_id != right->segment_id))
        throw Exception("The ranges of merge segments are not consecutive: first end: " + Redact::handleToDebugString(left->range.end)
                        + ", second start: " + Redact::handleToDebugString(right->range.start));

    auto getStream = [&](const SegmentPtr & segment, const SegmentSnapshotPtr & segment_snap) {
        auto                read_info = segment->getReadInfo(dm_context, *schema_snap, segment_snap);
        BlockInputStreamPtr stream    = getPlacedStream(dm_context,
                                                     read_info.read_columns,
                                                     segment->range,
                                                     EMPTY_FILTER,
                                                     segment_snap->stable,
                                                     segment_snap->delta,
                                                     read_info.index_begin,
                                                     read_info.index_end,
                                                     dm_context.stable_pack_rows);

        stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, segment->range, 0);
        stream = std::make_shared<ReorganizeBlockInputStream>(stream, EXTRA_HANDLE_COLUMN_NAME);
        stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            stream, read_info.read_columns, dm_context.min_version);

        return stream;
    };

    auto left_stream  = getStream(left, left_snap);
    auto right_stream = getStream(right, right_snap);

    auto merged_stream = std::make_shared<ConcatBlockInputStream>(BlockInputStreams{left_stream, right_stream});

    auto merged_stable_id = left->stable->getId();
    auto merged_stable    = createNewStable(dm_context, schema_snap, merged_stream, merged_stable_id, wbs);

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

    HandleRange merged_range = {left->range.start, right->range.end};

    auto left_tail_packs  = left->delta->checkHeadAndCloneTail(dm_context, merged_range, left_snap->delta->packs, wbs);
    auto right_tail_packs = right->delta->checkHeadAndCloneTail(dm_context, merged_range, right_snap->delta->packs, wbs);

    // Created references to tail pages' pages in "log" storage, we need to write them down.
    wbs.writeLogAndData();

    /// Make sure saved packs are appended before unsaved packs.
    Packs merged_packs;

    auto L_first_unsaved = std::find_if(left_tail_packs.begin(), left_tail_packs.end(), [](const PackPtr & p) { return !p->isSaved(); });
    auto R_first_unsaved = std::find_if(right_tail_packs.begin(), right_tail_packs.end(), [](const PackPtr & p) { return !p->isSaved(); });

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
    auto segment_snap = createSnapshot(dm_context, /*for_update=*/true);
    if (!segment_snap)
        return;
    getReadInfo(dm_context, /*read_columns=*/{getExtraHandleColumnDefine()}, segment_snap);
}

String Segment::simpleInfo() const
{
    return "{" + DB::toString(segment_id) + ":" + range.toDebugString() + "}";
}

String Segment::info() const
{
    std::stringstream s;
    s << "{[id:" << segment_id << "], [next:" << next_segment_id << "], [epoch:" << epoch << "], [range:" << range.toDebugString()
      << "], [delta rows:" << delta->getRows() << "], [delete ranges:" << delta->getDeletes() << "], [stable(" << stable->getDMFilesString()
      << "):" << stable->getRows() << "]}";
    return s.str();
}

Segment::ReadInfo Segment::getReadInfo(const DMContext &          dm_context,
                                       const ColumnDefines &      read_columns,
                                       const SegmentSnapshotPtr & segment_snap,
                                       const HandleRanges &       read_ranges,
                                       UInt64                     max_version) const
{
    LOG_DEBUG(log, __FUNCTION__ << " start");

    auto new_read_columns = arrangeReadColumns(getExtraHandleColumnDefine(), read_columns);
    segment_snap->delta->prepare(dm_context, new_read_columns);

    auto [my_delta_index, fully_indexed] = ensurePlace(dm_context, segment_snap->stable, segment_snap->delta, read_ranges, max_version);
    auto compacted_index                 = my_delta_index->getDeltaTree()->getCompactedEntries();
    // Hold compacted_index reference in snapshot, to prevent it from deallocated.
    segment_snap->delta->compacted_delta_index = compacted_index;

    LOG_DEBUG(log, __FUNCTION__ << " end");

    if (fully_indexed)
    {
        // Try update shared index, if my_delta_index is more advanced.
        bool ok = segment_snap->delta->shared_delta_index->updateIfAdvanced(*my_delta_index);
        if (ok)
            LOG_DEBUG(log, simpleInfo() << " Updated delta index");
    }

    return {
        .index_begin  = compacted_index->begin(),
        .index_end    = compacted_index->end(),
        .read_columns = new_read_columns,
    };
}

ColumnDefines Segment::arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read)
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

    return new_columns_to_read;
}

template <bool skippable_place, class IndexIterator>
SkippableBlockInputStreamPtr Segment::getPlacedStream(const DMContext &         dm_context,
                                                      const ColumnDefines &     read_columns,
                                                      const HandleRange &       handle_range,
                                                      const RSOperatorPtr &     filter,
                                                      const StableSnapshotPtr & stable_snap,
                                                      DeltaSnapshotPtr &        delta_snap,
                                                      const IndexIterator &     delta_index_begin,
                                                      const IndexIterator &     delta_index_end,
                                                      size_t                    expected_block_size,
                                                      UInt64                    max_version)
{
    SkippableBlockInputStreamPtr stable_input_stream
        = stable_snap->getInputStream(dm_context, read_columns, handle_range, filter, max_version, expected_block_size, false);
    return std::make_shared<DeltaMergeBlockInputStream<DeltaSnapshot, IndexIterator, skippable_place>>( //
        stable_input_stream,
        delta_snap,
        delta_index_begin,
        delta_index_end,
        handle_range,
        expected_block_size);
}

std::pair<DeltaIndexPtr, bool> Segment::ensurePlace(const DMContext &         dm_context,
                                                    const StableSnapshotPtr & stable_snap,
                                                    DeltaSnapshotPtr &        delta_snap,
                                                    const HandleRanges &      read_ranges,
                                                    UInt64                    max_version) const
{
    // Clone a new delta index.
    auto my_delta_index = delta_snap->shared_delta_index->tryClone(delta_snap->rows, delta_snap->deletes);
    auto my_delta_tree  = my_delta_index->getDeltaTree();

    HandleRange relevant_range = dm_context.enable_relevant_place ? mergeRanges(read_ranges) : HandleRange::newAll();

    auto [my_placed_rows, my_placed_deletes] = my_delta_index->getPlacedStatus();

    // Let's do a fast check, determine whether we need to do place or not.
    if (!delta_snap->shouldPlace(dm_context, my_delta_index, range, relevant_range, max_version))
        return {my_delta_index, false};

    CurrentMetrics::Increment cur_dm_segments{CurrentMetrics::DT_PlaceIndexUpdate};
    GET_METRIC(dm_context.metrics, tiflash_storage_subtask_count, type_place_index_update).Increment();
    Stopwatch watch;
    SCOPE_EXIT({
        GET_METRIC(dm_context.metrics, tiflash_storage_subtask_duration_seconds, type_place_index_update).Observe(watch.elapsedSeconds());
    });

    EventRecorder recorder(ProfileEvents::DMPlace, ProfileEvents::DMPlaceNS);

    auto blocks = delta_snap->getMergeBlocks(my_placed_rows, my_placed_deletes, delta_snap->rows, delta_snap->deletes);

    bool fully_indexed = true;
    for (auto & v : blocks)
    {
        if (!v.delete_range.none())
        {
            if (dm_context.enable_skippable_place)
                fully_indexed &= placeDelete<true>(dm_context, stable_snap, delta_snap, v.delete_range, *my_delta_tree, relevant_range);
            else
                fully_indexed &= placeDelete<false>(dm_context, stable_snap, delta_snap, v.delete_range, *my_delta_tree, relevant_range);

            ++my_placed_deletes;
        }
        else if (v.block)
        {
            auto rows = v.block.rows();
            if (dm_context.enable_skippable_place)
                fully_indexed &= placeUpsert<true>(
                    dm_context, stable_snap, delta_snap, my_placed_rows, std::move(v.block), *my_delta_tree, relevant_range);
            else
                fully_indexed &= placeUpsert<false>(
                    dm_context, stable_snap, delta_snap, my_placed_rows, std::move(v.block), *my_delta_tree, relevant_range);

            my_placed_rows += rows;
        }
    }

    if (unlikely(my_placed_rows != delta_snap->rows || my_placed_deletes != delta_snap->deletes))
        throw Exception("Placed status not match! Expected place rows:" + DB::toString(delta_snap->rows)
                        + ", deletes:" + DB::toString(delta_snap->deletes) + ", but actually placed rows:" + DB::toString(my_placed_rows)
                        + ", deletes:" + DB::toString(my_placed_deletes));

    my_delta_index->update(my_delta_tree, my_placed_rows, my_placed_deletes);

    LOG_DEBUG(log,
              __FUNCTION__ << simpleInfo() << " read_ranges:" << DB::DM::toDebugString(read_ranges) << ", blocks.size:" << blocks.size()
                           << ", shared delta index: " << delta_snap->shared_delta_index->toString()
                           << ", my delta index: " << my_delta_index->toString());

    return {my_delta_index, fully_indexed};
}

template <bool skippable_place>
bool Segment::placeUpsert(const DMContext &         dm_context,
                          const StableSnapshotPtr & stable_snap,
                          DeltaSnapshotPtr &        delta_snap,
                          size_t                    delta_value_space_offset,
                          Block &&                  block,
                          DeltaTree &               update_delta_tree,
                          const HandleRange &       relevant_range) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceUpsert, ProfileEvents::DMPlaceUpsertNS);

    IColumn::Permutation perm;

    auto & handle       = getExtraHandleColumnDefine();
    bool   do_sort      = sortBlockByPk(handle, block, perm);
    Handle first_handle = block.getByPosition(0).column->getInt(0);

    auto place_handle_range
        = skippable_place ? HandleRange(std::max(first_handle, relevant_range.start), HandleRange::MAX) : HandleRange::newAll();

    auto compacted_index = update_delta_tree.getCompactedEntries();

    auto merged_stream = getPlacedStream<skippable_place>( //
        dm_context,
        {handle, getVersionColumnDefine()},
        place_handle_range,
        EMPTY_FILTER,
        stable_snap,
        delta_snap,
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
bool Segment::placeDelete(const DMContext &         dm_context,
                          const StableSnapshotPtr & stable_snap,
                          DeltaSnapshotPtr &        delta_snap,
                          const HandleRange &       delete_range,
                          DeltaTree &               update_delta_tree,
                          const HandleRange &       relevant_range) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceDeleteRange, ProfileEvents::DMPlaceDeleteRangeNS);

    auto & handle = getExtraHandleColumnDefine();

    Blocks delete_data;
    {
        auto compacted_index = update_delta_tree.getCompactedEntries();

        BlockInputStreamPtr delete_stream = getPlacedStream( //
            dm_context,
            {handle, getVersionColumnDefine()},
            delete_range,
            EMPTY_FILTER,
            stable_snap,
            delta_snap,
            compacted_index->begin(),
            compacted_index->end(),
            dm_context.stable_pack_rows);

        delete_stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(delete_stream, delete_range, 0);

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
        Handle first_handle       = block.getByPosition(0).column->getInt(0);
        auto   place_handle_range = skippable_place ? HandleRange(first_handle, HandleRange::MAX) : HandleRange::newAll();

        auto compacted_index = update_delta_tree.getCompactedEntries();

        auto merged_stream = getPlacedStream<skippable_place>( //
            dm_context,
            {handle, getVersionColumnDefine()},
            place_handle_range,
            EMPTY_FILTER,
            stable_snap,
            delta_snap,
            compacted_index->begin(),
            compacted_index->end(),
            dm_context.stable_pack_rows);
        fully_indexed &= DM::placeDelete(merged_stream, block, relevant_range, update_delta_tree, getPkSort(handle));
    }
    return fully_indexed;
}

} // namespace DM
} // namespace DB
