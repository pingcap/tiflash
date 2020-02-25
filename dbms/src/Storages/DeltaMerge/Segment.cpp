#include <numeric>

#include <DataTypes/DataTypeFactory.h>

#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>

#include <Storages/DeltaMerge/DMDecoratorStreams.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMerge.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaPlace.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/FilterHelper.h>
#include <Storages/DeltaMerge/ReorganizeBlockInputStream.h>
#include <Storages/DeltaMerge/Segment.h>

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
                             const BlockInputStreamPtr & input_stream,
                             UInt64                      file_id,
                             const String &              parent_path)
{
    auto   dmfile        = DMFile::create(file_id, parent_path);
    auto   output_stream = std::make_shared<DMFileBlockOutputStream>(dm_context.db_context, dmfile, dm_context.store_columns);
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

StableValueSpacePtr createNewStable(DMContext & context, const BlockInputStreamPtr & input_stream, PageId stable_id, WriteBatches & wbs)
{
    auto [store_path_id, store_path] = context.extra_paths.choosePath();

    PageId dmfile_id = context.storage_pool.newDataPageId();
    auto   dmfile    = writeIntoNewDMFile(context, input_stream, dmfile_id, store_path + "/" + STABLE_FOLDER_NAME);
    auto   stable    = std::make_shared<StableValueSpace>(stable_id);
    stable->setFiles({dmfile});
    stable->saveMeta(wbs.meta);
    wbs.data.putExternal(dmfile_id, store_path_id);

    return stable;
}

//==========================================================================================
// Segment ser/deser
//==========================================================================================

Segment::Segment(UInt64                      epoch_, //
                 const HandleRange &         range_,
                 PageId                      segment_id_,
                 PageId                      next_segment_id_,
                 const DiskValueSpacePtr &   delta_,
                 const StableValueSpacePtr & stable_)
    : epoch(epoch_),
      range(range_),
      segment_id(segment_id_),
      next_segment_id(next_segment_id_),
      delta(delta_),
      stable(stable_),
      delta_tree(std::make_shared<DefaultDeltaTree>()),
      log(&Logger::get("Segment"))
{
}

SegmentPtr Segment::newSegment(
    DMContext & context, const HandleRange & range, PageId segment_id, PageId next_segment_id, PageId delta_id, PageId stable_id)
{
    WriteBatches wbs;

    auto delta = std::make_shared<DiskValueSpace>(true, delta_id);
    delta->replacePacks(wbs.meta, wbs.log, {}); // Write done the meta data of delta.

    auto stable = createNewStable(context, std::make_shared<EmptySkippableBlockInputStream>(context.store_columns), stable_id, wbs);

    auto segment = std::make_shared<Segment>(INITIAL_EPOCH, range, segment_id, next_segment_id, delta, stable);

    // Write metadata.
    segment->serialize(wbs.meta);

    wbs.writeAll(context.storage_pool);
    stable->enableDMFilesGC();

    return segment;
}

SegmentPtr Segment::newSegment(DMContext & context, const HandleRange & range, PageId segment_id, PageId next_segment_id)
{
    return newSegment(
        context, range, segment_id, next_segment_id, context.storage_pool.newMetaPageId(), context.storage_pool.newMetaPageId());
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

    auto delta = std::make_shared<DiskValueSpace>(true, delta_id);
    delta->restore(OpContext::createForLogStorage(context));
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
    writeIntBinary(delta->pageId(), buf);
    writeIntBinary(stable->getId(), buf);

    auto data_size = buf.count(); // Must be called before tryGetReadBuffer.
    wb.putPage(segment_id, 0, buf.tryGetReadBuffer(), data_size);
}

//==========================================================================================
// Segment public APIs.
//==========================================================================================

void Segment::write(DMContext & dm_context, const BlockOrDelete & update)
{
    auto         op_context   = OpContext::createForLogStorage(dm_context);
    auto &       storage_pool = dm_context.storage_pool;
    WriteBatches wbs;

    auto task = createAppendTask(op_context, wbs, update);

    wbs.writeLogAndData(storage_pool);

    applyAppendToWriteBatches(task, wbs);
    wbs.writeMeta(storage_pool);

    applyAppendTask(op_context, task, update);
    wbs.writeRemoves(storage_pool);
}

AppendTaskPtr Segment::createAppendTask(const OpContext & opc, WriteBatches & wbs, const BlockOrDelete & update)
{
    if (update.block)
        LOG_TRACE(log,
                  "Segment [" << DB::toString(segment_id) << "] create append task, write rows: " << DB::toString(update.block.rows()));
    else
        LOG_TRACE(log, "Segment [" << DB::toString(segment_id) << "] create append task, delete range: " << update.delete_range.toString());

    EventRecorder recorder(ProfileEvents::DMAppendDeltaPrepare, ProfileEvents::DMAppendDeltaPrepareNS);

    // Create everything we need to do the update.
    // We only need a shared lock because this operation won't do any modifications.
    std::shared_lock lock(read_write_mutex);
    return delta->createAppendTask(opc, wbs, update);
}

void Segment::applyAppendToWriteBatches(const AppendTaskPtr & task, WriteBatches & wbs)
{
    std::shared_lock lock(read_write_mutex);
    delta->applyAppendToWriteBatches(task, wbs);
}

void Segment::applyAppendTask(const OpContext & opc, const AppendTaskPtr & task, const BlockOrDelete & update)
{
    // Unique lock, to protect memory modifications against read threads.
    std::unique_lock segment_lock(read_write_mutex);

    EventRecorder recorder(ProfileEvents::DMAppendDeltaCommitMemory, ProfileEvents::DMAppendDeltaCommitMemoryNS);

    auto new_delta = delta->applyAppendTask(opc, task, update);

    if (update.block)
        LOG_TRACE(log, "Segment [" << DB::toString(segment_id) << "] apply append task, write rows: " << DB::toString(update.block.rows()));
    else
        LOG_TRACE(log, "Segment [" << DB::toString(segment_id) << "] apply append task, delete range: " << update.delete_range.toString());

    if (new_delta)
    {
        LOG_TRACE(log, "Segment [" << DB::toString(segment_id) << "] update delta instance");
        delta = new_delta;
    }
}

SegmentSnapshot Segment::getReadSnapshot(bool use_delta_cache) const
{
    SegmentSnapshot segment_snap;
    {
        // Synchronize between read/write threads.
        std::shared_lock lock(read_write_mutex);

        size_t delta_rows = delta->num_rows();
        if (!use_delta_cache)
            delta_rows -= delta->cacheRows();

        segment_snap = SegmentSnapshot{
            .stable          = stable, // Stable is constant.
            .delta           = std::make_shared<DiskValueSpace>(*delta),
            .delta_rows      = delta_rows,
            .delta_deletes   = delta->num_deletes(),
            .delta_index     = {},
            .delta_has_cache = use_delta_cache,
        };
    }

    {
        std::scoped_lock lock(read_read_mutex);

        // If current delta tree matches delta status, copy it now.
        if (segment_snap.delta_rows == placed_delta_rows && segment_snap.delta_deletes == placed_delta_deletes)
            segment_snap.delta_index = delta_tree->getEntriesCopy<Allocator<false>>();
    }
    return segment_snap;
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &       dm_context,
                                            const ColumnDefines &   columns_to_read,
                                            const SegmentSnapshot & segment_snap,
                                            const StorageSnapshot & storage_snap,
                                            const HandleRanges &    read_ranges,
                                            const RSOperatorPtr &   filter,
                                            UInt64                  max_version,
                                            size_t                  expected_block_size)
{
    auto read_info = getReadInfo<true>(dm_context, columns_to_read, segment_snap, storage_snap, mergeRanges(read_ranges));

    auto create_stream = [&](const HandleRange & read_range) -> BlockInputStreamPtr {
        BlockInputStreamPtr stream;
        if (dm_context.read_delta_only)
        {
            throw Exception("Unsupported");
        }
        else if (dm_context.read_stable_only)
        {
            stream
                = read_info.segment_snap.stable->getInputStream(dm_context, read_info.read_columns, read_range, filter, max_version, false);
        }
        else if (segment_snap.delta_rows == 0 && segment_snap.delta_deletes == 0 //
                 && !hasColumn(columns_to_read, EXTRA_HANDLE_COLUMN_ID)          //
                 && !hasColumn(columns_to_read, VERSION_COLUMN_ID)               //
                 && !hasColumn(columns_to_read, TAG_COLUMN_ID))
        {
            // No delta, let's try some optimizations.
            stream
                = read_info.segment_snap.stable->getInputStream(dm_context, read_info.read_columns, read_range, filter, max_version, true);
        }
        else
        {
            stream = getPlacedStream(dm_context,
                                     read_info.read_columns,
                                     read_range,
                                     filter,
                                     read_info.segment_snap.stable,
                                     read_info.delta_value_space,
                                     read_info.index_begin,
                                     read_info.index_end,
                                     read_info.index->entryCount(),
                                     expected_block_size);
        }

        stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, read_range, 0);
        stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_MVCC>>(stream, columns_to_read, max_version);

        return stream;
    };

    if (read_ranges.size() == 1)
    {
        LOG_TRACE(log,
                  "Segment [" << DB::toString(segment_id) << "] is read by max_version: " << max_version << ", 1"
                              << " range: " << toString(read_ranges));
        return create_stream(range.shrink(read_ranges[0]));
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
                              << DB::toString(streams.size()) << " ranges: " << toString(read_ranges));

        return std::make_shared<ConcatBlockInputStream>(streams);
    }
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &     dm_context,
                                            const ColumnDefines & columns_to_read,
                                            const HandleRanges &  read_ranges,
                                            const RSOperatorPtr & filter,
                                            UInt64                max_version,
                                            size_t                expected_block_size)
{
    return getInputStream(
        dm_context, columns_to_read, getReadSnapshot(), {dm_context.storage_pool}, read_ranges, filter, max_version, expected_block_size);
}

BlockInputStreamPtr Segment::getInputStreamRaw(const DMContext &       dm_context,
                                               const ColumnDefines &   columns_to_read,
                                               const SegmentSnapshot & segment_snap,
                                               const StorageSnapshot & storage_snap,
                                               bool                    do_range_filter)
{
    ColumnDefines new_columns_to_read;

    if (!do_range_filter)
    {
        new_columns_to_read = columns_to_read;
    }
    else
    {
        auto & handle = dm_context.handle_column;
        new_columns_to_read.push_back(handle);

        for (const auto & c : columns_to_read)
        {
            if (c.id != handle.id)
                new_columns_to_read.push_back(c);
        }
    }

    auto                delta_packs = segment_snap.delta->getPacksBefore(segment_snap.delta_rows, segment_snap.delta_deletes);
    BlockInputStreamPtr delta_stream = std::make_shared<PackBlockInputStream>(delta_packs, //
                                                                               new_columns_to_read,
                                                                               storage_snap.log_reader,
                                                                               EMPTY_FILTER);

    BlockInputStreamPtr stable_stream
        = segment_snap.stable->getInputStream(dm_context, new_columns_to_read, range, EMPTY_FILTER, MAX_UINT64, false);

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
    return getInputStreamRaw(dm_context, columns_to_read, getReadSnapshot(), {dm_context.storage_pool}, true);
}

SegmentPair
Segment::split(DMContext & dm_context, const SegmentSnapshot & segment_snap, const StorageSnapshot & storage_snap, WriteBatches & wbs) const
{
    LOG_DEBUG(log, "Segment " << info() << " start to split");

    SegmentPair res;

    if (!dm_context.enable_logical_split         //
        || segment_snap.stable->getPacks() <= 3 //
        || segment_snap.delta->num_rows() > segment_snap.stable->getRows())
        res = doSplitPhysical(dm_context, segment_snap, storage_snap, wbs);
    else
    {
        Handle split_point     = getSplitPointFast(dm_context, segment_snap.stable);
        bool   bad_split_point = !range.check(split_point) || split_point == range.start;
        if (bad_split_point)
            res = doSplitPhysical(dm_context, segment_snap, storage_snap, wbs);
        else
            res = doSplitLogical(dm_context, segment_snap, storage_snap, split_point, wbs);
    }

    LOG_DEBUG(log, "Segment " << info() << " split into " << res.first->info() << " and " << res.second->info());

    return res;
}

SegmentPair Segment::split(DMContext & dm_context) const
{
    WriteBatches wbs;
    auto &       storage_pool = dm_context.storage_pool;
    auto         res          = split(dm_context, getReadSnapshot(), {storage_pool}, wbs);
    wbs.writeAll(storage_pool);
    return res;
}

//SegmentPtr Segment::merge(DMContext &             dm_context,
//                          const SegmentPtr &      left,
//                          const SegmentSnapshot & left_snap,
//                          const SegmentPtr &      right,
//                          const SegmentSnapshot & right_snap,
//                          const StorageSnapshot & storage_snap,
//                          WriteBatches &          wbs)
SegmentPtr Segment::merge(DMContext &,
                          const SegmentPtr &,
                          const SegmentSnapshot &,
                          const SegmentPtr &,
                          const SegmentSnapshot &,
                          const StorageSnapshot &,
                          WriteBatches &)
{
    throw Exception("Unimplemented");
    //    LOG_DEBUG(left->log, "Merge segment [" << DB::toString(left->segment_id) << "] with [" << DB::toString(right->segment_id) << "]");
    //
    //    SegmentPtr my_left  = left;
    //    SegmentPtr my_right = right;
    //
    //    auto & left_packs  = left->stable->getPacks();
    //    auto & right_packs = right->stable->getPacks();
    //    if (!left_packs.empty() && !right_packs.empty())
    //    {
    //        if (left_packs[left_packs.size() - 1].getHandleFirstLast().second > right_packs[0].getHandleFirstLast().first)
    //        {
    //            auto new_left  = left->mergeDelta(dm_context, left_snap, storage_snap, wbs);
    //            auto new_right = right->mergeDelta(dm_context, right_snap, storage_snap, wbs);
    //
    //            my_left  = new_left;
    //            my_right = new_right;
    //        }
    //    }
    //
    //    auto res = doMergeLogical(dm_context, my_left, my_right, wbs);
    //
    //    LOG_DEBUG(left->log, "Done merge segment [" << DB::toString(left->segment_id) << "] with [" << DB::toString(right->segment_id) << "]");
    //
    //    return res;
}

SegmentPtr Segment::merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right)
{
    WriteBatches wbs;
    auto &       storage_pool = dm_context.storage_pool;
    auto         res          = merge(dm_context, left, left->getReadSnapshot(), right, right->getReadSnapshot(), {storage_pool}, wbs);
    wbs.writeAll(storage_pool);
    return res;
}

StableValueSpacePtr Segment::prepareMergeDelta(DMContext &             dm_context,
                                               const SegmentSnapshot & segment_snap,
                                               const StorageSnapshot & storage_snap,
                                               WriteBatches &          wbs) const
{
    LOG_DEBUG(log,
              "Segment [" << DB::toString(segment_id) << "] prepare merge delta start. delta packs: " << DB::toString(delta->num_packs())
                          << ", delta total rows: " << DB::toString(delta->num_rows()));

    EventRecorder recorder(ProfileEvents::DMDeltaMerge, ProfileEvents::DMDeltaMergeNS);

    auto read_info   = getReadInfo<false>(dm_context, dm_context.store_columns, segment_snap, storage_snap, HandleRange::newAll());
    auto data_stream = getPlacedStream(dm_context,
                                       read_info.read_columns,
                                       range,
                                       EMPTY_FILTER,
                                       segment_snap.stable,
                                       read_info.delta_value_space,
                                       read_info.index_begin,
                                       read_info.index_end,
                                       read_info.index->entryCount(),
                                       dm_context.stable_pack_rows);

    data_stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(data_stream, range, 0);
    data_stream = std::make_shared<ReorganizeBlockInputStream>(data_stream, dm_context.handle_column.name);
    data_stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
        data_stream, read_info.read_columns, dm_context.min_version);

    auto new_stable = createNewStable(dm_context, data_stream, segment_snap.stable->getId(), wbs);

    LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] prepare merge delta done.");

    return new_stable;
}

SegmentPtr Segment::applyMergeDelta(const SegmentSnapshot & segment_snap, WriteBatches & wbs, const StableValueSpacePtr & new_stable) const
{
    LOG_DEBUG(log, "Before apply merge delta: " << info());

    /// Here we use current delta instead of delta in segment_snap,
    /// because there could be new data appended into delta during preparing merge delta.

    auto remove_delta_packs = delta->getPacksBefore(segment_snap.delta_rows, segment_snap.delta_deletes);
    auto new_delta_packs    = delta->getPacksAfter(segment_snap.delta_rows, segment_snap.delta_deletes);

    auto new_delta = std::make_shared<DiskValueSpace>(true, delta->pageId(), std::move(remove_delta_packs));
    if (segment_snap.delta_has_cache)
    {
        // The delta data we just merge already contains the cache part of delta, so no need to copy the cache.
        new_delta->replacePacks(wbs.meta, wbs.removed_log, std::move(new_delta_packs));
    }
    else
    {
        new_delta->replacePacks(wbs.meta, //
                                 wbs.removed_log,
                                 std::move(new_delta_packs),
                                 delta->cloneCache(),
                                 delta->cachePacks());
    }

    auto new_me = std::make_shared<Segment>(epoch + 1, //
                                            range,
                                            segment_id,
                                            next_segment_id,
                                            new_delta,
                                            new_stable);

    // Store new meta data
    new_me->serialize(wbs.meta);
    // Remove old stable's files.
    for (auto & file : stable->getDMFiles())
    {
        // Here we should remove the ref id instead of file_id.
        // Because a dmfile could be used by several segments, and only after all ref_ids are removed, then the file_id removed.
        wbs.removed_data.delPage(file->refId());
    }

    LOG_DEBUG(log, "After apply merge delta new segment: " << new_me->info());

    return new_me;
}

SegmentPtr Segment::mergeDelta(DMContext &             dm_context,
                               const SegmentSnapshot & segment_snap,
                               const StorageSnapshot & storage_snap,
                               WriteBatches &          wbs) const
{
    auto new_stable = prepareMergeDelta(dm_context, segment_snap, storage_snap, wbs);
    return applyMergeDelta(segment_snap, wbs, new_stable);
}

SegmentPtr Segment::mergeDelta(DMContext & dm_context) const
{
    WriteBatches wbs;
    auto &       storage_pool = dm_context.storage_pool;
    auto         res          = mergeDelta(dm_context, getReadSnapshot(), {storage_pool}, wbs);
    wbs.writeAll(storage_pool);
    return res;
}

void Segment::check(DMContext & dm_context, const String & when) const
{
    auto &     storage_pool = dm_context.storage_pool;
    PageReader reader(storage_pool.meta(), storage_pool.meta().getSnapshot());
    delta->check(reader, when);
    //    stable->check(reader, when);
}

void Segment::flushCache(DMContext & dm_context)
{
    WriteBatch remove_log_wb;
    doFlushCache(dm_context, remove_log_wb);
    dm_context.storage_pool.log().write(std::move(remove_log_wb));
}

size_t Segment::getEstimatedRows() const
{
    std::shared_lock lock(read_write_mutex);
    return estimatedRows();
}

size_t Segment::getEstimatedStableRows() const
{
    // Stable is a constant and no need lock.
    return stable->getRows();
}

size_t Segment::getEstimatedBytes() const
{
    std::shared_lock lock(read_write_mutex);
    return estimatedBytes();
}

size_t Segment::getDeltaRawRows(bool with_delta_cache) const
{
    std::shared_lock lock(read_write_mutex);
    auto             rows = delta->num_rows();
    if (!with_delta_cache)
        rows -= delta->cacheRows();
    return rows;
}

size_t Segment::updatesInDeltaTree() const
{
    return delta_tree->numInserts() + delta_tree->numDeletes();
}

String Segment::simpleInfo() const
{
    return "{" + DB::toString(segment_id) + ":" + range.toString() + "}";
}

String Segment::info() const
{
    return "{id:" + DB::toString(segment_id) + ", next: " + DB::toString(next_segment_id) + ", epoch: " + DB::toString(epoch)
        + ", range: " + range.toString() + ", estimated rows: " + DB::toString(estimatedRows()) + ", delta: " + DB::toString(deltaRows())
        + ", stable(" + DB::toString(stable->getDMFilesString()) + "): " + DB::toString(stableRows()) + "}";
}

//==========================================================================================
// Segment private methods.
//==========================================================================================

template <bool add_tag_column>
Segment::ReadInfo Segment::getReadInfo(const DMContext &       dm_context,
                                       const ColumnDefines &   read_columns,
                                       const SegmentSnapshot & segment_snap,
                                       const StorageSnapshot & storage_snap,
                                       const HandleRange &     read_range) const
{
    LOG_TRACE(log, "getReadInfo start");

    auto new_read_columns = arrangeReadColumns<add_tag_column>(dm_context.handle_column, read_columns);
    auto delta_value_space
        = segment_snap.delta->getValueSpace(storage_snap.log_reader, new_read_columns, read_range, segment_snap.delta_rows);

    DeltaIndexPtr delta_index;
    if (segment_snap.delta_index)
    {
        delta_index = segment_snap.delta_index;
    }
    else
    {
        delta_index = ensurePlace(dm_context, //
                                  storage_snap,
                                  segment_snap.stable,
                                  *segment_snap.delta,
                                  segment_snap.delta_rows,
                                  segment_snap.delta_deletes,
                                  delta_value_space);
    }

    auto index_begin = DeltaIndex::begin(delta_index);
    auto index_end   = DeltaIndex::end(delta_index);

    LOG_TRACE(log, "getReadInfo end");

    return {
        .storage_snap      = storage_snap,
        .segment_snap      = segment_snap,
        .delta_value_space = delta_value_space,
        .index             = delta_index,
        .index_begin       = index_begin,
        .index_end         = index_end,
        .read_columns      = new_read_columns,
    };
}

template <bool add_tag_column>
ColumnDefines Segment::arrangeReadColumns(const ColumnDefine & handle, const ColumnDefines & columns_to_read)
{
    // We always put handle, version and tag column at the beginning of columns.
    ColumnDefines new_columns_to_read;

    new_columns_to_read.push_back(handle);
    new_columns_to_read.push_back(getVersionColumnDefine());
    if constexpr (add_tag_column)
        new_columns_to_read.push_back(getTagColumnDefine());

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        auto & c  = columns_to_read[i];
        bool   ok = c.id != handle.id && c.id != VERSION_COLUMN_ID;
        if constexpr (add_tag_column)
            ok = ok && c.id != TAG_COLUMN_ID;

        if (ok)
            new_columns_to_read.push_back(c);
    }

    return new_columns_to_read;
}

template <class IndexIterator>
BlockInputStreamPtr Segment::getPlacedStream(const DMContext &           dm_context,
                                             const ColumnDefines &       read_columns,
                                             const HandleRange &         handle_range,
                                             const RSOperatorPtr &       filter,
                                             const StableValueSpacePtr & stable_snap,
                                             const DeltaValueSpacePtr &  delta_value_space,
                                             const IndexIterator &       delta_index_begin,
                                             const IndexIterator &       delta_index_end,
                                             size_t                      index_size,
                                             size_t                      expected_block_size) const
{
    SkippableBlockInputStreamPtr stable_input_stream
        = stable_snap->getInputStream(dm_context, read_columns, handle_range, filter, MAX_UINT64, false);
    return std::make_shared<DeltaMergeBlockInputStream<DeltaValueSpace, IndexIterator>>( //
        stable_input_stream,
        delta_value_space,
        delta_index_begin,
        delta_index_end,
        index_size,
        handle_range,
        expected_block_size);
}

Handle Segment::getSplitPointFast(DMContext & dm_context, const StableValueSpacePtr & stable_snap) const
{
    // FIXME: this method does not consider invalid packs in stable dmfiles.

    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);
    auto          stable_rows = stable_snap->getRows();
    if (unlikely(!stable_rows))
        throw Exception("No stable rows");

    size_t split_row_index = stable_rows / 2;

    auto & dmfiles = stable_snap->getDMFiles();

    DMFilePtr read_file;
    auto      read_pack        = std::make_shared<IdSet>();
    size_t    read_row_in_pack = 0;

    size_t cur_rows = 0;
    for (auto & file : dmfiles)
    {
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

                    read_file = file;
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
                                  {getExtraHandleColumnDefine()},
                                  HandleRange::newAll(),
                                  EMPTY_FILTER,
                                  read_pack);

    stream.readSuffix();
    auto block = stream.read();
    if (!block)
        throw Exception("Unexpected empty block");
    stream.readSuffix();

    return block.getByPosition(0).column->getInt(read_row_in_pack);
}

Handle Segment::getSplitPointSlow(DMContext & dm_context, const ReadInfo & read_info) const
{
    EventRecorder recorder(ProfileEvents::DMSegmentGetSplitPoint, ProfileEvents::DMSegmentGetSplitPointNS);

    auto & handle     = dm_context.handle_column;
    size_t exact_rows = 0;

    {
        auto stream = getPlacedStream(dm_context,
                                      {dm_context.handle_column},
                                      range,
                                      EMPTY_FILTER,
                                      read_info.segment_snap.stable,
                                      read_info.delta_value_space,
                                      read_info.index_begin,
                                      read_info.index_end,
                                      read_info.index->entryCount(),
                                      dm_context.stable_pack_rows);
        stream      = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, range, 0);

        stream->readPrefix();
        Block block;
        while ((block = stream->read()))
            exact_rows += block.rows();
        stream->readSuffix();
    }

    auto stream = getPlacedStream(dm_context,
                                  {dm_context.handle_column},
                                  range,
                                  EMPTY_FILTER,
                                  read_info.segment_snap.stable,
                                  read_info.delta_value_space,
                                  read_info.index_begin,
                                  read_info.index_end,
                                  read_info.index->entryCount(),
                                  dm_context.stable_pack_rows);
    stream      = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, range, 0);

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

    if (!range.check(split_handle))
        throw Exception("getSplitPointSlow unexpected split_handle: " + DB::toString(split_handle) + ", should be in range "
                        + range.toString());

    return split_handle;
}

SegmentPair Segment::doSplitLogical(DMContext &             dm_context,
                                    const SegmentSnapshot & segment_snap,
                                    const StorageSnapshot & storage_snap,
                                    Handle                  split_point,
                                    WriteBatches &          wbs) const
{
    LOG_DEBUG(log, "Segment [" << segment_id << "] split logical");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & storage_pool = dm_context.storage_pool;

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

    if (my_range.none() || other_range.none())
        throw Exception("doSplitLogical: unexpected range! my_range: " + my_range.toString() + ", other_range: " + other_range.toString());

    GenPageId log_gen_page_id = std::bind(&StoragePool::newLogPageId, &storage_pool);

    Packs my_delta_packs    = createRefPacks(segment_snap.delta->getPacks(), log_gen_page_id, wbs.log);
    Packs other_delta_packs = createRefPacks(segment_snap.delta->getPacks(), log_gen_page_id, wbs.log);

    DMFiles my_stable_files;
    DMFiles other_stable_files;

    for (auto & dmfile : segment_snap.stable->getDMFiles())
    {
        auto ori_ref_id = dmfile->refId();
        auto file_id    = storage_snap.data_reader.getNormalPageId(ori_ref_id);
        auto page_entry = storage_snap.data_reader.getPageEntry(ori_ref_id);
        if (!page_entry.isValid())
            throw Exception("Page entry of " + DB::toString(ori_ref_id) + " not found!");
        auto path_id          = page_entry.tag;
        auto file_parent_path = dm_context.extra_paths.getPath(path_id) + "/" + STABLE_FOLDER_NAME;

        auto my_dmfile_id    = storage_pool.newDataPageId();
        auto other_dmfile_id = storage_pool.newDataPageId();

        wbs.data.putRefPage(my_dmfile_id, file_id);
        wbs.data.putRefPage(other_dmfile_id, file_id);
        wbs.removed_data.delPage(ori_ref_id);

        auto my_dmfile    = DMFile::restore(file_id, /* ref_id= */ my_dmfile_id, file_parent_path);
        auto other_dmfile = DMFile::restore(file_id, /* ref_id= */ other_dmfile_id, file_parent_path);

        my_stable_files.push_back(my_dmfile);
        other_stable_files.push_back(other_dmfile);
    }

    auto other_segment_id = storage_pool.newMetaPageId();
    auto other_delta_id   = storage_pool.newMetaPageId();
    auto other_stable_id  = storage_pool.newMetaPageId();

    auto my_stable    = std::make_shared<StableValueSpace>(segment_snap.stable->getId());
    auto other_stable = std::make_shared<StableValueSpace>(other_stable_id);

    my_stable->setFiles(my_stable_files, &dm_context, my_range);
    other_stable->setFiles(other_stable_files, &dm_context, other_range);

    auto my_delta    = std::make_shared<DiskValueSpace>(*segment_snap.delta);
    auto other_delta = std::make_shared<DiskValueSpace>(true, other_delta_id);

    auto new_me = std::make_shared<Segment>(this->epoch + 1, //
                                            my_range,
                                            this->segment_id,
                                            other_segment_id,
                                            my_delta,
                                            my_stable);

    auto other = std::make_shared<Segment>(INITIAL_EPOCH, //
                                           other_range,
                                           other_segment_id,
                                           this->next_segment_id,
                                           other_delta,
                                           other_stable);

    // Here remove the old pages in old delta packs.
    new_me->delta->replacePacks(wbs.meta, //
                                 wbs.removed_log,
                                 std::move(my_delta_packs),
                                 segment_snap.delta->cloneCache(),
                                 segment_snap.delta->cachePacks());
    new_me->stable->saveMeta(wbs.meta);
    new_me->serialize(wbs.meta);

    other->delta->replacePacks(wbs.meta, //
                                wbs.removed_log,
                                std::move(other_delta_packs),
                                segment_snap.delta->cloneCache(),
                                segment_snap.delta->cachePacks());
    other->stable->saveMeta(wbs.meta);
    other->serialize(wbs.meta);


    return {new_me, other};
}

SegmentPair Segment::doSplitPhysical(DMContext &             dm_context,
                                     const SegmentSnapshot & segment_snap,
                                     const StorageSnapshot & storage_snap,
                                     WriteBatches &          wbs) const
{
    LOG_DEBUG(log, "Segment [" << segment_id << "] split physical");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & storage_pool = dm_context.storage_pool;
    auto   read_info    = getReadInfo<false>(dm_context, dm_context.store_columns, segment_snap, storage_snap, HandleRange::newAll());

    auto split_point = getSplitPointSlow(dm_context, read_info);

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

    if (my_range.none() || other_range.none())
        throw Exception("doSplitPhysical: unexpected range! my_range: " + my_range.toString() + ", other_range: " + other_range.toString());

    StableValueSpacePtr my_new_stable;
    StableValueSpacePtr other_stable;

    {
        // Write my data
        BlockInputStreamPtr my_data = getPlacedStream(dm_context,
                                                      read_info.read_columns,
                                                      my_range,
                                                      EMPTY_FILTER,
                                                      segment_snap.stable,
                                                      read_info.delta_value_space,
                                                      read_info.index_begin,
                                                      read_info.index_end,
                                                      read_info.index->entryCount(),
                                                      dm_context.stable_pack_rows);
        my_data                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(my_data, my_range, 0);
        my_data                     = std::make_shared<ReorganizeBlockInputStream>(my_data, dm_context.handle_column.name);
        my_data                     = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            my_data, read_info.read_columns, dm_context.min_version);
        auto my_stable_id = segment_snap.stable->getId();
        my_new_stable     = createNewStable(dm_context, my_data, my_stable_id, wbs);
    }

    {
        // Write new segment's data
        BlockInputStreamPtr other_data = getPlacedStream(dm_context,
                                                         read_info.read_columns,
                                                         other_range,
                                                         EMPTY_FILTER,
                                                         segment_snap.stable,
                                                         read_info.delta_value_space,
                                                         read_info.index_begin,
                                                         read_info.index_end,
                                                         read_info.index->entryCount(),
                                                         dm_context.stable_pack_rows);
        other_data                     = std::make_shared<DMHandleFilterBlockInputStream<true>>(other_data, other_range, 0);
        other_data                     = std::make_shared<ReorganizeBlockInputStream>(other_data, dm_context.handle_column.name);
        other_data                     = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            other_data, read_info.read_columns, dm_context.min_version);
        auto other_stable_id = dm_context.storage_pool.newMetaPageId();
        other_stable         = createNewStable(dm_context, other_data, other_stable_id, wbs);
    }

    auto other_segment_id = storage_pool.newMetaPageId();
    auto other_delta_id   = storage_pool.newMetaPageId();

    auto new_me = std::make_shared<Segment>(this->epoch + 1, //
                                            my_range,
                                            this->segment_id,
                                            other_segment_id,
                                            std::make_shared<DiskValueSpace>(*segment_snap.delta),
                                            my_new_stable);

    auto other = std::make_shared<Segment>(INITIAL_EPOCH, //
                                           other_range,
                                           other_segment_id,
                                           this->next_segment_id,
                                           std::make_shared<DiskValueSpace>(true, other_delta_id),
                                           other_stable);

    new_me->delta->replacePacks(wbs.meta, wbs.removed_log, {});
    // Remove old stable's files.
    for (auto & file : stable->getDMFiles())
    {
        // Here we should remove the ref id instead of file_id.
        // Because a dmfile could be used by several segments, and only after all ref_ids are removed, then the file_id removed.
        wbs.removed_data.delPage(file->refId());
    }
    new_me->serialize(wbs.meta);

    other->delta->replacePacks(wbs.meta, wbs.removed_log, {});
    other->serialize(wbs.meta);

    return {new_me, other};
}

SegmentPtr Segment::doMergeLogical(
    DMContext & dm_context, const StorageSnapshot & storage_snap, const SegmentPtr & left, const SegmentPtr & right, WriteBatches & wbs)
{
    if (unlikely(left->range.end != right->range.start || left->next_segment_id != right->segment_id))
        throw Exception("The ranges of merge segments are not consecutive: first end: " + DB::toString(left->range.end)
                        + ", second start: " + DB::toString(right->range.start));

    EventRecorder recorder(ProfileEvents::DMSegmentMerge, ProfileEvents::DMSegmentMergeNS);

    auto & storage_pool = dm_context.storage_pool;

    HandleRange merge_range = {left->range.start, right->range.end};

    Packs merged_delta_packs = left->delta->getPacks();
    merged_delta_packs.insert(merged_delta_packs.end(), right->delta->getPacks().begin(), right->delta->getPacks().end());


    DMFiles stable_files;

    auto add_dmfiles = [&](const DMFiles & dmfiles) {
        for (auto & dmfile : dmfiles)
        {
            auto ori_ref_id = dmfile->refId();
            auto file_id    = storage_snap.data_reader.getNormalPageId(ori_ref_id);
            auto page_entry = storage_snap.data_reader.getPageEntry(ori_ref_id);
            if (!page_entry.isValid())
                throw Exception("Page entry of " + DB::toString(ori_ref_id) + " not found!");
            auto path_id          = page_entry.tag;
            auto file_parent_path = dm_context.extra_paths.getPath(path_id) + "/" + STABLE_FOLDER_NAME;

            auto new_dmfile_id = storage_pool.newDataPageId();

            wbs.data.putRefPage(new_dmfile_id, file_id);
            wbs.removed_data.delPage(ori_ref_id);

            auto new_dmfile = DMFile::restore(file_id, /* ref_id= */ new_dmfile_id, file_parent_path);
            stable_files.push_back(new_dmfile);
        }
    };

    add_dmfiles(left->stable->getDMFiles());
    add_dmfiles(right->stable->getDMFiles());

    auto merged_stable = std::make_shared<StableValueSpace>(left->stable->getId());
    merged_stable->setFiles(stable_files, &dm_context, merge_range);

    auto merged = std::make_shared<Segment>(left->epoch + 1, //
                                            merge_range,
                                            left->segment_id,
                                            right->next_segment_id,
                                            std::make_shared<DiskValueSpace>(*(left->delta)),
                                            merged_stable);

    merged->delta->replacePacks(wbs.meta, wbs.removed_log, std::move(merged_delta_packs));
    merged->stable->saveMeta(wbs.meta);
    merged->serialize(wbs.meta);

    return merged;
}

void Segment::doFlushCache(DMContext & dm_context, WriteBatch & remove_log_wb)
{
    std::unique_lock lock(read_write_mutex);

    auto new_delta = delta->tryFlushCache(OpContext::createForLogStorage(dm_context), remove_log_wb, /* force= */ true);
    if (new_delta)
    {
        LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] update delta instance");

        delta = new_delta;
    }
}

DeltaIndexPtr Segment::ensurePlace(const DMContext &           dm_context,
                                   const StorageSnapshot &     storage_snapshot,
                                   const StableValueSpacePtr & stable_snap,
                                   const DiskValueSpace &      to_place_delta,
                                   size_t                      delta_rows_limit,
                                   size_t                      delta_deletes_limit,
                                   const DeltaValueSpacePtr &  delta_value_space) const
{
    // Synchronize between read/read threads.
    std::scoped_lock lock(read_read_mutex);

    DeltaTreePtr update_delta_tree;
    size_t       my_placed_delta_rows;
    size_t       my_placed_delta_deletes;
    bool         is_update_local_delta_tree;

    // Already placed.
    if (placed_delta_rows == delta_rows_limit && placed_delta_deletes == delta_deletes_limit)
        return delta_tree->getEntriesCopy<Allocator<false>>();

    if (placed_delta_rows > delta_rows_limit || placed_delta_deletes > delta_deletes_limit)
    {
        // Current delta_tree in Segment is too new for those delta_rows_limit and delta_deletes_limit, we must recreate another one.
        update_delta_tree          = std::make_shared<DefaultDeltaTree>();
        my_placed_delta_rows       = 0;
        my_placed_delta_deletes    = 0;
        is_update_local_delta_tree = false;
    }
    else
    {
        update_delta_tree          = delta_tree;
        my_placed_delta_rows       = placed_delta_rows;
        my_placed_delta_deletes    = placed_delta_deletes;
        is_update_local_delta_tree = true;
    }

    EventRecorder recorder(ProfileEvents::DMPlace, ProfileEvents::DMPlaceNS);

    auto blocks = to_place_delta.getMergeBlocks(dm_context.handle_column,
                                                storage_snapshot.log_reader,
                                                my_placed_delta_rows,
                                                my_placed_delta_deletes,
                                                delta_rows_limit,
                                                delta_deletes_limit);

    for (auto & v : blocks)
    {
        if (!v.delete_range.none())
        {
            placeDelete(dm_context, stable_snap, delta_value_space, v.delete_range, *update_delta_tree);
            ++my_placed_delta_deletes;
        }
        else if (v.block)
        {
            auto rows = v.block.rows();
            placeUpsert(dm_context, stable_snap, delta_value_space, my_placed_delta_rows, std::move(v.block), *update_delta_tree);
            my_placed_delta_rows += rows;
        }
    }

    if (unlikely(my_placed_delta_rows != delta_rows_limit || my_placed_delta_deletes != delta_deletes_limit))
        throw Exception("Illegal status: place delta rows and deletes are not equal to requested limit");

    if (is_update_local_delta_tree)
    {
        placed_delta_rows    = my_placed_delta_rows;
        placed_delta_deletes = my_placed_delta_deletes;
    }

    return update_delta_tree->getEntriesCopy<Allocator<false>>();
}

void Segment::placeUpsert(const DMContext &           dm_context,
                          const StableValueSpacePtr & stable_snap,
                          const DeltaValueSpacePtr &  delta_value_space,
                          size_t                      delta_value_space_offset,
                          Block &&                    block,
                          DeltaTree &                 update_delta_tree) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceUpsert, ProfileEvents::DMPlaceUpsertNS);

    auto & handle            = dm_context.handle_column;
    auto   delta_index_begin = update_delta_tree.begin();
    auto   delta_index_end   = update_delta_tree.end();

    BlockInputStreamPtr merged_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
        dm_context,
        {handle, getVersionColumnDefine()},
        HandleRange::newAll(),
        EMPTY_FILTER,
        stable_snap,
        delta_value_space,
        delta_index_begin,
        delta_index_end,
        update_delta_tree.numEntries(),
        dm_context.stable_pack_rows);

    IColumn::Permutation perm;
    if (sortBlockByPk(handle, block, perm))
        DM::placeInsert<true>(merged_stream, block, update_delta_tree, delta_value_space_offset, perm, getPkSort(handle));
    else
        DM::placeInsert<false>(merged_stream, block, update_delta_tree, delta_value_space_offset, perm, getPkSort(handle));
}

void Segment::placeDelete(const DMContext &           dm_context,
                          const StableValueSpacePtr & stable_snap,
                          const DeltaValueSpacePtr &  delta_value_space,
                          const HandleRange &         delete_range,
                          DeltaTree &                 update_delta_tree) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceDeleteRange, ProfileEvents::DMPlaceDeleteRangeNS);

    auto & handle = dm_context.handle_column;

    Blocks delete_data;
    {
        auto delta_index_begin = update_delta_tree.begin();
        auto delta_index_end   = update_delta_tree.end();

        BlockInputStreamPtr delete_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
            dm_context,
            {handle, getVersionColumnDefine()},
            delete_range,
            EMPTY_FILTER,
            stable_snap,
            delta_value_space,
            delta_index_begin,
            delta_index_end,
            update_delta_tree.numEntries(),
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

    // Note that we can not do read and place at the same time.
    for (const auto & block : delete_data)
    {
        auto delta_index_begin = update_delta_tree.begin();
        auto delta_index_end   = update_delta_tree.end();

        BlockInputStreamPtr merged_stream = getPlacedStream<DefaultDeltaTree::EntryIterator>( //
            dm_context,
            {handle, getVersionColumnDefine()},
            HandleRange::newAll(),
            EMPTY_FILTER,
            stable_snap,
            delta_value_space,
            delta_index_begin,
            delta_index_end,
            update_delta_tree.numEntries(),
            dm_context.stable_pack_rows);
        DM::placeDelete(merged_stream, block, update_delta_tree, getPkSort(handle));
    }
}

size_t Segment::stableRows() const
{
    return stable->getRows();
}

size_t Segment::deltaRows() const
{
    // Not 100% accurate.
    ssize_t rows = delta_tree->numInserts() - delta_tree->numDeletes() + (delta->num_rows() - placed_delta_rows);
    return std::max(0, rows);
}

size_t Segment::estimatedRows() const
{
    return stableRows() + deltaRows();
}

size_t Segment::estimatedBytes() const
{
    size_t stable_bytes = stable->getBytes();
    if (!stable_bytes)
    {
        return delta->num_bytes();
    }
    else
    {
        return stable_bytes + delta->num_bytes() - (stable_bytes / stable->getRows()) * delta_tree->numDeletes();
    }
}

} // namespace DM
} // namespace DB
