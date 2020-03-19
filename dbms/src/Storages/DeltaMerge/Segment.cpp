#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
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
#include <Storages/DeltaMerge/FilterHelper.h>
#include <Storages/DeltaMerge/ReorganizeBlockInputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StableValueSpace.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

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
    auto   output_stream = std::make_shared<DMFileBlockOutputStream>(dm_context.db_context, dmfile, *dm_context.store_columns);
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
    auto & store_path = context.extra_paths.choosePath();

    PageId dmfile_id = context.storage_pool.newDataPageId();
    auto   dmfile    = writeIntoNewDMFile(context, input_stream, dmfile_id, store_path + "/" + STABLE_FOLDER_NAME);
    auto   stable    = std::make_shared<StableValueSpace>(stable_id);
    stable->setFiles({dmfile});
    stable->saveMeta(wbs.meta);
    wbs.data.putExternal(dmfile_id, 0);
    context.extra_paths.addDMFile(dmfile_id, dmfile->getBytes(), store_path);

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
      delta_tree(std::make_shared<DefaultDeltaTree>()),
      log(&Logger::get("Segment"))
{
}

SegmentPtr Segment::newSegment(
    DMContext & context, const HandleRange & range, PageId segment_id, PageId next_segment_id, PageId delta_id, PageId stable_id)
{
    WriteBatches wbs(context.storage_pool);

    auto delta  = std::make_shared<DeltaValueSpace>(delta_id);
    auto stable = createNewStable(context, std::make_shared<EmptySkippableBlockInputStream>(*context.store_columns), stable_id, wbs);

    auto segment = std::make_shared<Segment>(INITIAL_EPOCH, range, segment_id, next_segment_id, delta, stable);

    // Write metadata.
    delta->saveMeta(wbs);
    stable->saveMeta(wbs.meta);
    segment->serialize(wbs.meta);

    wbs.writeAll();
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
        delta->flush(dm_context);
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
        LOG_WARNING(log, "Try to write an invalid delete range " << delete_range.toString() << " into " << simpleInfo());
        return true;
    }

    LOG_TRACE(log, "Segment [" << segment_id << "] write delete range: " << delete_range.toString());
    return delta->appendDeleteRange(dm_context, delete_range);
}

SegmentSnapshotPtr Segment::createSnapshot(const DMContext & dm_context, bool is_update) const
{
    return std::make_shared<SegmentSnapshot>(delta->createSnapshot(dm_context, is_update), stable);
}

BlockInputStreamPtr Segment::getInputStream(const DMContext &          dm_context,
                                            const ColumnDefines &      columns_to_read,
                                            const SegmentSnapshotPtr & segment_snap,
                                            const HandleRanges &       read_ranges,
                                            const RSOperatorPtr &      filter,
                                            UInt64                     max_version,
                                            size_t                     expected_block_size)
{
    LOG_TRACE(log, "Segment [" << segment_id << "] create InputStream");

    auto read_info = getReadInfo<true>(dm_context, columns_to_read, segment_snap);

    auto create_stream = [&](const HandleRange & read_range) -> BlockInputStreamPtr {
        BlockInputStreamPtr stream;
        if (dm_context.read_delta_only)
        {
            throw Exception("Unsupported");
        }
        else if (dm_context.read_stable_only)
        {
            stream = segment_snap->stable->getInputStream(dm_context, read_info.read_columns, read_range, filter, max_version, false);
        }
        else if (segment_snap->delta->rows == 0 && segment_snap->delta->deletes == 0 //
                 && !hasColumn(columns_to_read, EXTRA_HANDLE_COLUMN_ID)              //
                 && !hasColumn(columns_to_read, VERSION_COLUMN_ID)                   //
                 && !hasColumn(columns_to_read, TAG_COLUMN_ID))
        {
            // No delta, let's try some optimizations.
            stream = segment_snap->stable->getInputStream(dm_context, read_info.read_columns, read_range, filter, max_version, true);
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

    auto segment_snap = createSnapshot(dm_context);
    return getInputStream(dm_context, columns_to_read, segment_snap, read_ranges, filter, max_version, expected_block_size);
}

BlockInputStreamPtr Segment::getInputStreamRaw(const DMContext &          dm_context,
                                               const ColumnDefines &      columns_to_read,
                                               const SegmentSnapshotPtr & segment_snap,
                                               bool                       do_range_filter)
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

    BlockInputStreamPtr stable_stream
        = segment_snap->stable->getInputStream(dm_context, new_columns_to_read, range, EMPTY_FILTER, MAX_UINT64, false);

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
    return getInputStreamRaw(dm_context, columns_to_read, segment_snap, true);
}

SegmentPtr Segment::mergeDelta(DMContext & dm_context) const
{
    WriteBatches wbs(dm_context.storage_pool);
    auto         segment_snap = createSnapshot(dm_context, true);
    if (!segment_snap)
        return {};

    auto new_stable = prepareMergeDelta(dm_context, segment_snap, wbs);

    wbs.writeLogAndData();
    new_stable->enableDMFilesGC();

    auto lock        = mustGetUpdateLock();
    auto new_segment = applyMergeDelta(dm_context, segment_snap, wbs, new_stable);

    wbs.writeAll();
    return new_segment;
}

StableValueSpacePtr Segment::prepareMergeDelta(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap, WriteBatches & wbs) const
{
    LOG_DEBUG(log,
              "Segment [" << DB::toString(segment_id)
                          << "] prepare merge delta start. delta packs: " << DB::toString(segment_snap->delta->getPackCount())
                          << ", delta total rows: " << DB::toString(segment_snap->delta->getRows()));

    EventRecorder recorder(ProfileEvents::DMDeltaMerge, ProfileEvents::DMDeltaMergeNS);

    auto read_info = getReadInfo<false>(dm_context, *dm_context.store_columns, segment_snap);

    BlockInputStreamPtr data_stream = getPlacedStream(dm_context,
                                                      read_info.read_columns,
                                                      range,
                                                      EMPTY_FILTER,
                                                      segment_snap->stable,
                                                      segment_snap->delta,
                                                      read_info.index_begin,
                                                      read_info.index_end,
                                                      read_info.index->entryCount(),
                                                      dm_context.stable_pack_rows);

    data_stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(data_stream, range, 0);
    data_stream = std::make_shared<ReorganizeBlockInputStream>(data_stream, EXTRA_HANDLE_COLUMN_NAME);
    data_stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
        data_stream, read_info.read_columns, dm_context.min_version);

    auto new_stable = createNewStable(dm_context, data_stream, segment_snap->stable->getId(), wbs);

    LOG_DEBUG(log, "Segment [" << DB::toString(segment_id) << "] prepare merge delta done.");

    return new_stable;
}

SegmentPtr Segment::applyMergeDelta(DMContext &                 context,
                                    const SegmentSnapshotPtr &  segment_snap,
                                    WriteBatches &              wbs,
                                    const StableValueSpacePtr & new_stable) const
{
    LOG_DEBUG(log, "Before apply merge delta: " << info());

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

    LOG_DEBUG(log, "After apply merge delta new segment: " << new_me->info());

    return new_me;
}

SegmentPair Segment::split(DMContext & dm_context) const
{
    WriteBatches wbs(dm_context.storage_pool);
    auto         segment_snap = createSnapshot(dm_context, true);
    if (!segment_snap)
        return {};

    auto split_info = prepareSplit(dm_context, segment_snap, wbs);

    wbs.writeLogAndData();
    split_info.my_stable->enableDMFilesGC();
    split_info.other_stable->enableDMFilesGC();

    auto lock         = mustGetUpdateLock();
    auto segment_pair = applySplit(dm_context, segment_snap, wbs, split_info);

    wbs.writeAll();

    return segment_pair;
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

Handle Segment::getSplitPointSlow(DMContext & dm_context, const ReadInfo & read_info, const SegmentSnapshotPtr & segment_snap) const
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
                                                     read_info.index->entryCount(),
                                                     dm_context.stable_pack_rows);

        stream = std::make_shared<DMHandleFilterBlockInputStream<true>>(stream, range, 0);

        stream->readPrefix();
        Block block;
        while ((block = stream->read()))
            exact_rows += block.rows();
        stream->readSuffix();
    }

    BlockInputStreamPtr stream = getPlacedStream(dm_context,
                                                 {handle},
                                                 range,
                                                 EMPTY_FILTER,
                                                 segment_snap->stable,
                                                 segment_snap->delta,
                                                 read_info.index_begin,
                                                 read_info.index_end,
                                                 read_info.index->entryCount(),
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

    if (!range.check(split_handle))
        throw Exception("getSplitPointSlow unexpected split_handle: " + DB::toString(split_handle) + ", should be in range "
                        + range.toString());

    return split_handle;
}

Segment::SplitInfo Segment::prepareSplit(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap, WriteBatches & wbs) const
{
    if (!dm_context.enable_logical_split         //
        || segment_snap->stable->getPacks() <= 3 //
        || segment_snap->delta->getRows() > segment_snap->stable->getRows())
        return prepareSplitPhysical(dm_context, segment_snap, wbs);
    else
    {
        Handle split_point     = getSplitPointFast(dm_context, segment_snap->stable);
        bool   bad_split_point = !range.check(split_point) || split_point == range.start;
        if (bad_split_point)
            return prepareSplitPhysical(dm_context, segment_snap, wbs);
        else
            return prepareSplitLogical(dm_context, segment_snap, split_point, wbs);
    }
}

Segment::SplitInfo
Segment::prepareSplitLogical(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap, Handle split_point, WriteBatches & wbs) const
{
    LOG_DEBUG(log, "Segment [" << segment_id << "] prepare split logical start");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto & storage_pool = dm_context.storage_pool;

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

    if (my_range.none() || other_range.none())
        throw Exception("prepareSplitLogical: unexpected range! my_range: " + my_range.toString()
                        + ", other_range: " + other_range.toString());

    GenPageId log_gen_page_id = std::bind(&StoragePool::newLogPageId, &storage_pool);

    DMFiles my_stable_files;
    DMFiles other_stable_files;

    for (auto & dmfile : segment_snap->stable->getDMFiles())
    {
        auto ori_ref_id       = dmfile->refId();
        auto file_id          = segment_snap->delta->storage_snap->data_reader.getNormalPageId(ori_ref_id);
        auto file_parent_path = dm_context.extra_paths.getPath(file_id) + "/" + STABLE_FOLDER_NAME;

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

    auto other_stable_id = storage_pool.newMetaPageId();

    auto my_stable    = std::make_shared<StableValueSpace>(segment_snap->stable->getId());
    auto other_stable = std::make_shared<StableValueSpace>(other_stable_id);

    my_stable->setFiles(my_stable_files, &dm_context, my_range);
    other_stable->setFiles(other_stable_files, &dm_context, other_range);

    LOG_DEBUG(log, "Segment [" << segment_id << "] prepare split logical done");

    return {true, split_point, my_stable, other_stable};
}

Segment::SplitInfo Segment::prepareSplitPhysical(DMContext & dm_context, const SegmentSnapshotPtr & segment_snap, WriteBatches & wbs) const
{
    LOG_DEBUG(log, "Segment [" << segment_id << "] prepare split physical start");

    EventRecorder recorder(ProfileEvents::DMSegmentSplit, ProfileEvents::DMSegmentSplitNS);

    auto read_info   = getReadInfo<false>(dm_context, *dm_context.store_columns, segment_snap);
    auto split_point = getSplitPointSlow(dm_context, read_info, segment_snap);

    HandleRange my_range    = {range.start, split_point};
    HandleRange other_range = {split_point, range.end};

    if (my_range.none() || other_range.none())
        throw Exception("prepareSplitPhysical: unexpected range! my_range: " + my_range.toString()
                        + ", other_range: " + other_range.toString());

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
                                                      read_info.index->entryCount(),
                                                      dm_context.stable_pack_rows);

        LOG_DEBUG(log, "Created my placed stream");

        my_data = std::make_shared<DMHandleFilterBlockInputStream<true>>(my_data, my_range, 0);
        my_data = std::make_shared<ReorganizeBlockInputStream>(my_data, EXTRA_HANDLE_COLUMN_NAME);
        my_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            my_data, read_info.read_columns, dm_context.min_version);
        auto my_stable_id = segment_snap->stable->getId();
        my_new_stable     = createNewStable(dm_context, my_data, my_stable_id, wbs);
    }

    LOG_DEBUG(log, "prepare my_new_stable done");

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
                                                         read_info.index->entryCount(),
                                                         dm_context.stable_pack_rows);

        LOG_DEBUG(log, "Created other placed stream");

        other_data = std::make_shared<DMHandleFilterBlockInputStream<true>>(other_data, other_range, 0);
        other_data = std::make_shared<ReorganizeBlockInputStream>(other_data, EXTRA_HANDLE_COLUMN_NAME);
        other_data = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
            other_data, read_info.read_columns, dm_context.min_version);
        auto other_stable_id = dm_context.storage_pool.newMetaPageId();
        other_stable         = createNewStable(dm_context, other_data, other_stable_id, wbs);
    }

    LOG_DEBUG(log, "prepare other_stable done");

    // Remove old stable's files.
    for (auto & file : stable->getDMFiles())
    {
        // Here we should remove the ref id instead of file_id.
        // Because a dmfile could be used by several segments, and only after all ref_ids are removed, then the file_id removed.
        wbs.removed_data.delPage(file->refId());
    }

    LOG_DEBUG(log, "Segment [" << segment_id << "] prepare split physical end");

    return {false, split_point, my_new_stable, other_stable};
}

SegmentPair Segment::applySplit(DMContext &                dm_context, //
                                const SegmentSnapshotPtr & segment_snap,
                                WriteBatches &             wbs,
                                SplitInfo &                split_info) const
{
    LOG_DEBUG(log, "Segment [" << segment_id << "] apply split");

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

    LOG_DEBUG(log, "Segment " << info() << " split into " << new_me->info() << " and " << other->info());

    return {new_me, other};
}

SegmentPtr Segment::merge(DMContext & dm_context, const SegmentPtr & left, const SegmentPtr & right)
{
    WriteBatches wbs(dm_context.storage_pool);

    auto left_snap  = left->createSnapshot(dm_context, true);
    auto right_snap = right->createSnapshot(dm_context, true);
    if (!left_snap || !right_snap)
        return {};

    auto merged_stable = prepareMerge(dm_context, left, left_snap, right, right_snap, wbs);

    wbs.writeLogAndData();
    merged_stable->enableDMFilesGC();

    auto left_lock  = left->mustGetUpdateLock();
    auto right_lock = right->mustGetUpdateLock();

    auto merged = applyMerge(dm_context, left, left_snap, right, right_snap, wbs, merged_stable);

    wbs.writeAll();
    return merged;
}

StableValueSpacePtr Segment::prepareMerge(DMContext &                dm_context, //
                                          const SegmentPtr &         left,
                                          const SegmentSnapshotPtr & left_snap,
                                          const SegmentPtr &         right,
                                          const SegmentSnapshotPtr & right_snap,
                                          WriteBatches &             wbs)
{
    LOG_DEBUG(left->log, "Segment [" << left->segmentId() << "] and [" << right->segmentId() << "] prepare merge start");

    if (unlikely(left->range.end != right->range.start || left->next_segment_id != right->segment_id))
        throw Exception("The ranges of merge segments are not consecutive: first end: " + DB::toString(left->range.end)
                        + ", second start: " + DB::toString(right->range.start));

    auto getStream = [&](const SegmentPtr & segment, const SegmentSnapshotPtr & segment_snap) {
        auto                read_info = segment->getReadInfo<false>(dm_context, *dm_context.store_columns, segment_snap);
        BlockInputStreamPtr stream    = segment->getPlacedStream(dm_context,
                                                              read_info.read_columns,
                                                              segment->range,
                                                              EMPTY_FILTER,
                                                              segment_snap->stable,
                                                              segment_snap->delta,
                                                              read_info.index_begin,
                                                              read_info.index_end,
                                                              read_info.index->entryCount(),
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
    auto merged_stable    = createNewStable(dm_context, merged_stream, merged_stable_id, wbs);

    LOG_DEBUG(left->log, "Segment [" << left->segmentId() << "] and [" << right->segmentId() << "] prepare merge end");

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
    LOG_DEBUG(left->log, "Segment [" << left->segmentId() << "] and [" << right->segmentId() << "] apply merge");

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

    LOG_DEBUG(left->log, "Segment [" << left->info() << "] and [" << right->info() << "] merged into " << merged->info());

    return merged;
}

void Segment::check(DMContext &, const String &) const {}

bool Segment::flushCache(DMContext & dm_context)
{
    return delta->flush(dm_context);
}

size_t Segment::getEstimatedRows() const
{
    return estimatedRows();
}

size_t Segment::getEstimatedStableRows() const
{
    // Stable is a constant and no need lock.
    return stable->getRows();
}

size_t Segment::getEstimatedBytes() const
{
    return estimatedBytes();
}

size_t Segment::getDeltaRawRows(bool use_unsaved) const
{
    return delta->getRows(use_unsaved);
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

template <bool add_tag_column>
Segment::ReadInfo
Segment::getReadInfo(const DMContext & dm_context, const ColumnDefines & read_columns, const SegmentSnapshotPtr & segment_snap) const
{
    LOG_DEBUG(log, "getReadInfo start");

    auto new_read_columns = arrangeReadColumns<add_tag_column>(getExtraHandleColumnDefine(), read_columns);
    segment_snap->delta->prepare(dm_context, new_read_columns);

    DeltaIndexPtr delta_index = ensurePlace(dm_context, segment_snap->stable, segment_snap->delta);

    auto index_begin = DeltaIndex::begin(delta_index);
    auto index_end   = DeltaIndex::end(delta_index);

    LOG_DEBUG(log, "getReadInfo end");

    return {
        .index        = delta_index,
        .index_begin  = index_begin,
        .index_end    = index_end,
        .read_columns = new_read_columns,
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

template <class IndexIterator, bool skippable_place>
SkippableBlockInputStreamPtr Segment::getPlacedStream(const DMContext &           dm_context,
                                                      const ColumnDefines &       read_columns,
                                                      const HandleRange &         handle_range,
                                                      const RSOperatorPtr &       filter,
                                                      const StableValueSpacePtr & stable_snap,
                                                      DeltaSnapshotPtr &          delta_snap,
                                                      const IndexIterator &       delta_index_begin,
                                                      const IndexIterator &       delta_index_end,
                                                      size_t                      index_size,
                                                      size_t                      expected_block_size) const
{
    SkippableBlockInputStreamPtr stable_input_stream
        = stable_snap->getInputStream(dm_context, read_columns, handle_range, filter, MAX_UINT64, false);
    return std::make_shared<DeltaMergeBlockInputStream<DeltaSnapshot, IndexIterator, skippable_place>>( //
        stable_input_stream,
        delta_snap,
        delta_index_begin,
        delta_index_end,
        index_size,
        handle_range,
        expected_block_size);
}

DeltaIndexPtr
Segment::ensurePlace(const DMContext & dm_context, const StableValueSpacePtr & stable_snap, DeltaSnapshotPtr & delta_snap) const
{
    // Synchronize between read/read threads.
    std::scoped_lock lock(read_read_mutex);

    auto delta_rows_limit    = delta_snap->rows;
    auto delta_deletes_limit = delta_snap->deletes;

    DeltaTreePtr update_delta_tree;
    size_t       my_placed_delta_rows;
    size_t       my_placed_delta_deletes;
    bool         is_update_local_delta_tree;

    // Already placed.
    // If a delta tree contains more rows than our delta snapshot, it is ok. As we ca do filtering later.
    // But we if it contains more delete ranges, we cannot use it. Because delete range cannot be filtered out.
    if (placed_delta_rows >= delta_rows_limit && placed_delta_deletes == delta_deletes_limit)
        return delta_tree->getEntriesCopy<Allocator<false>>();

    if (placed_delta_rows > delta_rows_limit || placed_delta_deletes > delta_deletes_limit)
    {
        // Current delta_tree in Segment is newer than expected, we must recreate another one.
        update_delta_tree          = std::make_shared<DefaultDeltaTree>();
        my_placed_delta_rows       = 0;
        my_placed_delta_deletes    = 0;
        is_update_local_delta_tree = false;
    }
    else
    {
        // Current delta_tree is older, let's update it.
        update_delta_tree          = delta_tree;
        my_placed_delta_rows       = placed_delta_rows;
        my_placed_delta_deletes    = placed_delta_deletes;
        is_update_local_delta_tree = true;
    }

    EventRecorder recorder(ProfileEvents::DMPlace, ProfileEvents::DMPlaceNS);

    auto blocks = delta_snap->getMergeBlocks(my_placed_delta_rows, my_placed_delta_deletes, delta_rows_limit, delta_deletes_limit);

    for (auto & v : blocks)
    {
        if (!v.delete_range.none())
        {

            if (dm_context.enable_skippable_place)
                placeDelete<true>(dm_context, stable_snap, delta_snap, v.delete_range, *update_delta_tree);
            else
                placeDelete<false>(dm_context, stable_snap, delta_snap, v.delete_range, *update_delta_tree);

            ++my_placed_delta_deletes;
        }
        else if (v.block)
        {
            auto rows = v.block.rows();
            if (dm_context.enable_skippable_place)
                placeUpsert<true>(dm_context, stable_snap, delta_snap, my_placed_delta_rows, std::move(v.block), *update_delta_tree);
            else
                placeUpsert<false>(dm_context, stable_snap, delta_snap, my_placed_delta_rows, std::move(v.block), *update_delta_tree);

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

template <bool skippable_place>
void Segment::placeUpsert(const DMContext &           dm_context,
                          const StableValueSpacePtr & stable_snap,
                          DeltaSnapshotPtr &          delta_snap,
                          size_t                      delta_value_space_offset,
                          Block &&                    block,
                          DeltaTree &                 update_delta_tree) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceUpsert, ProfileEvents::DMPlaceUpsertNS);

    IColumn::Permutation perm;

    auto & handle             = getExtraHandleColumnDefine();
    auto   delta_index_begin  = update_delta_tree.begin();
    auto   delta_index_end    = update_delta_tree.end();
    bool   do_sort            = sortBlockByPk(handle, block, perm);
    Handle first_handle       = block.getByPosition(0).column->getInt(0);
    auto   place_handle_range = skippable_place ? HandleRange(first_handle, HandleRange::MAX) : HandleRange::newAll();


    auto merged_stream = getPlacedStream<DefaultDeltaTree::EntryIterator, skippable_place>( //
        dm_context,
        {handle, getVersionColumnDefine()},
        place_handle_range,
        EMPTY_FILTER,
        stable_snap,
        delta_snap,
        delta_index_begin,
        delta_index_end,
        update_delta_tree.numEntries(),
        dm_context.stable_pack_rows);

    if (do_sort)
        DM::placeInsert<true>(merged_stream, block, update_delta_tree, delta_value_space_offset, perm, getPkSort(handle));
    else
        DM::placeInsert<false>(merged_stream, block, update_delta_tree, delta_value_space_offset, perm, getPkSort(handle));
}

template <bool skippable_place>
void Segment::placeDelete(const DMContext &           dm_context,
                          const StableValueSpacePtr & stable_snap,
                          DeltaSnapshotPtr &          delta_snap,
                          const HandleRange &         delete_range,
                          DeltaTree &                 update_delta_tree) const
{
    EventRecorder recorder(ProfileEvents::DMPlaceDeleteRange, ProfileEvents::DMPlaceDeleteRangeNS);

    auto & handle = getExtraHandleColumnDefine();

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
            delta_snap,
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

        Handle first_handle       = block.getByPosition(0).column->getInt(0);
        auto   place_handle_range = skippable_place ? HandleRange(first_handle, HandleRange::MAX) : HandleRange::newAll();

        auto merged_stream = getPlacedStream<DefaultDeltaTree::EntryIterator, skippable_place>( //
            dm_context,
            {handle, getVersionColumnDefine()},
            place_handle_range,
            EMPTY_FILTER,
            stable_snap,
            delta_snap,
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
    ssize_t rows = delta_tree->numInserts() - delta_tree->numDeletes() + (delta->getRows() - placed_delta_rows);
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
        return delta->getBytes();
    }
    else
    {
        return stable_bytes + delta->getBytes() - (stable_bytes / stable->getRows()) * delta_tree->numDeletes();
    }
}

} // namespace DM
} // namespace DB
