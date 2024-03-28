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

#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/StableValueSpace.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/PathPool.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DM
{
void StableValueSpace::setFiles(const DMFiles & files_, const RowKeyRange & range, const DMContext * dm_context)
{
    UInt64 rows = 0;
    UInt64 bytes = 0;

    if (range.all())
    {
        for (const auto & file : files_)
        {
            rows += file->getRows();
            bytes += file->getBytes();
        }
    }
    else if (dm_context != nullptr)
    {
        auto index_cache = dm_context->db_context.getGlobalContext().getMinMaxIndexCache();
        for (const auto & file : files_)
        {
            auto pack_filter = DMFilePackFilter::loadFrom(
                file,
                index_cache,
                /*set_cache_if_miss*/ true,
                {range},
                EMPTY_RS_OPERATOR,
                {},
                dm_context->db_context.getFileProvider(),
                dm_context->getReadLimiter(),
                dm_context->scan_context,
                dm_context->tracing_id);
            auto [file_valid_rows, file_valid_bytes] = pack_filter.validRowsAndBytes();
            rows += file_valid_rows;
            bytes += file_valid_bytes;
        }
    }

    this->valid_rows = rows;
    this->valid_bytes = bytes;
    this->files = files_;
}

void StableValueSpace::saveMeta(WriteBatchWrapper & meta_wb)
{
    MemoryWriteBuffer buf(0, 8192);
    // The method must call `buf.count()` to get the last seralized size before `buf.tryGetReadBuffer`
    auto data_size = saveMeta(buf);
    meta_wb.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

UInt64 StableValueSpace::saveMeta(WriteBuffer & buf) const
{
    writeIntBinary(STORAGE_FORMAT_CURRENT.stable, buf);
    writeIntBinary(valid_rows, buf);
    writeIntBinary(valid_bytes, buf);
    writeIntBinary(static_cast<UInt64>(files.size()), buf);
    for (const auto & f : files)
        writeIntBinary(f->pageId(), buf);

    return buf.count();
}

std::string StableValueSpace::serializeMeta() const
{
    WriteBufferFromOwnString wb;
    saveMeta(wb);
    return wb.releaseStr();
}

StableValueSpacePtr StableValueSpace::restore(DMContext & context, PageIdU64 id)
{
    Page page = context.storage_pool->metaReader()->read(id); // not limit restore
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    return StableValueSpace::restore(context, buf, id);
}

StableValueSpacePtr StableValueSpace::restore(DMContext & context, ReadBuffer & buf, PageIdU64 id)
{
    auto stable = std::make_shared<StableValueSpace>(id);

    UInt64 version, valid_rows, valid_bytes, size;
    readIntBinary(version, buf);
    if (version != StableFormat::V1)
        throw Exception("Unexpected version: " + DB::toString(version));

    readIntBinary(valid_rows, buf);
    readIntBinary(valid_bytes, buf);
    readIntBinary(size, buf);
    UInt64 page_id;
    auto remote_data_store = context.db_context.getSharedContextDisagg()->remote_data_store;
    for (size_t i = 0; i < size; ++i)
    {
        readIntBinary(page_id, buf);

        DMFilePtr dmfile;
        auto path_delegate = context.path_pool->getStableDiskDelegator();
        if (remote_data_store)
        {
            auto wn_ps = context.db_context.getWriteNodePageStorage();
            auto full_page_id = UniversalPageIdFormat::toFullPageId(
                UniversalPageIdFormat::toFullPrefix(context.keyspace_id, StorageType::Data, context.physical_table_id),
                page_id);
            auto full_external_id = wn_ps->getNormalPageId(full_page_id);
            auto local_external_id = UniversalPageIdFormat::getU64ID(full_external_id);
            auto remote_data_location = wn_ps->getCheckpointLocation(full_page_id);
            const auto & lock_key_view = S3::S3FilenameView::fromKey(*(remote_data_location->data_file_id));
            auto file_oid = lock_key_view.asDataFile().getDMFileOID();
            RUNTIME_CHECK(file_oid.keyspace_id == context.keyspace_id);
            RUNTIME_CHECK(file_oid.table_id == context.physical_table_id);
            auto prepared = remote_data_store->prepareDMFile(file_oid, page_id);
            dmfile = prepared->restore(DMFile::ReadMetaMode::all());
            // gc only begin to run after restore so we can safely call addRemoteDTFileIfNotExists here
            path_delegate.addRemoteDTFileIfNotExists(local_external_id, dmfile->getBytesOnDisk());
        }
        else
        {
            auto file_id = context.storage_pool->dataReader()->getNormalPageId(page_id);
            auto file_parent_path = path_delegate.getDTFilePath(file_id);
            dmfile = DMFile::restore(
                context.db_context.getFileProvider(),
                file_id,
                page_id,
                file_parent_path,
                DMFile::ReadMetaMode::all());
            auto res = path_delegate.updateDTFileSize(file_id, dmfile->getBytesOnDisk());
            RUNTIME_CHECK_MSG(res, "update dt file size failed, path={}", dmfile->path());
        }
        stable->files.push_back(dmfile);
    }

    stable->valid_rows = valid_rows;
    stable->valid_bytes = valid_bytes;

    return stable;
}

StableValueSpacePtr StableValueSpace::createFromCheckpoint( //
    [[maybe_unused]] const LoggerPtr & parent_log,
    DMContext & context,
    UniversalPageStoragePtr temp_ps,
    PageIdU64 stable_id,
    WriteBatches & wbs)
{
    auto stable = std::make_shared<StableValueSpace>(stable_id);

    auto stable_page_id = UniversalPageIdFormat::toFullPageId(
        UniversalPageIdFormat::toFullPrefix(context.keyspace_id, StorageType::Meta, context.physical_table_id),
        stable_id);
    auto page = temp_ps->read(stable_page_id);
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());

    // read stable meta info
    UInt64 version, valid_rows, valid_bytes, size;
    {
        readIntBinary(version, buf);
        if (version != StableFormat::V1)
            throw Exception("Unexpected version: " + DB::toString(version));

        readIntBinary(valid_rows, buf);
        readIntBinary(valid_bytes, buf);
        readIntBinary(size, buf);
    }

    auto remote_data_store = context.db_context.getSharedContextDisagg()->remote_data_store;
    for (size_t i = 0; i < size; ++i)
    {
        UInt64 page_id;
        readIntBinary(page_id, buf);
        auto full_page_id = UniversalPageIdFormat::toFullPageId(
            UniversalPageIdFormat::toFullPrefix(context.keyspace_id, StorageType::Data, context.physical_table_id),
            page_id);
        auto remote_data_location = temp_ps->getCheckpointLocation(full_page_id);
        auto data_key_view = S3::S3FilenameView::fromKey(*(remote_data_location->data_file_id)).asDataFile();
        auto file_oid = data_key_view.getDMFileOID();
        auto data_key = data_key_view.toFullKey();
        auto delegator = context.path_pool->getStableDiskDelegator();
        auto new_local_page_id = context.storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
        PS::V3::CheckpointLocation loc{
            .data_file_id = std::make_shared<String>(data_key),
            .offset_in_file = 0,
            .size_in_file = 0,
        };
        wbs.data.putRemoteExternal(new_local_page_id, loc);
        auto prepared = remote_data_store->prepareDMFile(file_oid, new_local_page_id);
        auto dmfile = prepared->restore(DMFile::ReadMetaMode::all());
        wbs.writeLogAndData();
        // new_local_page_id is already applied to PageDirectory so we can safely call addRemoteDTFileIfNotExists here
        delegator.addRemoteDTFileIfNotExists(new_local_page_id, dmfile->getBytesOnDisk());
        stable->files.push_back(dmfile);
    }

    stable->valid_rows = valid_rows;
    stable->valid_bytes = valid_bytes;

    return stable;
}

size_t StableValueSpace::getRows() const
{
    return valid_rows;
}

size_t StableValueSpace::getBytes() const
{
    return valid_bytes;
}

size_t StableValueSpace::getDMFilesBytesOnDisk() const
{
    size_t bytes = 0;
    for (const auto & file : files)
        bytes += file->getBytesOnDisk();
    return bytes;
}

size_t StableValueSpace::getDMFilesPacks() const
{
    size_t packs = 0;
    for (const auto & file : files)
        packs += file->getPacks();
    return packs;
}

size_t StableValueSpace::getDMFilesRows() const
{
    size_t rows = 0;
    for (const auto & file : files)
        rows += file->getRows();
    return rows;
}

size_t StableValueSpace::getDMFilesBytes() const
{
    size_t bytes = 0;
    for (const auto & file : files)
        bytes += file->getBytes();
    return bytes;
}

String StableValueSpace::getDMFilesString()
{
    String s;
    for (auto & file : files)
        s += "dmf_" + DB::toString(file->fileId()) + ",";
    if (!s.empty())
        s.erase(s.length() - 1);
    return s;
}

void StableValueSpace::enableDMFilesGC(DMContext & context)
{
    if (auto data_store = context.db_context.getSharedContextDisagg()->remote_data_store; !data_store)
    {
        for (auto & file : files)
            file->enableGC();
    }
    else
    {
        auto delegator = context.path_pool->getStableDiskDelegator();
        for (auto & file : files)
            delegator.enableGCForRemoteDTFile(file->fileId());
    }
}

void StableValueSpace::recordRemovePacksPages(WriteBatches & wbs) const
{
    for (const auto & file : files)
    {
        // Here we should remove the ref id instead of file_id.
        // Because a dmfile could be used by several segments, and only after all ref_ids are removed, then the file_id removed.
        wbs.removed_data.delPage(file->pageId());
    }
}

void StableValueSpace::calculateStableProperty(
    const DMContext & context,
    const RowKeyRange & rowkey_range,
    bool is_common_handle)
{
    property.gc_hint_version = std::numeric_limits<UInt64>::max();
    property.num_versions = 0;
    property.num_puts = 0;
    property.num_rows = 0;
    for (auto & file : files)
    {
        const auto & pack_stats = file->getPackStats();
        const auto & pack_properties = file->getPackProperties();
        if (pack_stats.empty())
            continue;
        // if PackProperties of this DMFile is empty, this must be an old format file generated by previous version.
        // so we need to create file property for this file.
        // but to keep dmfile immutable, we just cache the result in memory.
        //
        // `new_pack_properties` is the temporary container for the calculation result of this StableValueSpace's pack property.
        // Note that `pack_stats` stores the stat of the whole underlying DTFile,
        // and this Segment may share this DTFile with other Segment. So `pack_stats` may be larger than `new_pack_properties`.
        DMFile::PackProperties new_pack_properties;
        if (pack_properties.property_size() == 0)
        {
            LOG_DEBUG(log, "Try to calculate StableProperty from column data for stable {}", id);
            ColumnDefines read_columns;
            read_columns.emplace_back(getExtraHandleColumnDefine(is_common_handle));
            read_columns.emplace_back(getVersionColumnDefine());
            read_columns.emplace_back(getTagColumnDefine());
            // Note we `RowKeyRange::newAll` instead of `segment_range`,
            // because we need to calculate StableProperty based on the whole DTFile,
            // and then use related info for this StableValueSpace.
            //
            // If we pass `segment_range` instead,
            // then the returned stream is a `SkippableBlockInputStream` which will complicate the implementation
            DMFileBlockInputStreamBuilder builder(context.db_context);
            BlockInputStreamPtr data_stream
                = builder
                      .setRowsThreshold(std::numeric_limits<UInt64>::max()) // because we just read one pack at a time
                      .onlyReadOnePackEveryTime()
                      .setTracingID(fmt::format("{}-calculateStableProperty", context.tracing_id))
                      .build(file, read_columns, RowKeyRanges{rowkey_range}, context.scan_context);
            auto mvcc_stream = std::make_shared<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
                data_stream,
                read_columns,
                0,
                is_common_handle);
            mvcc_stream->readPrefix();
            while (true)
            {
                size_t last_effective_num_rows = mvcc_stream->getEffectiveNumRows();

                Block block = mvcc_stream->read();
                if (!block)
                    break;
                if (!block.rows())
                    continue;

                size_t cur_effective_num_rows = mvcc_stream->getEffectiveNumRows();
                size_t gc_hint_version = mvcc_stream->getGCHintVersion();
                auto * pack_property = new_pack_properties.add_property();
                pack_property->set_num_rows(cur_effective_num_rows - last_effective_num_rows);
                pack_property->set_gc_hint_version(gc_hint_version);
                pack_property->set_deleted_rows(mvcc_stream->getDeletedRows());
            }
            mvcc_stream->readSuffix();
        }
        auto pack_filter = DMFilePackFilter::loadFrom(
            file,
            context.db_context.getGlobalContext().getMinMaxIndexCache(),
            /*set_cache_if_miss*/ false,
            {rowkey_range},
            EMPTY_RS_OPERATOR,
            {},
            context.db_context.getFileProvider(),
            context.getReadLimiter(),
            context.scan_context,
            context.tracing_id);
        const auto & use_packs = pack_filter.getUsePacksConst();
        size_t new_pack_properties_index = 0;
        const bool use_new_pack_properties = pack_properties.property_size() == 0;
        if (use_new_pack_properties)
        {
            const size_t use_packs_count = std::count(use_packs.begin(), use_packs.end(), true);
            RUNTIME_CHECK_MSG(
                static_cast<size_t>(new_pack_properties.property_size()) == use_packs_count,
                "size doesn't match, new_pack_properties_size={} use_packs_size={}",
                new_pack_properties.property_size(),
                use_packs_count);
        }
        for (size_t pack_id = 0; pack_id < use_packs.size(); ++pack_id)
        {
            if (!use_packs[pack_id])
                continue;
            property.num_versions += pack_stats[pack_id].rows;
            property.num_puts += pack_stats[pack_id].rows - pack_stats[pack_id].not_clean;
            if (use_new_pack_properties)
            {
                const auto & pack_property = new_pack_properties.property(new_pack_properties_index);
                property.num_rows += pack_property.num_rows();
                property.gc_hint_version = std::min(property.gc_hint_version, pack_property.gc_hint_version());
                new_pack_properties_index += 1;
            }
            else
            {
                const auto & pack_property = pack_properties.property(pack_id);
                property.num_rows += pack_property.num_rows();
                property.gc_hint_version = std::min(property.gc_hint_version, pack_property.gc_hint_version());
            }
        }
    }
    is_property_cached.store(true, std::memory_order_release);
}


// ================================================
// StableValueSpace::Snapshot
// ================================================

using Snapshot = StableValueSpace::Snapshot;
using SnapshotPtr = std::shared_ptr<Snapshot>;

SnapshotPtr StableValueSpace::createSnapshot()
{
    auto snap = std::make_shared<Snapshot>(this->shared_from_this());
    snap->id = id;
    snap->valid_rows = valid_rows;
    snap->valid_bytes = valid_bytes;

    for (size_t i = 0; i < files.size(); i++)
    {
        auto column_cache = std::make_shared<ColumnCache>();
        snap->column_caches.emplace_back(column_cache);
    }

    return snap;
}

void StableValueSpace::drop(const FileProviderPtr & file_provider)
{
    for (auto & file : files)
    {
        file->remove(file_provider);
    }
}

SkippableBlockInputStreamPtr StableValueSpace::Snapshot::getInputStream(
    const DMContext & context,
    const ColumnDefines & read_columns,
    const RowKeyRanges & rowkey_ranges,
    const RSOperatorPtr & filter,
    UInt64 max_data_version,
    size_t expected_block_size,
    bool enable_handle_clean_read,
    bool is_fast_scan,
    bool enable_del_clean_read,
    const std::vector<IdSetPtr> & read_packs,
    bool need_row_id,
    BitmapFilterPtr bitmap_filter)
{
    LOG_DEBUG(
        log,
        "start_ts: {}, enable_handle_clean_read: {}, is_fast_mode: {}, enable_del_clean_read: {}",
        max_data_version,
        enable_handle_clean_read,
        is_fast_scan,
        enable_del_clean_read);
    SkippableBlockInputStreams streams;
    std::vector<size_t> rows;
    streams.reserve(stable->files.size());
    rows.reserve(stable->files.size());

    size_t last_rows = 0;

    for (size_t i = 0; i < stable->files.size(); i++)
    {
        DMFileBlockInputStreamBuilder builder(context.db_context);
        builder.enableCleanRead(enable_handle_clean_read, is_fast_scan, enable_del_clean_read, max_data_version)
            .setRSOperator(filter)
            .setColumnCache(column_caches[i])
            .setTracingID(context.tracing_id)
            .setRowsThreshold(expected_block_size)
            .setReadPacks(read_packs.size() > i ? read_packs[i] : nullptr);
        if (bitmap_filter)
        {
            builder = builder.setBitmapFilter(
                BitmapFilterView(bitmap_filter, last_rows, last_rows + stable->files[i]->getRows()));
            last_rows += stable->files[i]->getRows();
        }

        streams.push_back(builder.build2(stable->files[i], read_columns, rowkey_ranges, context.scan_context));
        rows.push_back(stable->files[i]->getRows());
    }
    if (need_row_id)
    {
        return std::make_shared<ConcatSkippableBlockInputStream</*need_row_id*/ true>>(
            streams,
            std::move(rows),
            context.scan_context);
    }
    else
    {
        return std::make_shared<ConcatSkippableBlockInputStream</*need_row_id*/ false>>(
            streams,
            std::move(rows),
            context.scan_context);
    }
}

RowsAndBytes StableValueSpace::Snapshot::getApproxRowsAndBytes(const DMContext & context, const RowKeyRange & range)
    const
{
    // Avoid unnecessary reading IO
    if (valid_rows == 0 || range.none())
        return {0, 0};

    size_t match_packs = 0;
    size_t total_match_rows = 0;
    size_t total_match_bytes = 0;
    // Usually, this method will be called for some "cold" key ranges.
    // Loading the index into cache may pollute the cache and make the hot index cache invalid.
    // So don't refill the cache if the index does not exist.
    for (auto & f : stable->files)
    {
        auto filter = DMFilePackFilter::loadFrom(
            f,
            context.db_context.getGlobalContext().getMinMaxIndexCache(),
            /*set_cache_if_miss*/ false,
            {range},
            RSOperatorPtr{},
            IdSetPtr{},
            context.db_context.getFileProvider(),
            context.getReadLimiter(),
            context.scan_context,
            context.tracing_id);
        const auto & pack_stats = f->getPackStats();
        const auto & use_packs = filter.getUsePacksConst();
        for (size_t i = 0; i < pack_stats.size(); ++i)
        {
            if (use_packs[i])
            {
                ++match_packs;
                total_match_rows += pack_stats[i].rows;
                total_match_bytes += pack_stats[i].bytes;
            }
        }
    }
    if (!total_match_rows || !match_packs)
        return {0, 0};
    Float64 avg_pack_rows = static_cast<Float64>(total_match_rows) / match_packs;
    Float64 avg_pack_bytes = static_cast<Float64>(total_match_bytes) / match_packs;
    // By average, the first and last pack are only half covered by the range.
    // And if this range only covers one pack, then return the pack's stat.
    size_t approx_rows = std::max(avg_pack_rows, total_match_rows - avg_pack_rows / 2);
    size_t approx_bytes = std::max(avg_pack_bytes, total_match_bytes - avg_pack_bytes / 2);
    return {approx_rows, approx_bytes};
}

StableValueSpace::Snapshot::AtLeastRowsAndBytesResult //
StableValueSpace::Snapshot::getAtLeastRowsAndBytes(const DMContext & context, const RowKeyRange & range) const
{
    AtLeastRowsAndBytesResult ret{};

    // Usually, this method will be called for some "cold" key ranges.
    // Loading the index into cache may pollute the cache and make the hot index cache invalid.
    // So don't refill the cache if the index does not exist.
    for (size_t file_idx = 0; file_idx < stable->files.size(); ++file_idx)
    {
        const auto & file = stable->files[file_idx];
        auto filter = DMFilePackFilter::loadFrom(
            file,
            context.db_context.getGlobalContext().getMinMaxIndexCache(),
            /*set_cache_if_miss*/ false,
            {range},
            RSOperatorPtr{},
            IdSetPtr{},
            context.db_context.getFileProvider(),
            context.getReadLimiter(),
            context.scan_context,
            context.tracing_id);
        const auto & handle_filter_result = filter.getHandleRes();
        if (file_idx == 0)
        {
            // TODO: this check may not be correct when support multiple files in a stable, let's just keep it now for simplicity
            if (handle_filter_result.empty())
                ret.first_pack_intersection = RSResult::None;
            else
                ret.first_pack_intersection = handle_filter_result.front();
        }
        if (file_idx == stable->files.size() - 1)
        {
            // TODO: this check may not be correct when support multiple files in a stable, let's just keep it now for simplicity
            if (handle_filter_result.empty())
                ret.last_pack_intersection = RSResult::None;
            else
                ret.last_pack_intersection = handle_filter_result.back();
        }

        const auto & pack_stats = file->getPackStats();
        for (size_t pack_idx = 0; pack_idx < pack_stats.size(); ++pack_idx)
        {
            // Only count packs that are fully contained by the range.
            if (handle_filter_result[pack_idx] == RSResult::All)
            {
                ret.rows += pack_stats[pack_idx].rows;
                ret.bytes += pack_stats[pack_idx].bytes;
            }
        }
    }

    return ret;
}

static size_t defaultValueBytes(const Field & f)
{
    switch (f.getType())
    {
    case Field::Types::Decimal32:
        return 4;
    case Field::Types::UInt64:
    case Field::Types::Int64:
    case Field::Types::Float64:
    case Field::Types::Decimal64:
        return 8;
    case Field::Types::UInt128:
    case Field::Types::Int128:
    case Field::Types::Decimal128:
        return 16;
    case Field::Types::Int256:
    case Field::Types::Decimal256:
        return 32;
    case Field::Types::String:
        return f.get<String>().size();
    default: // Null, Array, Tuple. In fact, it should not be Array or Tuple here.
        // But we don't throw exceptions here because it is not the critical path.
        return 1;
    }
}

size_t StableValueSpace::avgRowBytes(const ColumnDefines & read_columns)
{
    size_t avg_bytes = 0;
    if (likely(!files.empty()))
    {
        const auto & file = files.front();
        for (const auto & col : read_columns)
        {
            if (file->isColumnExist(col.id))
            {
                const auto & stat = file->getColumnStat(col.id);
                avg_bytes += stat.avg_size;
            }
            else
            {
                avg_bytes += defaultValueBytes(col.default_value);
            }
        }
    }
    return avg_bytes;
}

} // namespace DM
} // namespace DB
