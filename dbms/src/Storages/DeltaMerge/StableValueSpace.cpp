// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/Filter/FilterHelper.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/StableValueSpace.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/PathPool.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DM
{
void StableValueSpace::setFiles(const DMFiles & files_, const RowKeyRange & range, DMContext * dm_context)
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
    else
    {
        auto index_cache = dm_context->db_context.getGlobalContext().getMinMaxIndexCache();
        for (const auto & file : files_)
        {
            auto pack_filter = DMFilePackFilter::loadFrom(
                file,
                index_cache,
                /*set_cache_if_miss*/ true,
                {range},
                EMPTY_FILTER,
                {},
                dm_context->db_context.getFileProvider(),
                dm_context->getReadLimiter(),
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

void StableValueSpace::saveMeta(WriteBatch & meta_wb)
{
    MemoryWriteBuffer buf(0, 8192);
    writeIntBinary(STORAGE_FORMAT_CURRENT.stable, buf);
    writeIntBinary(valid_rows, buf);
    writeIntBinary(valid_bytes, buf);
    writeIntBinary(static_cast<UInt64>(files.size()), buf);
    for (auto & f : files)
        writeIntBinary(f->pageId(), buf);

    auto data_size = buf.count(); // Must be called before tryGetReadBuffer.
    meta_wb.putPage(id, 0, buf.tryGetReadBuffer(), data_size);
}

StableValueSpacePtr StableValueSpace::restore(DMContext & context, PageId id)
{
    auto stable = std::make_shared<StableValueSpace>(id);

    Page page = context.storage_pool.metaReader()->read(id); // not limit restore
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    UInt64 version, valid_rows, valid_bytes, size;
    readIntBinary(version, buf);
    if (version != StableFormat::V1)
        throw Exception("Unexpected version: " + DB::toString(version));

    readIntBinary(valid_rows, buf);
    readIntBinary(valid_bytes, buf);
    readIntBinary(size, buf);
    UInt64 page_id;
    for (size_t i = 0; i < size; ++i)
    {
        readIntBinary(page_id, buf);

        auto file_id = context.storage_pool.dataReader()->getNormalPageId(page_id);
        auto file_parent_path = context.path_pool.getStableDiskDelegator().getDTFilePath(file_id);

        auto dmfile = DMFile::restore(context.db_context.getFileProvider(), file_id, page_id, file_parent_path, DMFile::ReadMetaMode::all());
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

size_t StableValueSpace::getBytesOnDisk() const
{
    // If this stable value space is logical splitted, some file may not used,
    // and this will return more bytes than actual used.
    size_t bytes = 0;
    for (const auto & file : files)
        bytes += file->getBytesOnDisk();
    return bytes;
}

size_t StableValueSpace::getPacks() const
{
    size_t packs = 0;
    for (const auto & file : files)
        packs += file->getPacks();
    return packs;
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

void StableValueSpace::enableDMFilesGC()
{
    for (auto & file : files)
        file->enableGC();
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

void StableValueSpace::calculateStableProperty(const DMContext & context, const RowKeyRange & rowkey_range, bool is_common_handle)
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
            LOG_FMT_DEBUG(log, "Try to calculate StableProperty from column data for stable {}", id);
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
            BlockInputStreamPtr data_stream = builder
                                                  .setRowsThreshold(std::numeric_limits<UInt64>::max()) // because we just read one pack at a time
                                                  .onlyReadOnePackEveryTime()
                                                  .setTracingID(fmt::format("{}-calculateStableProperty", context.tracing_id))
                                                  .build(file, read_columns, RowKeyRanges{rowkey_range});
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
            EMPTY_FILTER,
            {},
            context.db_context.getFileProvider(),
            context.getReadLimiter(),
            context.tracing_id);
        const auto & use_packs = pack_filter.getUsePacks();
        size_t new_pack_properties_index = 0;
        bool use_new_pack_properties = pack_properties.property_size() == 0;
        if (use_new_pack_properties)
        {
            size_t use_packs_count = 0;
            for (auto is_used : use_packs)
            {
                if (is_used)
                    use_packs_count += 1;
            }
            if (unlikely((size_t)new_pack_properties.property_size() != use_packs_count))
            {
                throw Exception(
                    fmt::format("size doesn't match [new_pack_properties_size={}] [use_packs_size={}]", new_pack_properties.property_size(), use_packs_count),
                    ErrorCodes::LOGICAL_ERROR);
            }
        }
        for (size_t pack_id = 0; pack_id < use_packs.size(); pack_id++)
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
    auto snap = std::make_shared<Snapshot>();
    snap->id = id;
    snap->valid_rows = valid_rows;
    snap->valid_bytes = valid_bytes;
    snap->stable = this->shared_from_this();

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

SkippableBlockInputStreamPtr
StableValueSpace::Snapshot::getInputStream(
    const DMContext & context,
    const ColumnDefines & read_columns,
    const RowKeyRanges & rowkey_ranges,
    const RSOperatorPtr & filter,
    UInt64 max_data_version,
    size_t expected_block_size,
    bool enable_handle_clean_read,
    bool is_fast_scan,
    bool enable_del_clean_read)
{
    LOG_FMT_DEBUG(log, "max_data_version: {}, enable_handle_clean_read: {}, is_fast_mode: {}, enable_del_clean_read: {}", max_data_version, enable_handle_clean_read, is_fast_scan, enable_del_clean_read);
    SkippableBlockInputStreams streams;

    for (size_t i = 0; i < stable->files.size(); i++)
    {
        DMFileBlockInputStreamBuilder builder(context.db_context);
        builder
            .enableCleanRead(enable_handle_clean_read, is_fast_scan, enable_del_clean_read, max_data_version)
            .setRSOperator(filter)
            .setColumnCache(column_caches[i])
            .setTracingID(context.tracing_id)
            .setRowsThreshold(expected_block_size);
        streams.push_back(builder.build(stable->files[i], read_columns, rowkey_ranges));
    }
    return std::make_shared<ConcatSkippableBlockInputStream>(streams);
}

RowsAndBytes StableValueSpace::Snapshot::getApproxRowsAndBytes(const DMContext & context, const RowKeyRange & range) const
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
            context.tracing_id);
        const auto & pack_stats = f->getPackStats();
        const auto & use_packs = filter.getUsePacks();
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

std::pair<bool, bool> StableValueSpace::Snapshot::isFirstAndLastPackIncludedInRange(const DMContext & context, const RowKeyRange & range) const
{
    // Usually, this method will be called for some "cold" key ranges.
    // Loading the index into cache may pollute the cache and make the hot index cache invalid.
    // So don't refill the cache if the index does not exist.
    bool first_pack_included = false;
    bool last_pack_included = false;
    for (size_t i = 0; i < stable->files.size(); i++)
    {
        const auto & file = stable->files[i];
        auto filter = DMFilePackFilter::loadFrom(
            file,
            context.db_context.getGlobalContext().getMinMaxIndexCache(),
            /*set_cache_if_miss*/ false,
            {range},
            RSOperatorPtr{},
            IdSetPtr{},
            context.db_context.getFileProvider(),
            context.getReadLimiter(),
            context.tracing_id);
        const auto & use_packs = filter.getUsePacks();
        if (i == 0)
        {
            // TODO: this check may not be correct when support multiple files in a stable, let's just keep it now for simplicity
            first_pack_included = use_packs.empty() || use_packs[0];
        }
        if (i == stable->files.size() - 1)
        {
            // TODO: this check may not be correct when support multiple files in a stable, let's just keep it now for simplicity
            last_pack_included = use_packs.empty() || use_packs.back();
        }
    }

    return std::make_pair(first_pack_included, last_pack_included);
}

} // namespace DM
} // namespace DB
