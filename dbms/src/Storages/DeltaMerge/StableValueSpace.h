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

#pragma once

#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFilePackFilter.h>
#include <Storages/DeltaMerge/File/DMFileReader.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>
#include <Storages/Page/PageStorage_fwd.h>

namespace DB
{
namespace DM
{

struct WriteBatches;

struct DMContext;
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;

class StableValueSpace;
using StableValueSpacePtr = std::shared_ptr<StableValueSpace>;

class StableValueSpace : public std::enable_shared_from_this<StableValueSpace>
{
public:
    StableValueSpace(PageIdU64 id_)
        : id(id_)
        , log(Logger::get())
    {}

    static StableValueSpacePtr restore(DMContext & context, PageIdU64 id);

    static StableValueSpacePtr createFromCheckpoint( //
        DMContext & context,
        UniversalPageStoragePtr temp_ps,
        PageIdU64 stable_id,
        WriteBatches & wbs);

    /**
     * Resets the logger by using the one from the segment.
     * Segment_log is not available when constructing, because usually
     * at that time the segment has not been constructed yet.
     */
    void resetLogger(const LoggerPtr & segment_log) { log = segment_log; }

    // Set DMFiles for this value space.
    // If this value space is logical split, specify `range` and `dm_context` so that we can get more precise
    // bytes and rows.
    void setFiles(const DMFiles & files_, const RowKeyRange & range, const DMContext * dm_context = nullptr);

    PageIdU64 getId() const { return id; }
    void saveMeta(WriteBatchWrapper & meta_wb);

    size_t getRows() const;
    size_t getBytes() const;

    /**
     * Return the underlying DTFiles.
     * DTFiles are not fully included in the segment range will be also included in the result.
     * Note: Out-of-range DTFiles may be produced by logical split.
     */
    const DMFiles & getDMFiles() const { return files; }

    String getDMFilesString();

    /**
     * Return the total on-disk size of the underlying DTFiles.
     * DTFiles are not fully included in the segment range will be also counted in.
     * Note: Out-of-range DTFiles may be produced by logical split.
     */
    size_t getDMFilesBytesOnDisk() const;

    /**
     * Return the total number of packs of the underlying DTFiles.
     * Packs that are not included in the segment range will be also counted in.
     * Note: Out-of-range packs may be produced by logical split.
     */
    size_t getDMFilesPacks() const;

    /**
     * Return the total number of rows of the underlying DTFiles.
     * Rows from packs that are not included in the segment range will be also counted in.
     * Note: Out-of-range rows may be produced by logical split.
     */
    size_t getDMFilesRows() const;

    /**
     * Return the total size of the data of the underlying DTFiles.
     * Rows from packs that are not included in the segment range will be also counted in.
     * Note: Out-of-range rows may be produced by logical split.
     */
    size_t getDMFilesBytes() const;

    void enableDMFilesGC(DMContext & context);

    void recordRemovePacksPages(WriteBatches & wbs) const;

    bool isStablePropertyCached() const { return is_property_cached.load(std::memory_order_acquire); }

    struct StableProperty
    {
        // when gc_safe_point exceed this version, there must be some data obsolete
        UInt64 gc_hint_version;
        // number of rows including all puts and deletes
        UInt64 num_versions;
        // number of visible rows using the latest timestamp
        UInt64 num_puts;
        // number of rows having at least one version(include delete)
        UInt64 num_rows;

        const String toDebugString() const
        {
            return "StableProperty: gc_hint_version [" + std::to_string(this->gc_hint_version) + "] num_versions ["
                + std::to_string(this->num_versions) + "] num_puts[" + std::to_string(this->num_puts) + "] num_rows["
                + std::to_string(this->num_rows) + "]";
        }
    };

    const StableProperty & getStableProperty() const { return property; }

    void calculateStableProperty(const DMContext & context, const RowKeyRange & rowkey_range, bool is_common_handle);

    struct Snapshot;
    using SnapshotPtr = std::shared_ptr<Snapshot>;

    struct Snapshot
        : public std::enable_shared_from_this<Snapshot>
        , private boost::noncopyable
    {
        StableValueSpacePtr stable;

        PageIdU64 id;
        UInt64 valid_rows;
        UInt64 valid_bytes;

        bool is_common_handle;
        size_t rowkey_column_size;

        /// TODO: The members below are not actually snapshots, they should not be here.

        ColumnCachePtrs column_caches;

        Snapshot(StableValueSpacePtr stable_)
            : stable(stable_)
            , log(stable->log)
        {}

        SnapshotPtr clone() const
        {
            auto c = std::make_shared<Snapshot>(stable);
            c->id = id;
            c->valid_rows = valid_rows;
            c->valid_bytes = valid_bytes;

            for (size_t i = 0; i < column_caches.size(); i++)
            {
                auto column_cache = std::make_shared<ColumnCache>();
                c->column_caches.emplace_back(column_cache);
            }
            return c;
        }

        PageIdU64 getId() const { return id; }

        size_t getRows() const { return valid_rows; }
        size_t getBytes() const { return valid_bytes; }

        /**
         * Return the underlying DTFiles.
         * DTFiles are not fully included in the segment range will be also included in the result.
         * Note: Out-of-range DTFiles may be produced by logical split.
         */
        const DMFiles & getDMFiles() const { return stable->getDMFiles(); }

        /**
         * Return the total number of packs of the underlying DTFiles.
         * Packs that are not included in the segment range will be also counted in.
         * Note: Out-of-range packs may be produced by logical split.
         */
        size_t getDMFilesPacks() const { return stable->getDMFilesPacks(); }

        /**
         * Return the total number of rows of the underlying DTFiles.
         * Rows from packs that are not included in the segment range will be also counted in.
         * Note: Out-of-range rows may be produced by logical split.
         */
        size_t getDMFilesRows() const { return stable->getDMFilesRows(); };

        /**
         * Return the total size of the data of the underlying DTFiles.
         * Rows from packs that are not included in the segment range will be also counted in.
         * Note: Out-of-range rows may be produced by logical split.
         */
        size_t getDMFilesBytes() const { return stable->getDMFilesBytes(); };

        ColumnCachePtrs & getColumnCaches() { return column_caches; }

        void clearColumnCaches()
        {
            for (auto & col_cache : column_caches)
            {
                col_cache->clear();
            }
        }

        SkippableBlockInputStreamPtr getInputStream(
            const DMContext & context, //
            const ColumnDefines & read_columns,
            const RowKeyRanges & rowkey_ranges,
            const RSOperatorPtr & filter,
            UInt64 max_data_version,
            size_t expected_block_size,
            bool enable_handle_clean_read,
            ReadTag read_tag,
            bool is_fast_scan = false,
            bool enable_del_clean_read = false,
            const std::vector<IdSetPtr> & read_packs = {},
            bool need_row_id = false);

        RowsAndBytes getApproxRowsAndBytes(const DMContext & context, const RowKeyRange & range) const;

        struct AtLeastRowsAndBytesResult
        {
            size_t rows = 0;
            size_t bytes = 0;
            RSResult first_pack_intersection = RSResult::None;
            RSResult last_pack_intersection = RSResult::None;
        };

        /**
         * Get the rows and bytes calculated from packs that is **fully contained** by the given range.
         * If the pack is partially intersected, then it is not counted.
         */
        AtLeastRowsAndBytesResult getAtLeastRowsAndBytes(const DMContext & context, const RowKeyRange & range) const;

    private:
        LoggerPtr log;
    };

    SnapshotPtr createSnapshot();

    void drop(const FileProviderPtr & file_provider);

    size_t avgRowBytes(const ColumnDefines & read_columns);

private:
    const PageIdU64 id;

    // Valid rows is not always the sum of rows in file,
    // because after logical split, two segments could reference to a same file.
    UInt64 valid_rows; /* At most. The actual valid rows may be lower than this value. */
    UInt64 valid_bytes; /* At most. The actual valid bytes may be lower than this value. */

    DMFiles files;

    StableProperty property;
    std::atomic<bool> is_property_cached = false;

    LoggerPtr log;
};

using StableSnapshot = StableValueSpace::Snapshot;
using StableSnapshotPtr = StableValueSpace::SnapshotPtr;

} // namespace DM
} // namespace DB
