#pragma once

#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>
#include <Storages/Page/PageStorage.h>


namespace DB
{
namespace DM
{
struct WriteBatches;
struct DMContext;

class StableValueSpace;
using StableValueSpacePtr = std::shared_ptr<StableValueSpace>;

static const String STABLE_FOLDER_NAME = "stable";

class StableValueSpace : public std::enable_shared_from_this<StableValueSpace>
{
public:
    StableValueSpace(PageId id_, bool is_common_handle_, size_t rowkey_column_size_)
        : id(id_), is_common_handle(is_common_handle_), rowkey_column_size(rowkey_column_size_), log(&Logger::get("StableValueSpace"))
    {
    }

    // Set DMFiles for this value space.
    // If this value space is logical split, specify `range` and `dm_context` so that we can get more precise
    // bytes and rows.
    void setFiles(const DMFiles & files_, const RowKeyRange & range, DMContext * dm_context = nullptr);

    PageId          getId() { return id; }
    void            saveMeta(WriteBatch & meta_wb);
    const DMFiles & getDMFiles() { return files; }
    String          getDMFilesString();

    size_t getRows() const;
    size_t getBytes() const;
    size_t getBytesOnDisk() const;
    size_t getPacks() const;

    void enableDMFilesGC();

    static StableValueSpacePtr restore(DMContext & context, PageId id);

    void recordRemovePacksPages(WriteBatches & wbs) const;

    struct Snapshot;
    using SnapshotPtr = std::shared_ptr<Snapshot>;

    struct Snapshot : public std::enable_shared_from_this<Snapshot>, private boost::noncopyable
    {
        StableValueSpacePtr stable;

        PageId id;
        UInt64 valid_rows;
        UInt64 valid_bytes;

        bool   is_common_handle;
        size_t rowkey_column_size;

        /// TODO: The members below are not actually snapshots, they should not be here.

        ColumnCachePtrs column_caches;

        Snapshot() : log(&Logger::get("StableValueSpace::Snapshot")) {}

        SnapshotPtr clone()
        {
            auto c                = std::make_shared<Snapshot>();
            c->stable             = stable;
            c->id                 = id;
            c->valid_rows         = valid_rows;
            c->valid_bytes        = valid_bytes;
            c->is_common_handle   = is_common_handle;
            c->rowkey_column_size = rowkey_column_size;

            for (size_t i = 0; i < column_caches.size(); i++)
            {
                auto column_cache = std::make_shared<ColumnCache>();
                c->column_caches.emplace_back(column_cache);
            }
            return c;
        }

        PageId getId() { return id; }

        size_t getRows() { return valid_rows; }
        size_t getBytes() { return valid_bytes; }

        const DMFiles & getDMFiles() { return stable->getDMFiles(); }

        size_t getPacks()
        {
            size_t packs = 0;
            for (auto & file : getDMFiles())
                packs += file->getPacks();
            return packs;
        }

        ColumnCachePtrs & getColumnCaches() { return column_caches; }

        SkippableBlockInputStreamPtr getInputStream(const DMContext &     context, //
                                                    const ColumnDefines & read_columns,
                                                    const RowKeyRange &   rowkey_range,
                                                    const RSOperatorPtr & filter,
                                                    UInt64                max_data_version,
                                                    size_t                expected_block_size,
                                                    bool                  enable_clean_read);

        RowsAndBytes getApproxRowsAndBytes(const DMContext & context, const RowKeyRange & range);

    private:
        Logger * log;
    };

    SnapshotPtr createSnapshot();

    void drop(const FileProviderPtr & file_provider);

private:
    static const Int64 CURRENT_VERSION;

    const PageId id;

    // Valid rows is not always the sum of rows in file,
    // because after logical split, two segments could reference to a same file.
    UInt64  valid_rows;
    UInt64  valid_bytes;
    DMFiles files;
    bool    is_common_handle;
    size_t  rowkey_column_size;

    Logger * log;
};

using StableSnapshot    = StableValueSpace::Snapshot;
using StableSnapshotPtr = StableValueSpace::SnapshotPtr;

} // namespace DM
} // namespace DB
