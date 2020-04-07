#pragma once

#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/File/DMFile.h>
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
    StableValueSpace(PageId id_) : id(id_), log(&Logger::get("StableValueSpace")) {}

    void setFiles(const DMFiles & files_, DMContext * dm_context = nullptr, HandleRange range = HandleRange::newAll());

    PageId          getId() { return id; }
    void            saveMeta(WriteBatch & meta_wb);
    const DMFiles & getDMFiles() { return files; }
    String          getDMFilesString();

    size_t getRows();
    size_t getBytes();
    size_t getPacks();

    void enableDMFilesGC();

    //    SkippableBlockInputStreamPtr getInputStream(const DMContext &     context,
    //                                                const ColumnDefines & read_columns,
    //                                                const HandleRange &   handle_range,
    //                                                const RSOperatorPtr & filter,
    //                                                UInt64                max_data_version,
    //                                                bool                  enable_clean_read);

    static StableValueSpacePtr restore(DMContext & context, PageId id);

    void recordRemovePacksPages(WriteBatches & wbs) const;

    struct Snapshot : public std::enable_shared_from_this<Snapshot>, private boost::noncopyable
    {
        Snapshot() { column_cache = std::make_shared<ColumnCache>(); }
        StableValueSpacePtr stable;
        ColumnCachePtr      column_cache;

        PageId getId() { return stable->getId(); }

        const DMFiles & getDMFiles() { return stable->getDMFiles(); }

        size_t getRows() { return stable->getRows(); }
        size_t getBytes() { return stable->getBytes(); }
        size_t getPacks() { return stable->getPacks(); }

        SkippableBlockInputStreamPtr getInputStream(const DMContext &     context, //
                                                    const ColumnDefines & read_columns,
                                                    const HandleRange &   handle_range,
                                                    const RSOperatorPtr & filter,
                                                    UInt64                max_data_version,
                                                    bool                  enable_clean_read);
    };
    using SnapshotPtr = std::shared_ptr<Snapshot>;

    SnapshotPtr createSnapshot();

private:
    static const Int64 CURRENT_VERSION;

    const PageId id;

    // Valid rows is not always the sum of rows in file,
    // because after logical split, two segments could reference to a same file.
    UInt64  valid_rows;
    UInt64  valid_bytes;
    DMFiles files;

    Logger * log;
};

using StableSnapshot    = StableValueSpace::Snapshot;
using StableSnapshotPtr = StableValueSpace::SnapshotPtr;

} // namespace DM
} // namespace DB
