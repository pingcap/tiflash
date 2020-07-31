#pragma once

#include <IO/FileProvider.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageFile.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSetWithDelta.h>
#include <Storages/Page/WriteBatch.h>

#include <functional>
#include <optional>
#include <queue>
#include <set>
#include <shared_mutex>
#include <type_traits>
#include <unordered_map>

namespace DB
{

class TiFlashMetrics;
using TiFlashMetricsPtr = std::shared_ptr<TiFlashMetrics>;
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;


/**
 * A storage system stored pages. Pages are serialized objects referenced by PageId. Store Page with the same PageId
 * will covered the old ones. The file used to persist the Pages called PageFile. The meta data of a Page, like the
 * latest PageFile the Page is stored , the offset in file, and checksum, are cached in memory. Users should call
 * #gc() constantly to clean up the sparse PageFiles and release disk space.
 *
 * This class is multi-threads safe. Support single thread write, and multi threads read.
 */
class PageStorage
{
public:
    struct Config
    {
        Config() {}

        bool sync_on_write = true;

        size_t file_roll_size  = PAGE_FILE_ROLL_SIZE;
        size_t file_max_size   = PAGE_FILE_MAX_SIZE;
        size_t file_small_size = PAGE_FILE_SMALL_SIZE;

        size_t file_meta_roll_size = PAGE_META_ROLL_SIZE;

        Float64 merge_hint_low_used_rate            = 0.35;
        size_t  merge_hint_low_used_file_total_size = PAGE_FILE_ROLL_SIZE;
        size_t  merge_hint_low_used_file_num        = 10;

        size_t num_write_slots = 1;

        // Minimum number of legacy files to be selected for compaction
        size_t gc_compact_legacy_min_num = 3;

        Seconds open_file_max_idle_time{15};

        ::DB::MVCC::VersionSetConfig version_set_config;
    };

    struct ListPageFilesOption
    {
        ListPageFilesOption() {}

        bool remove_tmp_files  = false;
        bool ignore_legacy     = false;
        bool ignore_checkpoint = false;
    };

    using VersionedPageEntries = PageEntriesVersionSetWithDelta;

    using SnapshotPtr   = VersionedPageEntries::SnapshotPtr;
    using WriterPtr     = std::unique_ptr<PageFile::Writer>;
    using ReaderPtr     = std::shared_ptr<PageFile::Reader>;
    using OpenReadFiles = std::map<PageFileIdAndLevel, ReaderPtr>;

    using MetaMergingQueue = std::
        priority_queue<PageFile::MetaMergingReaderPtr, std::vector<PageFile::MetaMergingReaderPtr>, PageFile::MergingPtrComparator<false>>;

    using PathAndIdsVec        = std::vector<std::pair<String, std::set<PageId>>>;
    using ExternalPagesScanner = std::function<PathAndIdsVec()>;
    using ExternalPagesRemover
        = std::function<void(const PathAndIdsVec & pengding_external_pages, const std::set<PageId> & valid_normal_pages)>;

    // Debugging info for restore
    struct StatisticsInfo
    {
        size_t puts    = 0;
        size_t refs    = 0;
        size_t deletes = 0;
        size_t upserts = 0;
        bool   empty() const { return puts == 0 && refs == 0 && deletes == 0 && upserts == 0; }
        String toString() const;
        void   mergeEdits(const PageEntriesEdit & edit);
    };

public:
    PageStorage(String                  name,
                const String &          storage_path,
                const Config &          config_,
                const FileProviderPtr & file_provider_,
                TiFlashMetricsPtr       metrics_         = nullptr,
                PathCapacityMetricsPtr  global_capacity_ = nullptr);

    void restore();

    PageId getMaxId();

    void write(WriteBatch && write_batch);

    SnapshotPtr getSnapshot();
    size_t      getNumSnapshots() const;

    PageEntry getEntry(PageId page_id, SnapshotPtr snapshot = {});
    Page      read(PageId page_id, SnapshotPtr snapshot = {});
    PageMap   read(const std::vector<PageId> & page_ids, SnapshotPtr snapshot = {});
    void      read(const std::vector<PageId> & page_ids, const PageHandler & handler, SnapshotPtr snapshot = {});

    using FieldIndices   = std::vector<size_t>;
    using PageReadFields = std::pair<PageId, FieldIndices>;
    PageMap read(const std::vector<PageReadFields> & page_fields, SnapshotPtr snapshot = {});

    void traverse(const std::function<void(const Page & page)> & acceptor, SnapshotPtr snapshot = {});
    void traversePageEntries(const std::function<void(PageId page_id, const PageEntry & page)> & acceptor, SnapshotPtr snapshot);

    void drop();

    bool gc();

    PageId getNormalPageId(PageId page_id, SnapshotPtr snapshot = {});

    // Register two callback:
    // `scanner` for scanning avaliable external page ids.
    // `remover` will be called with living normal page ids after gc run a round.
    void registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover);

    FileProviderPtr getFileProvider() const { return file_provider; }

    static PageFileSet listAllPageFiles(const String &              storage_path,
                                        const FileProviderPtr &     file_provider,
                                        Poco::Logger *              page_file_log,
                                        const ListPageFilesOption & option = ListPageFilesOption());

private:
    WriterPtr getWriter(PageFile & page_file);
    ReaderPtr getReader(const PageFileIdAndLevel & file_id_level);

    static constexpr const char * ARCHIVE_SUBDIR = "archive";

    void archivePageFiles(const PageFileSet & page_files_to_archive);

    std::tuple<size_t, size_t> //
    gcRemoveObsoleteData(PageFileSet &                        page_files,
                         const PageFileIdAndLevel &           writing_file_id_level,
                         const std::set<PageFileIdAndLevel> & live_files);

    friend class LegacyCompactor;

    template <typename SnapshotPtr>
    friend class DataCompactor;

private:
    String storage_name; // Identify between different Storage
    String storage_path;
    Config config;

    FileProviderPtr file_provider;

    std::mutex              write_mutex;
    std::condition_variable write_mutex_cv;
    std::vector<PageFile>   write_files;
    std::deque<WriterPtr>   idle_writers;

    // A sequence number to keep ordering between multi-writers.
    std::atomic<WriteBatch::SequenceID> write_batch_seq = 0;

    OpenReadFiles open_read_files;
    std::mutex    open_read_files_mutex; // A mutex only used to protect open_read_files.

    Poco::Logger * page_file_log;
    Poco::Logger * log;

    VersionedPageEntries versioned_page_entries;

    std::atomic<bool> gc_is_running = false;

    ExternalPagesScanner external_pages_scanner = nullptr;
    ExternalPagesRemover external_pages_remover = nullptr;

    StatisticsInfo statistics;

    // For reporting metrics to prometheus
    TiFlashMetricsPtr metrics;

    const PathCapacityMetricsPtr global_capacity;
};

class PageReader
{
public:
    /// Not snapshot read.
    explicit PageReader(PageStorage & storage_) : storage(storage_), snap() {}
    /// Snapshot read.
    PageReader(PageStorage & storage_, const PageStorage::SnapshotPtr & snap_) : storage(storage_), snap(snap_) {}

    Page    read(PageId page_id) const { return storage.read(page_id, snap); }
    PageMap read(const std::vector<PageId> & page_ids) const { return storage.read(page_ids, snap); }
    void    read(const std::vector<PageId> & page_ids, PageHandler & handler) const { storage.read(page_ids, handler, snap); }

    using PageReadFields = PageStorage::PageReadFields;
    PageMap read(const std::vector<PageReadFields> & page_fields) const { return storage.read(page_fields, snap); }

    PageId    getNormalPageId(PageId page_id) const { return storage.getNormalPageId(page_id, snap); }
    UInt64    getPageChecksum(PageId page_id) const { return storage.getEntry(page_id, snap).checksum; }
    PageEntry getPageEntry(PageId page_id) const { return storage.getEntry(page_id, snap); }

private:
    PageStorage &            storage;
    PageStorage::SnapshotPtr snap;
};

} // namespace DB
