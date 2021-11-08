#pragma once

#include <Core/Types.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/V2/VersionSet/PageEntriesVersionSetWithDelta.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/Page/mvcc/VersionSetWithDelta.h>

#include <condition_variable>
#include <functional>
#include <optional>
#include <queue>
#include <set>
#include <shared_mutex>
#include <type_traits>
#include <unordered_map>

namespace DB
{
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
class PSDiskDelegator;
using PSDiskDelegatorPtr = std::shared_ptr<PSDiskDelegator>;
class Context;
class PageStorage;
using PageStoragePtr = std::shared_ptr<PageStorage>;

class PageStorage : private boost::noncopyable
{
public:
    using SnapshotPtr = DB::PS::V2::PageEntriesVersionSetWithDelta::SnapshotPtr;

    struct Config
    {
        SettingBool sync_on_write = true;

        SettingUInt64 file_roll_size = PAGE_FILE_ROLL_SIZE;
        SettingUInt64 file_max_size = PAGE_FILE_MAX_SIZE;
        SettingUInt64 file_small_size = PAGE_FILE_SMALL_SIZE;

        SettingUInt64 file_meta_roll_size = PAGE_META_ROLL_SIZE;

        // When the value of gc_force_hardlink_rate is less than or equal to 1,
        // It means that candidates whose valid rate is greater than this value will be forced to hardlink(This will reduce the gc duration).
        // Otherwise, if gc_force_hardlink_rate is greater than 1, hardlink won't happen
        SettingDouble gc_force_hardlink_rate = 0.8;

        SettingDouble gc_max_valid_rate = 0.35;
        SettingUInt64 gc_min_bytes = PAGE_FILE_ROLL_SIZE;
        SettingUInt64 gc_min_files = 10;
        // Minimum number of legacy files to be selected for compaction
        SettingUInt64 gc_min_legacy_num = 3;

        SettingUInt64 gc_max_expect_legacy_files = 100;
        SettingDouble gc_max_valid_rate_bound = 1.0;

        // Maximum write concurrency. Must not be changed once the PageStorage object is created.
        SettingUInt64 num_write_slots = 1;

        // Maximum seconds of reader / writer idle time.
        // 0 for never reclaim idle file descriptor.
        SettingUInt64 open_file_max_idle_time = 15;

        // Probability to do gc when write is low.
        // The probability is `prob_do_gc_when_write_is_low` out of 1000.
        SettingUInt64 prob_do_gc_when_write_is_low = 10;

        MVCC::VersionSetConfig version_set_config;

        void reload(const Config & rhs)
        {
            // Reload is not atomic, but should be good enough

            // Reload gc threshold
            gc_force_hardlink_rate = rhs.gc_force_hardlink_rate;
            gc_max_valid_rate = rhs.gc_max_valid_rate;
            gc_min_bytes = rhs.gc_min_bytes;
            gc_min_files = rhs.gc_min_files;
            gc_min_legacy_num = rhs.gc_min_legacy_num;
            prob_do_gc_when_write_is_low = rhs.prob_do_gc_when_write_is_low;
            // Reload fd idle time
            open_file_max_idle_time = rhs.open_file_max_idle_time;
        }

        String toDebugString() const
        {
            std::stringstream ss;
            ss << "PageStorage::Config {gc_min_files:" << gc_min_files << ", gc_min_bytes:" << gc_min_bytes
               << ", gc_force_hardlink_rate:" << DB::toString(gc_force_hardlink_rate.get(), 3)
               << ", gc_max_valid_rate:" << DB::toString(gc_max_valid_rate.get(), 3)
               << ", gc_min_legacy_num:" << gc_min_legacy_num
               << ", gc_max_expect_legacy: " << DB::toString(gc_max_expect_legacy_files.get())
               << ", gc_max_valid_rate_bound: " << DB::toString(gc_max_valid_rate_bound.get(), 3)
               << ", prob_do_gc_when_write_is_low:" << prob_do_gc_when_write_is_low
               << ", open_file_max_idle_time:" << open_file_max_idle_time << "}";
            return ss.str();
        }
    };
    void reloadSettings(const Config & new_config) { config.reload(new_config); };


    using PathAndIdsVec = std::vector<std::pair<String, std::set<PageId>>>;
    using ExternalPagesScanner = std::function<PathAndIdsVec()>;
    using ExternalPagesRemover
        = std::function<void(const PathAndIdsVec & pengding_external_pages, const std::set<PageId> & valid_normal_pages)>;

public:
    static PageStoragePtr
    create(
        String name,
        PSDiskDelegatorPtr delegator,
        const PageStorage::Config & config,
        const FileProviderPtr & file_provider);

    PageStorage(
        String name,
        PSDiskDelegatorPtr delegator_,
        const Config & config_,
        const FileProviderPtr & file_provider_)
        : storage_name(std::move(name))
        , delegator(std::move(delegator_))
        , config(config_)
        , file_provider(file_provider_)
    {
    }

    virtual ~PageStorage() = default;

    virtual void restore() = 0;

    virtual void drop() = 0;

    virtual PageId getMaxId() = 0;

    virtual SnapshotPtr getSnapshot() = 0;

    // Get some statistics of all living snapshots and the oldest living snapshot.
    // Return < num of snapshots,
    //          living time(seconds) of the oldest snapshot,
    //          created thread id of the oldest snapshot      >
    virtual std::tuple<size_t, double, unsigned> getSnapshotsStat() const = 0;

    virtual void write(WriteBatch && write_batch, const WriteLimiterPtr & write_limiter = nullptr) = 0;

    virtual PageEntry getEntry(PageId page_id, SnapshotPtr snapshot = {}) = 0;

    virtual Page read(PageId page_id, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}) = 0;

    virtual PageMap read(const std::vector<PageId> & page_ids, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}) = 0;

    virtual void read(const std::vector<PageId> & page_ids, const PageHandler & handler, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}) = 0;

    using FieldIndices = std::vector<size_t>;
    using PageReadFields = std::pair<PageId, FieldIndices>;
    virtual PageMap read(const std::vector<PageReadFields> & page_fields, const ReadLimiterPtr & read_limiter = nullptr, SnapshotPtr snapshot = {}) = 0;

    virtual void traverse(const std::function<void(const DB::Page & page)> & acceptor, SnapshotPtr snapshot = {}) = 0;

    virtual void traversePageEntries(const std::function<void(PageId page_id, const PageEntry & page)> & acceptor, SnapshotPtr snapshot) = 0;

    virtual PageId getNormalPageId(PageId page_id, SnapshotPtr snapshot = {}) = 0;

    // We may skip the GC to reduce useless reading by default.
    virtual bool gc(bool not_skip = false, const WriteLimiterPtr & write_limiter = nullptr, const ReadLimiterPtr & read_limiter = nullptr) = 0;

    // Register two callback:
    // `scanner` for scanning avaliable external page ids.
    // `remover` will be called with living normal page ids after gc run a round.
    virtual void registerExternalPagesCallbacks(ExternalPagesScanner scanner, ExternalPagesRemover remover) = 0;

#ifndef DBMS_PUBLIC_GTEST
protected:
#endif
    String storage_name; // Identify between different Storage
    PSDiskDelegatorPtr delegator; // Get paths for storing data
    Config config;
    FileProviderPtr file_provider;
};


class PageReader : private boost::noncopyable
{
public:
    /// Not snapshot read.
    explicit PageReader(PageStoragePtr storage_, ReadLimiterPtr read_limiter_)
        : storage(storage_)
        , snap()
        , read_limiter(read_limiter_)
    {}
    /// Snapshot read.
    PageReader(PageStoragePtr storage_, const PageStorage::SnapshotPtr & snap_, ReadLimiterPtr read_limiter_)
        : storage(storage_)
        , snap(snap_)
        , read_limiter(read_limiter_)
    {}
    PageReader(PageStoragePtr storage_, PageStorage::SnapshotPtr && snap_, ReadLimiterPtr read_limiter_)
        : storage(storage_)
        , snap(std::move(snap_))
        , read_limiter(read_limiter_)
    {}

    DB::Page read(PageId page_id) const
    {
        return storage->read(page_id, read_limiter, snap);
    }

    PageMap read(const std::vector<PageId> & page_ids) const
    {
        return storage->read(page_ids, read_limiter, snap);
    }

    void read(const std::vector<PageId> & page_ids, PageHandler & handler) const
    {
        storage->read(page_ids, handler, read_limiter, snap);
    }

    using PageReadFields = PageStorage::PageReadFields;
    PageMap read(const std::vector<PageReadFields> & page_fields) const
    {
        return storage->read(page_fields, read_limiter, snap);
    }

    PageId getNormalPageId(PageId page_id) const
    {
        return storage->getNormalPageId(page_id, snap);
    }

    UInt64 getPageChecksum(PageId page_id) const
    {
        return storage->getEntry(page_id, snap).checksum;
    }

    PageEntry getPageEntry(PageId page_id) const
    {
        return storage->getEntry(page_id, snap);
    }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    PageStoragePtr storage;
    PageStorage::SnapshotPtr snap;
    ReadLimiterPtr read_limiter;
};


} // namespace DB
