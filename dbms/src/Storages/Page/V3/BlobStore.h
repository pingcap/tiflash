#pragma once

#include <Common/Exception.h>
#include <Common/LRUCache.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/Page/V3/BlobFile.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>

#include <mutex>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace PS::V3
{
using PageIdAndVersionedEntries = std::vector<std::tuple<PageIdV3Internal, PageVersionType, PageEntryV3>>;

class BlobStore : private Allocator<false>
{
public:
    struct Config
    {
        SettingUInt64 file_limit_size = BLOBFILE_LIMIT_SIZE;
        SettingUInt64 spacemap_type = SpaceMap::SpaceMapType::SMAP64_STD_MAP;
        SettingUInt64 cached_fd_size = BLOBSTORE_CACHED_FD_SIZE;
        SettingDouble heavy_gc_valid_rate = 0.2;
    };

    class BlobStats
    {
    public:
        enum BlobStatType
        {
            NORMAL = 1,

            // Read Only.
            // Only after heavy GC, BlobFile will change to READ_ONLY type.
            // After GC remove, empty files will be removed.
            READ_ONLY = 2
        };

        static String blobTypeToString(BlobStatType type)
        {
            switch (type)
            {
            case BlobStatType::NORMAL:
                return "normal";
            case BlobStatType::READ_ONLY:
                return "read only";
            }
            return "Invalid";
        }

        struct BlobStat
        {
            const SpaceMapPtr smap;
            const BlobFileId id;
            BlobStatType type;

            /**
            * If no any data inside. It shoule be same as space map `biggest_cap`
            */
            UInt64 sm_max_caps = 0;

            UInt64 sm_total_size = 0;
            UInt64 sm_valid_size = 0;
            double sm_valid_rate = 1.0;

            std::mutex sm_lock;

            BlobStat(BlobFileId id_, SpaceMapPtr && smap_)
                : smap(std::move(smap_))
                , id(id_)
                , type(BlobStatType::NORMAL)
            {}

            [[nodiscard]] std::lock_guard<std::mutex> lock()
            {
                return std::lock_guard(sm_lock);
            }

            bool isReadOnly() const
            {
                return type == BlobStatType::READ_ONLY;
            }

            void changeToReadOnly()
            {
                type = BlobStatType::READ_ONLY;
            }

            BlobFileOffset getPosFromStat(size_t buf_size);

            void removePosFromStat(BlobFileOffset offset, size_t buf_size);

            /**
             * This method is only used when blobstore restore
             * Restore space map won't change the `sm_total_size`/`sm_valid_size`/`sm_valid_rate`
             */
            void restoreSpaceMap(BlobFileOffset offset, size_t buf_size);

            /**
             * After we restore the space map.
             * We still need to recalculate a `sm_total_size`/`sm_valid_size`/`sm_valid_rate`.
             */
            void recalculateSpaceMap();
        };

        using BlobStatPtr = std::shared_ptr<BlobStat>;

    public:
        BlobStats(LogWithPrefixPtr log_, BlobStore::Config config);

        [[nodiscard]] std::lock_guard<std::mutex> lock() const;

        BlobStatPtr createStatNotCheckingRoll(BlobFileId blob_file_id, const std::lock_guard<std::mutex> &);

        BlobStatPtr createStat(BlobFileId blob_file_id, const std::lock_guard<std::mutex> &);

        void eraseStat(const BlobStatPtr && stat, const std::lock_guard<std::mutex> &);

        void eraseStat(BlobFileId blob_file_id, const std::lock_guard<std::mutex> &);

        /**
         * Choose a available `BlobStat` from `BlobStats`.
         * 
         * If we can't find any usable span to fit `buf_size` in the existed stats.
         * Then it will return null `BlobStat` with a available `BlobFileId`. 
         * eq. {nullptr,`BlobFileId`}.
         * The `BlobFileId` can use to create a new `BlobFile`.
         *  
         * If we do find a usable span to fit `buf_size`.
         * Then it will return a available `BlobStatPtr` with a `INVALID_BLOBFILE_ID`.
         * eq. {`BlobStatPtr`,INVALID_BLOBFILE_ID}.
         * The `INVALID_BLOBFILE_ID` means that you don't need create a new `BlobFile`.
         * 
         */
        std::pair<BlobStatPtr, BlobFileId> chooseStat(size_t buf_size, UInt64 file_limit_size, const std::lock_guard<std::mutex> &);

        BlobStatPtr blobIdToStat(BlobFileId file_id, bool restore_if_not_exist = false);

        std::list<BlobStatPtr> getStats() const
        {
            auto guard = lock();
            return stats_map;
        }

#ifndef DBMS_PUBLIC_GTEST
    private:
#endif
        void restoreByEntry(const PageEntryV3 & entry);
        void restore();
        friend class PageDirectoryFactory;

#ifndef DBMS_PUBLIC_GTEST
    private:
#endif
        LogWithPrefixPtr log;
        BlobStore::Config config;

        BlobFileId roll_id = 1;
        std::list<BlobStatPtr> stats_map;
        mutable std::mutex lock_stats;
    };

    BlobStore(const FileProviderPtr & file_provider_, String path, BlobStore::Config config);

    std::vector<BlobFileId> getGCStats();

    PageEntriesEdit gc(std::map<BlobFileId, PageIdAndVersionedEntries> & entries_need_gc,
                       const PageSize & total_page_size,
                       const WriteLimiterPtr & write_limiter = nullptr,
                       const ReadLimiterPtr & read_limiter = nullptr);

    PageEntriesEdit write(DB::WriteBatch & wb, const WriteLimiterPtr & write_limiter = nullptr);

    void remove(const PageEntriesV3 & del_entries);

    PageMap read(PageIDAndEntriesV3 & entries, const ReadLimiterPtr & read_limiter = nullptr);

    Page read(const PageIDAndEntryV3 & entry, const ReadLimiterPtr & read_limiter = nullptr);

    void read(PageIDAndEntriesV3 & entries, const PageHandler & handler, const ReadLimiterPtr & read_limiter = nullptr);

    struct FieldReadInfo
    {
        PageIdV3Internal page_id;
        PageEntryV3 entry;
        std::vector<size_t> fields;

        FieldReadInfo(PageIdV3Internal id_, PageEntryV3 entry_, std::vector<size_t> fields_)
            : page_id(id_)
            , entry(entry_)
            , fields(std::move(fields_))
        {}
    };
    using FieldReadInfos = std::vector<FieldReadInfo>;
    PageMap read(FieldReadInfos & to_read, const ReadLimiterPtr & read_limiter = nullptr);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    void read(BlobFileId blob_id, BlobFileOffset offset, char * buffers, size_t size, const ReadLimiterPtr & read_limiter = nullptr);

    /**
     *  Ask BlobStats to get a span from BlobStat.
     *  We will lock BlobStats until we get a BlobStat that can hold the size.
     *  Then lock the BlobStat to get the span.
     */
    std::pair<BlobFileId, BlobFileOffset> getPosFromStats(size_t size);

    /**
     *  Request a specific BlobStat to delete a certain span.
     *  We will lock the BlobStat until it have been makefree in memory.
     */
    void removePosFromStats(BlobFileId blob_id, BlobFileOffset offset, size_t size);

    String getBlobFilePath(BlobFileId blob_id) const;

    BlobFilePtr getBlobFile(BlobFileId blob_id);

    friend class PageDirectoryFactory;
#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    FileProviderPtr file_provider;
    String path{};
    Config config;

    LogWithPrefixPtr log;

    BlobStats blob_stats;

    DB::LRUCache<BlobFileId, BlobFile> cached_files;
};
using BlobStorePtr = std::shared_ptr<BlobStore>;

} // namespace PS::V3
} // namespace DB
