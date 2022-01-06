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
using VersionedPageId = std::pair<PageId, PageVersionType>;
using VersionedPageIdAndEntry = std::tuple<PageId, PageVersionType, PageEntryV3>;
using VersionedPageIdAndEntryList = std::vector<std::tuple<PageId, PageVersionType, PageEntryV3>>;
using VersionedPageIdAndEntries = std::vector<std::pair<PageId, VersionedEntries>>;

class BlobStore : public Allocator<false>
{
public:
    struct Config
    {
        SettingUInt64 file_limit_size = BLOBFILE_LIMIT_SIZE;
        SettingUInt64 spacemap_type = SpaceMap::SpaceMapType::SMAP64_STD_MAP;
        SettingUInt64 cached_fd_size = BLOBSTORE_CACHED_FD_SIZE;
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
            SpaceMapPtr smap;
            BlobStatType type;
            BlobFileId id;

            /**
            * If no any data inside. It shoule be same as space map `biggest_cap`
            */
            UInt64 sm_max_caps = 0;

            UInt64 sm_total_size = 0;
            UInt64 sm_valid_size = 0;
            double sm_valid_rate = 1.0;

            std::mutex sm_lock;

            void changeToReadOnly()
            {
                type = BlobStatType::READ_ONLY;
            }

            BlobFileOffset getPosFromStat(size_t buf_size);

            void removePosFromStat(BlobFileOffset offset, size_t buf_size);
        };

        using BlobStatPtr = std::shared_ptr<BlobStat>;

    public:
        BlobStats(Poco::Logger * log_, BlobStore::Config config);

        std::lock_guard<std::mutex> lock();

        std::lock_guard<std::mutex> statLock(BlobStatPtr stat);

        BlobStatPtr createStat(BlobFileId blob_file_id);

        void eraseStat(BlobFileId blob_file_id);

        void changeToReadOnly(BlobFileId blob_file_id);

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
        std::pair<BlobStatPtr, BlobFileId> chooseStat(size_t buf_size, UInt64 file_limit_size);

        std::pair<BlobStatPtr, BlobFileId> chooseNewStat();

        BlobStatPtr fileIdToStat(BlobFileId file_id);

        std::list<BlobStatPtr> getStats() const
        {
            return stats_map;
        }

#ifndef DBMS_PUBLIC_GTEST
    private:
#endif
        Poco::Logger * log;
        BlobStore::Config config;

        BlobFileId roll_id = 0;
        std::list<BlobFileId> old_ids;
        std::list<BlobStatPtr> stats_map;
        std::mutex lock_stats;
    };

    BlobStore(const FileProviderPtr & file_provider_, String path, BlobStore::Config config);

    void restore();

    std::vector<BlobFileId> getGCStats();

    VersionedPageIdAndEntryList gc(std::map<BlobFileId, VersionedPageIdAndEntries> & entries_need_gc,
                                   const PageSize & total_page_size,
                                   const WriteLimiterPtr & write_limiter = nullptr,
                                   const ReadLimiterPtr & read_limiter = nullptr);

    PageEntriesEdit write(DB::WriteBatch & wb, const WriteLimiterPtr & write_limiter = nullptr);

    void remove(PageEntriesV3 del_entries);

    PageMap read(PageIDAndEntriesV3 & entries, const ReadLimiterPtr & read_limiter = nullptr);

    Page read(const PageIDAndEntryV3 & entry, const ReadLimiterPtr & read_limiter = nullptr);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    void read(BlobFileId blob_id, BlobFileOffset offset, char * buffers, size_t size, const ReadLimiterPtr & read_limiter = nullptr);

    std::pair<BlobFileId, BlobFileOffset> getPosFromStats(size_t size);

    void removePosFromStats(BlobFileId blob_id, BlobFileOffset offset, size_t size);

    String getBlobFilePath(BlobFileId blob_id) const;

    BlobFilePtr getBlobFile(BlobFileId blob_id);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    FileProviderPtr file_provider;
    String path{};
    Config config;

    Poco::Logger * log;

    BlobStats blob_stats;

    DB::LRUCache<BlobFileId, BlobFile> cached_file;
};
using BlobStorePtr = std::shared_ptr<BlobStore>;

} // namespace PS::V3
} // namespace DB