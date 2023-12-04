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

#include <Common/Exception.h>
#include <Common/LRUCache.h>
#include <Interpreters/SettingsCommon.h>
#include <Storages/Page/FileUsage.h>
#include <Storages/Page/V3/BlobFile.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/PathPool.h>

#include <mutex>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace PS::V3
{
using PageIdAndVersionedEntries = std::vector<std::tuple<PageIdV3Internal, PageVersion, PageEntryV3>>;

class BlobStore : private Allocator<false>
{
public:
    struct Config
    {
        SettingUInt64 file_limit_size = BLOBFILE_LIMIT_SIZE;
        SettingUInt64 spacemap_type = SpaceMap::SpaceMapType::SMAP64_STD_MAP;
        SettingUInt64 cached_fd_size = BLOBSTORE_CACHED_FD_SIZE;
        SettingUInt64 block_alignment_bytes = 0;
        SettingDouble heavy_gc_valid_rate = 0.2;

        String toString()
        {
            return fmt::format("BlobStore Config Info: "
                               "[file_limit_size={}],[spacemap_type={}],"
                               "[cached_fd_size={}],[block_alignment_bytes={}],"
                               "[heavy_gc_valid_rate={}]",
                               file_limit_size,
                               spacemap_type,
                               cached_fd_size,
                               block_alignment_bytes,
                               heavy_gc_valid_rate);
        }
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
            READ_ONLY = 2,

            // Big Blob file
            // Only used to page size > config.file_limit_size
            BIG_BLOB = 3
        };

        static String blobTypeToString(BlobStatType type)
        {
            switch (type)
            {
            case BlobStatType::NORMAL:
                return "normal";
            case BlobStatType::READ_ONLY:
                return "read only";
            case BlobStatType::BIG_BLOB:
                return "big blob";
            }
            return "Invalid";
        }

        struct BlobStat
        {
            const BlobFileId id;
            std::atomic<BlobStatType> type;

            std::mutex sm_lock;
            const SpaceMapPtr smap;
            /**
             * If no any data inside. It shoule be same as space map `biggest_cap`,
             * It is a hint for choosing quickly, should use `recalculateCapacity`
             * to update it after some space are free in the spacemap.
             */
            UInt64 sm_max_caps = 0;
            UInt64 sm_total_size = 0;
            UInt64 sm_valid_size = 0;
            double sm_valid_rate = 0.0;

        public:
            BlobStat(BlobFileId id_, SpaceMap::SpaceMapType sm_type, UInt64 sm_max_caps_)
                : id(id_)
                , type(BlobStatType::NORMAL)
                , smap(SpaceMap::createSpaceMap(sm_type, 0, sm_max_caps_))
                , sm_max_caps(sm_max_caps_)
            {
                if (sm_type == SpaceMap::SpaceMapType::SMAP64_BIG)
                {
                    type = BlobStatType::BIG_BLOB;
                }

                // Won't create read-only blob by default.
                assert(type != BlobStatType::READ_ONLY);
            }

            [[nodiscard]] std::lock_guard<std::mutex> lock()
            {
                return std::lock_guard(sm_lock);
            }

            bool isNormal() const
            {
                return type.load() == BlobStatType::NORMAL;
            }

            bool isReadOnly() const
            {
                return type.load() == BlobStatType::READ_ONLY;
            }

            void changeToReadOnly()
            {
                type.store(BlobStatType::READ_ONLY);
            }

            bool isBigBlob() const
            {
                return type.load() == BlobStatType::BIG_BLOB;
            }

            BlobFileOffset getPosFromStat(size_t buf_size, const std::lock_guard<std::mutex> &);

            bool removePosFromStat(BlobFileOffset offset, size_t buf_size, const std::lock_guard<std::mutex> &);

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

            /**
             * The `sm_max_cap` is not accurate after GC removes out-of-date data, or after restoring from disk.
             * Caller should call this function to update the `sm_max_cap` so that we can reuse the space in this BlobStat.
             */
            void recalculateCapacity();
        };

        using BlobStatPtr = std::shared_ptr<BlobStat>;

    public:
        BlobStats(LoggerPtr log_, PSDiskDelegatorPtr delegator_, BlobStore::Config & config);

        // Don't require a lock from BlobStats When you already hold a BlobStat lock
        //
        // Safe options:
        // 1. Hold a BlobStats lock, then Hold a/many BlobStat lock(s).
        // 2. Without hold a BlobStats lock, But hold a/many BlobStat lock(s).
        // 3. Hold a BlobStats lock, without hold a/many BlobStat lock(s).
        //
        // Not safe options:
        // 1. then Hold a/many BlobStat lock(s), then a BlobStats lock.
        //
        [[nodiscard]] std::lock_guard<std::mutex> lock() const;

        BlobStatPtr createStatNotChecking(BlobFileId blob_file_id, const std::lock_guard<std::mutex> &);

        BlobStatPtr createStat(BlobFileId blob_file_id, const std::lock_guard<std::mutex> & guard);

        BlobStatPtr createBigPageStatNotChecking(BlobFileId blob_file_id, const std::lock_guard<std::mutex> &);

        BlobStatPtr createBigStat(BlobFileId blob_file_id, const std::lock_guard<std::mutex> & guard);

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
        std::pair<BlobStatPtr, BlobFileId> chooseStat(size_t buf_size, const std::lock_guard<std::mutex> &);

        BlobFileId chooseBigStat(const std::lock_guard<std::mutex> &) const;

        BlobStatPtr blobIdToStat(BlobFileId file_id, bool ignore_not_exist = false);

        using StatsMap = std::map<String, std::list<BlobStatPtr>>;
        StatsMap getStats() const
        {
            auto guard = lock();
            return stats_map;
        }

        static std::pair<BlobFileId, String> getBlobIdFromName(String blob_name);

#ifndef DBMS_PUBLIC_GTEST
    private:
#endif
        void restoreByEntry(const PageEntryV3 & entry);
        void restore();
        friend class PageDirectoryFactory;

#ifndef DBMS_PUBLIC_GTEST
    private:
#endif
        LoggerPtr log;
        PSDiskDelegatorPtr delegator;
        BlobStore::Config & config;

        mutable std::mutex lock_stats;
        BlobFileId roll_id = 1;
        // Index for selecting next path for creating new blobfile
        UInt32 stats_map_path_index = 0;
        std::map<String, std::list<BlobStatPtr>> stats_map;
    };

    BlobStore(String storage_name, const FileProviderPtr & file_provider_, PSDiskDelegatorPtr delegator_, const BlobStore::Config & config);

    void registerPaths();

    void reloadConfig(const BlobStore::Config & rhs);

    FileUsageStatistics getFileUsageStatistics() const;

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

    PageEntriesEdit handleLargeWrite(DB::WriteBatch & wb, const WriteLimiterPtr & write_limiter = nullptr);

    BlobFilePtr read(const PageIdV3Internal & page_id_v3, BlobFileId blob_id, BlobFileOffset offset, char * buffers, size_t size, const ReadLimiterPtr & read_limiter = nullptr, bool background = false);

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

    String getBlobFileParentPath(BlobFileId blob_id);

    BlobFilePtr getBlobFile(BlobFileId blob_id);

    friend class PageDirectoryFactory;
    friend class PageStorageControl;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    PSDiskDelegatorPtr delegator;

    FileProviderPtr file_provider;
    Config config;

    LoggerPtr log;

    BlobStats blob_stats;

    DB::LRUCache<BlobFileId, BlobFile> cached_files;
};
using BlobStorePtr = std::shared_ptr<BlobStore>;

} // namespace PS::V3
} // namespace DB
