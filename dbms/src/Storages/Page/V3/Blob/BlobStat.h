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

#include <Common/Logger.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V3/Blob/BlobConfig.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/PageType.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>
#include <Storages/PathPool.h>
#include <common/types.h>

namespace DB::PS::V3
{

/**
 * BlobStats is a collection of BlobStat, which have red-black tree/ map implemention.
 * Each node on the tree records the information of a Blob(<BlobFile, SpaceMap>).
 */
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

    // All operations on SpaceMap should be done by this class.
    struct BlobStat
    {
        const BlobFileId id;
        std::atomic<BlobStatType> type;

        std::mutex sm_lock;
        const SpaceMapPtr smap;

        // The max capacity hint of all available slots in SpaceMap
        // A hint means that it is not an absolutely accurate value after inserting data,
        // but is useful for quickly choosing BlobFile.
        // Should call `recalculateCapacity` to get an accurate value after removing data.
        UInt64 sm_max_caps = 0;
        // The current file size of the BlobFile
        UInt64 sm_total_size = 0;
        // The sum of the size of all valid data in the BlobFile
        UInt64 sm_valid_size = 0;
        // sm_valid_size / sm_total_size
        double sm_valid_rate = 0.0;

        BlobStat(BlobFileId id_, SpaceMap::SpaceMapType sm_type, UInt64 sm_max_caps_, BlobStatType type_)
            : id(id_)
            , type(type_)
            , smap(SpaceMap::createSpaceMap(sm_type, 0, sm_max_caps_))
            , sm_max_caps(sm_max_caps_)
        {}

        [[nodiscard]] std::unique_lock<std::mutex> lock() { return std::unique_lock(sm_lock); }

        [[nodiscard]] std::unique_lock<std::mutex> defer_lock() { return std::unique_lock(sm_lock, std::defer_lock); }

        bool isNormal() const { return type.load() == BlobStatType::NORMAL; }

        bool isReadOnly() const { return type.load() == BlobStatType::READ_ONLY; }

        void changeToReadOnly() { type.store(BlobStatType::READ_ONLY); }

        BlobFileOffset getPosFromStat(size_t buf_size, const std::unique_lock<std::mutex> &);

        /**
          * The return value is the valid data size remained in the BlobFile after the remove
          */
        size_t removePosFromStat(BlobFileOffset offset, size_t buf_size, const std::unique_lock<std::mutex> &);

        /**
         * This method is only used when blobstore restore
         * Restore space map won't change the `sm_total_size`/`sm_valid_size`/`sm_valid_rate`
         */
        std::tuple<bool, String> restoreSpaceMap(BlobFileOffset offset, size_t buf_size);

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
    BlobStats(LoggerPtr log_, PSDiskDelegatorPtr delegator_, BlobConfig & config);

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

    BlobStatPtr createStatNotChecking(BlobFileId blob_file_id, UInt64 max_caps, const std::lock_guard<std::mutex> &);

    BlobStatPtr createStat(BlobFileId blob_file_id, UInt64 max_caps, const std::lock_guard<std::mutex> & guard);

    void eraseStat(const BlobStatPtr && stat, const std::lock_guard<std::mutex> &);

    void eraseStat(BlobFileId blob_file_id, const std::lock_guard<std::mutex> &);

    /**
     * Change all existing BlobStat to be `ReadOnly`. So following new blobs will
     * be written to new `BlobStat`.
     */
    void setAllToReadOnly();

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
    std::pair<BlobStatPtr, BlobFileId> chooseStat(
        size_t buf_size,
        PageType page_type,
        const std::lock_guard<std::mutex> &);

    BlobStatPtr blobIdToStat(BlobFileId file_id, bool ignore_not_exist = false);

    using StatsMap = std::map<String, std::list<BlobStatPtr>>;
    StatsMap getStats() const;

    static std::pair<BlobFileId, String> getBlobIdFromName(const String & blob_name);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    std::tuple<bool, String> restoreByEntry(const PageEntryV3 & entry);
    void restore();
    template <typename>
    friend class PageDirectoryFactory;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    LoggerPtr log;
    PSDiskDelegatorPtr delegator;
    BlobConfig & config;

    mutable std::mutex lock_stats;
    const PageTypeAndConfig page_type_and_config;
    BlobFileId cur_max_id = 1;
    // Index for selecting next path for creating new blobfile
    UInt32 stats_map_path_index = 0;
    std::map<String, std::list<BlobStatPtr>> stats_map;
};

} // namespace DB::PS::V3
