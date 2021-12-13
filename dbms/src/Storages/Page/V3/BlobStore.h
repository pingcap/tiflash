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
        struct BlobStat
        {
            SpaceMapPtr smap;

            BlobFileId id;

            /**
            * If no any data inside. It shoule be same as space map `biggest_cap`
            */
            UInt64 sm_max_caps = 0;

            UInt64 sm_total_size = 0;
            UInt64 sm_valid_size = 0;
            double sm_valid_rate = 1.0;

            std::mutex sm_lock;
        };

        using BlobStatPtr = std::shared_ptr<BlobStat>;

    public:
        BlobStats(Poco::Logger * log_, BlobStore::Config config);

        std::lock_guard<std::mutex> lock();

        std::lock_guard<std::mutex> statLock(BlobStatPtr stat);

        BlobStatPtr createStat(BlobFileId blob_file_id);

        void eraseStat(BlobFileId blob_file_id);

        std::pair<BlobStatPtr, BlobFileId> chooseStat(size_t buf_size, UInt64 file_limit_size);

        BlobFileOffset getPosFromStat(BlobStatPtr stat, size_t buf_size);

        void removePosFromStat(BlobStatPtr stat, BlobFileOffset offset, size_t buf_size);

        BlobStatPtr fileIdToStat(BlobFileId file_id);

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

    BlobStats getAllBlobStats();

    PageEntriesEdit write(DB::WriteBatch & wb, const WriteLimiterPtr & write_limiter = nullptr);

    PageMap read(PageIDAndEntriesV3 & entries, const ReadLimiterPtr & read_limiter = nullptr);

    Page read(const PageIDAndEntryV3 & entry, const ReadLimiterPtr & read_limiter = nullptr);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    void read(BlobFileId blob_id, BlobFileOffset offset, char * buffers, size_t size, const ReadLimiterPtr & read_limiter = nullptr);

    std::pair<BlobFileId, BlobFileOffset> getPosFromStats(size_t size);

    void removePosFromStats(BlobFileId file_id, BlobFileOffset offset, size_t size);

    String getBlobFilePath(BlobFileId blob_id);

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

} // namespace PS::V3
} // namespace DB