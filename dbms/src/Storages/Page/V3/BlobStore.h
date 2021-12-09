#pragma once

#include <Common/Exception.h>
#include <Common/LRUCache.h>
#include <Storages/Page/V3/BlobFile.h>
#include <Storages/Page/V3/PageEntriesEdit.h>
#include <Storages/Page/V3/PageEntry.h>
#include <Storages/Page/V3/spacemap/SpaceMap.h>

#include <mutex>


// TBD : need add these into config
#define BLOBSTORE_TEST_PATH "./BlobStore/"
#define BLOBSTORE_CACHED_FD_SIZE 100
#define BLOBSTORE_SMAP_TYPE SpaceMap::SpaceMapType::SMAP64_RBTREE
#define BLOBFILE_NAME_PRE "blobfile_"
#define BLOBFILE_LIMIT_SIZE 512 * DB::MB

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
    using BlobFileId = UInt16;

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
            UInt64 sm_max_caps = BLOBFILE_LIMIT_SIZE;

            UInt64 sm_total_size = 0;
            UInt64 sm_valid_size = 0;
            double sm_valid_rate = 1.0;

            std::mutex sm_lock;
        };

        using BlobStatPtr = std::shared_ptr<BlobStat>;

    public:
        BlobStats(Poco::Logger * log_);

        void lock();

        void unlock();

        void statLock(BlobStatPtr stat);

        void statUnlock(BlobStatPtr stat);

        BlobStatPtr createStat(BlobFileId blob_file_id);

        void earseStat(BlobFileId blob_file_id);

        std::pair<BlobStatPtr, BlobFileId> chooseStat(size_t buf_size);

        UInt64 getPosFromStat(BlobStatPtr stat, size_t buf_size);

        void removePosFromStat(BlobStatPtr stat, UInt64 offset, size_t buf_size);

        BlobStatPtr fileIdToStat(BlobFileId file_id);

#ifndef DBMS_PUBLIC_GTEST
    private:
#endif
        Poco::Logger * log;

        BlobFileId roll_id = 0;
        std::list<BlobFileId> old_ids;
        std::list<BlobStatPtr> stats_map;

        /**
            * TBD : not sure we need total
             *  For now these two value are not update and unused.
             */
        UInt64 total_sm_used;
        UInt64 total_sm_size;

        std::mutex lock_stats;
    };

    BlobStore(const FileProviderPtr & file_provider_);

    void recover();

    BlobStats getAllBlobStats();

    PageEntriesEdit write(DB::WriteBatch & wb, const WriteLimiterPtr & write_limiter);

    // TBD : may replace std::vector<char *> with a align buffer.
    void read(std::vector<std::tuple<BlobFileId, UInt64, size_t>>,
              std::vector<char *> buffers,
              const ReadLimiterPtr & read_limiter = nullptr);

    void read(BlobFileId blob_id, UInt64 offset, char * buffers, size_t size, const ReadLimiterPtr & read_limiter = nullptr);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    // TBD: after single path work, do the multi-path
    // String choosePath();

    std::pair<BlobFileId, UInt64> getPosFromStats(size_t size);

    void removePosFromStats(BlobFileId file_id, UInt64 offset, size_t size);

    String getBlobFilePath(BlobFileId blob_id);

    BlobFilePtr getBlobFile(BlobFileId blob_id);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    FileProviderPtr file_provider;

    Poco::Logger * log;

    BlobStats blob_stats;

    DB::LRUCache<BlobFileId, BlobFile> cached_file;
};

} // namespace PS::V3
} // namespace DB