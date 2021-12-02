#pragma once

#include <Common/Exception.h>
#include <Storages/Page/V3/BlobFile.h>
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
// TBD : need add these into config
#define BLOBFILE_NAME_PRE "blobfile_"
#define BLOBFILE_LIMIT_SIZE 512 * DB::MB
#define BLOBFILE_CACHED_FD_SIZE 20
#define BLOBFILE_SMAP_TYPE SpaceMap::SpaceMapType::SMAP64_RBTREE

class BlobStore
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
            UInt64 sm_valid_rate = 1;

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

    private:
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

    void write(char * buffer, size_t size);

    void read(BlobFileId file, UInt64 offset, char * buffer, size_t size);

private:
    // TBD: after single path work, do the multi-path
    // String choosePath();


private:
    std::map<BlobFileId, BlobFile> cached_writer;

    FileProviderPtr file_provider;

    Poco::Logger * log;

    BlobStats blob_stats;
};

} // namespace PS::V3
} // namespace DB