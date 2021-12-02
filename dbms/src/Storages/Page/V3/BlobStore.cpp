#include <Storages/Page/V3/BlobStore.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace PS::V3
{
using BlobStat = BlobStore::BlobStats::BlobStat;
using BlobStatPtr = BlobStore::BlobStats::BlobStatPtr;

#define INVAILD_BLOBFILE_ID UINT16_MAX

BlobStore::BlobStore(const FileProviderPtr & file_provider_)
    : file_provider(file_provider_)
    , log(&Poco::Logger::get("RBTreeSpaceMap"))
    , blob_stats(log)
{
}

void BlobStore::write(char * buffer, size_t size)
{
    UInt16 blob_file_id = INVAILD_BLOBFILE_ID;
    BlobStatPtr stat;

find_stat:
    blob_stats.lock();
    std::tie(stat, blob_file_id) = blob_stats.chooseStat(size);

    if (blob_file_id != INVAILD_BLOBFILE_ID)
    {
        stat = blob_stats.createStat(blob_file_id);
    }

    blob_stats.unlock();

    // blobfile write
    blob_stats.statLock(stat);
    UInt64 offset = blob_stats.getPosFromStat(stat, size);
    if (offset == UINT64_MAX)
    {
        blob_stats.statUnlock(stat);
        goto find_stat;
    }
    blob_stats.statUnlock(stat);

    // TBD: do write
    (void)buffer;
}

BlobStore::BlobStats::BlobStats(Poco::Logger * log_)
    : log(log_)
    , total_sm_used(0)
    , total_sm_size(0)
{
}

void BlobStore::BlobStats::lock()
{
    lock_stats.lock();
}

void BlobStore::BlobStats::unlock()
{
    lock_stats.unlock();
}

void BlobStore::BlobStats::statLock(BlobStatPtr stat)
{
    stat->sm_lock.lock();
}

void BlobStore::BlobStats::statUnlock(BlobStatPtr stat)
{
    stat->sm_lock.unlock();
}


BlobStatPtr BlobStore::BlobStats::createStat(BlobFileId blob_file_id)
{
    BlobStatPtr stat;

    // New blob file id won't bigger than roll_id
    if (blob_file_id > roll_id)
    {
        throw Exception("BlobStats won't create [BlobFileId=" + DB::toString(blob_file_id) + "], which is bigger than [RollMaxId="
                            + DB::toString(roll_id) + "]",
                        ErrorCodes::LOGICAL_ERROR);
    }

    stat = std::make_shared<BlobStat>();

    LOG_DEBUG(log, "Created a new BlobStat which [BlobFileId= " << blob_file_id << "]");
    stat->id = blob_file_id;
    stat->smap = SpaceMap::createSpaceMap(BLOBFILE_SMAP_TYPE, 0, BLOBFILE_LIMIT_SIZE);

    stats_map.emplace_back(std::move(stat));

    // Roll to the next new blob id
    if (blob_file_id == roll_id)
    {
        roll_id++;
    }

    return stat;
}

void BlobStore::BlobStats::earseStat(BlobFileId blob_file_id)
{
    BlobStatPtr stat;
    for (auto stat : stats_map)
    {
        if (stat->id == blob_file_id)
            break;
    }

    if (!stat)
    {
        LOG_ERROR(log, "No exist BlobStat which [BlobFileId= " << blob_file_id << "]");
        return;
    }

    stats_map.remove(stat);
    old_ids.emplace_back(blob_file_id);
}

std::pair<BlobStatPtr, BlobStore::BlobFileId> BlobStore::BlobStats::chooseStat(size_t buf_size)
{
    BlobStatPtr stat_ptr = NULL;
    BlobFileId littest_valid_rate = 2;

    // No stats exist
    if (stats_map.empty())
    {
        goto new_blob;
    }

    for (auto stat : stats_map)
    {
        if (stat->sm_max_caps >= buf_size
            && stat->sm_total_size + buf_size < BLOBFILE_LIMIT_SIZE
            && stat->sm_valid_size < littest_valid_rate)
        {
            littest_valid_rate = stat->sm_valid_size;
            stat_ptr = stat;
        }
    }

    if (!stat_ptr)
    {
        goto new_blob;
    }

    return std::make_pair(stat_ptr, INVAILD_BLOBFILE_ID);

new_blob:
    /**
     * If we do have a old blob id which may be GC(Then this id have been removed) 
     * in the `old_ids` . Then we should get a old id rather than create new one.
     * If there are no old id in `old_ids` , we will use the `roll_id` as the new 
     * id return. After roll_id generate a `BlobStat`, it will `++`.
     */
    if (old_ids.empty())
    {
        return std::make_pair(stat_ptr, roll_id);
    }
    else
    {
        auto rv = std::make_pair(stat_ptr, old_ids.front());
        old_ids.pop_front();
        return rv;
    }
}

UInt64 BlobStore::BlobStats::getPosFromStat(BlobStatPtr stat, size_t buf_size)
{
    UInt64 offset = 0, max_cap = 0;
    stat->smap->searchRange(buf_size, &offset, &max_cap);

    // Whatever request success or not, it still need update.
    stat->sm_max_caps = max_cap;
    if (offset != UINT64_MAX)
    {
        // TBD

        // Int64 size_update = offset - stat->sm_total_size;
        // if (size_update > 0) {

        //     stat->sm_total_size += size_update
        // }
    }
    return offset;
}

} // namespace PS::V3
} // namespace DB