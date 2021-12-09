#include <Common/Checksum.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Storages/Page/V3/BlobStore.h>

namespace ProfileEvents
{
extern const Event PSMWritePages;
}

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
using ChecksumClass = Digest::CRC64;

#define INVAILD_BLOBFILE_ID UINT16_MAX

BlobStore::BlobStore(const FileProviderPtr & file_provider_)
    : file_provider(file_provider_)
    , log(&Poco::Logger::get("BlobStore"))
    , blob_stats(log)
    , cached_file(BLOBSTORE_CACHED_FD_SIZE)
{
}

PageEntriesEdit BlobStore::write(DB::WriteBatch & wb, const WriteLimiterPtr & write_limiter)
{
    ProfileEvents::increment(ProfileEvents::PSMWritePages, wb.putWriteCount());

    PageEntriesEdit edit;
    const size_t all_page_data_size = wb.getTotalDataSize();

    char * buffer = (char *)alloc(all_page_data_size);
    char * buffer_pos = buffer;
    BlobStore::BlobFileId blob_id;
    UInt64 offset_in_file;

    std::tie(blob_id, offset_in_file) = getPosFromStats(all_page_data_size);
    size_t offset_in_allocated = 0;

    for (auto & write : wb.getWrites())
    {
        switch (write.type)
        {
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::UPSERT:
        {
            ChecksumClass digest;
            PageEntryV3 entry;

            entry.file_id = blob_id;
            entry.size = write.size;
            entry.offset = offset_in_file + offset_in_allocated;
            offset_in_allocated += write.size;
            digest.update(buffer_pos, write.size);
            entry.checksum = digest.checksum();

            UInt64 field_begin, field_end;

            for (size_t i = 0; i < write.offsets.size(); ++i)
            {
                ChecksumClass digest;
                field_begin = write.offsets[i].first;
                field_end = (i == write.offsets.size() - 1) ? write.size : write.offsets[i + 1].first;

                digest.update(buffer_pos + field_begin, field_end - field_begin);
                write.offsets[i].second = digest.checksum();
            }

            if (write.offsets.size())
            {
                // we can swap from WriteBatch instead of copying
                entry.field_offsets.swap(write.offsets);
            }

            write.read_buffer->readStrict(buffer_pos, write.size);
            buffer_pos += write.size;

            if (write.type == WriteBatch::WriteType::PUT)
            {
                edit.put(write.page_id, entry);
            }
            else // WriteBatch::WriteType::UPSERT
            {
                edit.upsertPage(write.page_id, entry);
            }


            break;
        }
        case WriteBatch::WriteType::DEL:
        {
            edit.del(write.page_id);
        }
        case WriteBatch::WriteType::REF:
        {
            edit.ref(write.page_id, write.ori_page_id);
            break;
        }
        }
    }

    if (buffer_pos != buffer + all_page_data_size)
    {
        removePosFromStats(blob_id, offset_in_file, all_page_data_size);
        throw Exception("write batch have a invalid total size, or something wrong in parse write batch.",
                        ErrorCodes::LOGICAL_ERROR);
    }

    auto blob_file = getBlobFile(blob_id);
    blob_file->write(buffer, offset_in_file, all_page_data_size, write_limiter);
    return edit;
}

std::pair<BlobStore::BlobFileId, UInt64> BlobStore::getPosFromStats(size_t size)
{
    UInt16 blob_file_id = INVAILD_BLOBFILE_ID;
    BlobStatPtr stat;

    blob_stats.lock();
    std::tie(stat, blob_file_id) = blob_stats.chooseStat(size);

    if (blob_file_id != INVAILD_BLOBFILE_ID)
    {
        stat = blob_stats.createStat(blob_file_id);
    }

    blob_stats.unlock();

    // Get Postion from single stat
    blob_stats.statLock(stat);
    UInt64 offset = blob_stats.getPosFromStat(stat, size);

    // Can't insert into this spacemap
    if (offset == UINT64_MAX)
    {
        blob_stats.statUnlock(stat);
        throw Exception("Get postion from BlobStat Failed, it may caused by `sm_max_caps` is no corrent.",
                        ErrorCodes::LOGICAL_ERROR);
    }
    blob_stats.statUnlock(stat);
    return std::make_pair(stat->id, offset);
}

void BlobStore::removePosFromStats(BlobFileId blob_id, UInt64 offset, size_t size)
{
    blob_stats.removePosFromStat(blob_stats.fileIdToStat(blob_id), offset, size);
}

void BlobStore::read(std::vector<std::tuple<BlobStore::BlobFileId, UInt64, size_t>> req_list,
                     std::vector<char *> buffers,
                     const ReadLimiterPtr & read_limiter)
{
    assert(req_list.size() == buffers.size());

    if (req_list.empty())
    {
        return;
    }

    if (req_list.size() == 1)
    {
        read(std::get<0>(req_list.front()), std::get<1>(req_list.front()), buffers.front(), std::get<2>(req_list.front()));
        return;
    }

    std::sort(req_list.begin(), req_list.end(), [](const std::tuple<BlobStore::BlobFileId, UInt64, size_t> & l, const std::tuple<BlobStore::BlobFileId, UInt64, size_t> & r) {
        return std::get<1>(l) < std::get<1>(r);
    });

    size_t buf_size = 0;
    for (const auto & p : req_list)
    {
        buf_size += std::get<2>(p);
    }

    size_t index = 0;
    for (const auto & [blob_id, offset, size] : req_list)
    {
        BlobStore::read(blob_id, offset, buffers[index++], size, read_limiter);
    }
}

void BlobStore::read(BlobStore::BlobFileId blob_id, UInt64 offset, char * buffers, size_t size, const ReadLimiterPtr & read_limiter)
{
    assert(buffers != nullptr);
    getBlobFile(blob_id)->read(buffers, offset, size, read_limiter);
}


String BlobStore::getBlobFilePath(BlobStore::BlobFileId blob_id)
{
    return (String)BLOBSTORE_TEST_PATH + BLOBFILE_NAME_PRE + DB::toString(blob_id);
}

BlobFilePtr BlobStore::getBlobFile(BlobStore::BlobFileId blob_id)
{
    auto blob_file = cached_file.get(blob_id);
    if (blob_file)
    {
        return blob_file;
    }

    blob_file = std::make_shared<BlobFile>(getBlobFilePath(blob_id), file_provider);
    cached_file.set(blob_id, blob_file);
    return blob_file;
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

    for (auto & stat : stats_map)
    {
        if (stat->id == blob_file_id)
        {
            throw Exception("BlobStats won't create [BlobFileId=" + DB::toString(blob_file_id) + "] which is exist",
                            ErrorCodes::LOGICAL_ERROR);
        }
    }

    stat = std::make_shared<BlobStat>();

    LOG_DEBUG(log, "Created a new BlobStat which [BlobFileId= " << blob_file_id << "]");
    stat->id = blob_file_id;
    stat->smap = SpaceMap::createSpaceMap(BLOBSTORE_SMAP_TYPE, 0, BLOBFILE_LIMIT_SIZE);

    stats_map.emplace_back(stat);

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

    for (auto it = stats_map.begin(); it != stats_map.end(); it++)
    {
        stat = *it;
        if (stat->id == blob_file_id)
            break;
    }

    if (!stat)
    {
        LOG_ERROR(log, "No exist BlobStat which [BlobFileId= " << blob_file_id << "]");
        return;
    }

    LOG_DEBUG(log, "Eease BlobStat from maps which [BlobFileId= " << blob_file_id << "]");

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

    std::tie(offset, max_cap) = stat->smap->searchInsertOffset(buf_size);

    // Whatever request success or not, it still need update.
    stat->sm_max_caps = max_cap;
    if (offset != UINT64_MAX)
    {
        if (offset + buf_size > stat->sm_total_size)
        {
            // This file must be expanded
            auto expand_size = buf_size - (stat->sm_total_size - offset);
            stat->sm_total_size += expand_size;
            stat->sm_valid_size += buf_size;
        }
        else
        {
            /**
             * All in old place, no expand.
             * Just update valid size
             */
            stat->sm_valid_size += buf_size;
        }

        stat->sm_valid_rate = stat->sm_valid_size * 1.0 / stat->sm_total_size;
    }
    return offset;
}

void BlobStore::BlobStats::removePosFromStat(BlobStatPtr stat, UInt64 offset, size_t buf_size)
{
    if (!stat->smap->markFree(offset, buf_size))
    {
        throw Exception("[offset=" + DB::toString(offset) + " , buf_size=" + DB::toString(buf_size) + "] is invalid.",
                        ErrorCodes::LOGICAL_ERROR);
    }

    stat->sm_valid_size -= buf_size;
    stat->sm_valid_rate = stat->sm_valid_size * 1.0 / stat->sm_total_size;
}

BlobStatPtr BlobStore::BlobStats::fileIdToStat(BlobFileId file_id)
{
    for (auto & stat : stats_map)
    {
        if (stat->id == file_id)
        {
            return stat;
        }
    }

    throw Exception("Can't find BlobStat with [BlobFileId=" + DB::toString(file_id) + "]",
                    ErrorCodes::LOGICAL_ERROR);
}

} // namespace PS::V3
} // namespace DB