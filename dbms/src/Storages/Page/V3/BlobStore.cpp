#include <Common/Checksum.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Storages/Page/V3/BlobStore.h>
#include <Storages/Page/V3/PageEntriesEdit.h>

#include <ext/scope_guard.h>
#include <mutex>

namespace ProfileEvents
{
extern const Event PSMWritePages;
extern const Event PSMReadPages;
} // namespace ProfileEvents

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CHECKSUM_DOESNT_MATCH;
} // namespace ErrorCodes

namespace PS::V3
{
static constexpr bool BLOBSTORE_CHECKSUM_ON_READ = true;

using BlobStat = BlobStore::BlobStats::BlobStat;
using BlobStatPtr = BlobStore::BlobStats::BlobStatPtr;
using ChecksumClass = Digest::CRC64;

BlobStore::BlobStore(const FileProviderPtr & file_provider_, String path_, BlobStore::Config config_)
    : file_provider(file_provider_)
    , path(path_)
    , config(config_)
    , log(&Poco::Logger::get("BlobStore"))
    , blob_stats(log, config_)
    , cached_file(config.cached_fd_size)
{
}

PageEntriesEdit BlobStore::write(DB::WriteBatch & wb, const WriteLimiterPtr & write_limiter)
{
    ProfileEvents::increment(ProfileEvents::PSMWritePages, wb.putWriteCount());

    PageEntriesEdit edit;
    const size_t all_page_data_size = wb.getTotalDataSize();

    if (all_page_data_size > config.file_limit_size)
    {
        throw Exception(fmt::format("Write batch is too large. It should less than [file_limit_size={}]",
                                    config.file_limit_size.get()),
                        ErrorCodes::LOGICAL_ERROR);
    }

    if (all_page_data_size == 0)
    {
        // Shortcut for WriteBatch that don't need to persist blob data.
        for (auto & write : wb.getWrites())
        {
            switch (write.type)
            {
            case WriteBatch::WriteType::DEL:
            {
                edit.del(write.page_id);
                break;
            }
            case WriteBatch::WriteType::REF:
            {
                edit.ref(write.page_id, write.ori_page_id);
                break;
            }
            case WriteBatch::WriteType::PUT:
            { // Only putExternal won't have data.
                PageEntryV3 entry;
                entry.tag = write.tag;

                edit.put(write.page_id, entry);
                break;
            }
            default:
                throw Exception("write batch have a invalid total size.",
                                ErrorCodes::LOGICAL_ERROR);
            }
        }
        return edit;
    }

    char * buffer = static_cast<char *>(alloc(all_page_data_size));
    SCOPE_EXIT({
        free(buffer, all_page_data_size);
    });
    char * buffer_pos = buffer;
    auto [blob_id, offset_in_file] = getPosFromStats(all_page_data_size);

    size_t offset_in_allocated = 0;

    for (auto & write : wb.getWrites())
    {
        switch (write.type)
        {
        case WriteBatch::WriteType::PUT:
        {
            ChecksumClass digest;
            PageEntryV3 entry;

            write.read_buffer->readStrict(buffer_pos, write.size);

            entry.file_id = blob_id;
            entry.size = write.size;
            entry.tag = write.tag;
            entry.offset = offset_in_file + offset_in_allocated;
            offset_in_allocated += write.size;

            digest.update(buffer_pos, write.size);
            entry.checksum = digest.checksum();

            UInt64 field_begin, field_end;

            for (size_t i = 0; i < write.offsets.size(); ++i)
            {
                ChecksumClass field_digest;
                field_begin = write.offsets[i].first;
                field_end = (i == write.offsets.size() - 1) ? write.size : write.offsets[i + 1].first;

                field_digest.update(buffer_pos + field_begin, field_end - field_begin);
                write.offsets[i].second = field_digest.checksum();
            }

            if (!write.offsets.empty())
            {
                // we can swap from WriteBatch instead of copying
                entry.field_offsets.swap(write.offsets);
            }

            buffer_pos += write.size;
            edit.put(write.page_id, entry);
            break;
        }
        case WriteBatch::WriteType::DEL:
        {
            edit.del(write.page_id);
            break;
        }
        case WriteBatch::WriteType::REF:
        {
            edit.ref(write.page_id, write.ori_page_id);
            break;
        }
        default:
            throw Exception(fmt::format("Unknown write type: {}", write.type));
        }
    }

    if (buffer_pos != buffer + all_page_data_size)
    {
        removePosFromStats(blob_id, offset_in_file, all_page_data_size);
        throw Exception("write batch have a invalid total size, or something wrong in parse write batch.",
                        ErrorCodes::LOGICAL_ERROR);
    }

    try
    {
        auto blob_file = getBlobFile(blob_id);
        blob_file->write(buffer, offset_in_file, all_page_data_size, write_limiter);
    }
    catch (DB::Exception & e)
    {
        removePosFromStats(blob_id, offset_in_file, all_page_data_size);
        LOG_FMT_ERROR(log, "[Blobid={}, offset_in_file={}, size={}] write failed.", blob_id, offset_in_file, all_page_data_size);
        throw e;
    }

    return edit;
}

void BlobStore::remove(const PageEntriesV3 & del_entries)
{
    for (const auto & entry : del_entries)
    {
        if (entry.size == 0)
        {
            throw Exception(fmt::format("Invaild entry. entry size 0. [id={},offset={}]",
                                        entry.file_id,
                                        entry.offset));
        }
        removePosFromStats(entry.file_id, entry.offset, entry.size);
    }
}

std::pair<BlobFileId, BlobFileOffset> BlobStore::getPosFromStats(size_t size)
{
    BlobStatPtr stat;

    auto lock_stat = [size, this, &stat]() -> std::lock_guard<std::mutex> {
        auto lock_stats = blob_stats.lock();
        BlobFileId blob_file_id = INVALID_BLOBFILE_ID;
        std::tie(stat, blob_file_id) = blob_stats.chooseStat(size, config.file_limit_size, lock_stats);

        // No valid stat for puting data with `size`, create a new one
        if (stat == nullptr)
        {
            stat = blob_stats.createStat(blob_file_id, lock_stats);
        }

        // We need to assume that this insert will reduce max_cap.
        // Because other threads may also be waiting for BlobStats to chooseStat during this time.
        // If max_cap is not reduced, it may cause the same BlobStat to accept multiple buffers and exceed its max_cap.
        // After the BlobStore records the buffer size, max_caps will also get an accurate update.
        // So there won't get problem in reducing max_caps here.
        stat->sm_max_caps -= size;

        // We must get the lock from BlobStat under the BlobStats lock
        // to ensure that BlobStat updates are serialized.
        // Otherwise it may cause stat to fail to get the span for writing
        // and throwing exception.

        return stat->lock();
    }();

    // Get Postion from single stat
    auto old_max_cap = stat->sm_max_caps;
    BlobFileOffset offset = stat->getPosFromStat(size);

    // Can't insert into this spacemap
    if (offset == INVALID_BLOBFILE_OFFSET)
    {
        stat->smap->logStats();
        throw Exception(fmt::format("Get postion from BlobStat failed, it may caused by `sm_max_caps` is no correct. [size={}, old_max_caps={}, max_caps={}, BlobFileId={}]",
                                    size,
                                    old_max_cap,
                                    stat->sm_max_caps,
                                    stat->id),
                        ErrorCodes::LOGICAL_ERROR);
    }

    return std::make_pair(stat->id, offset);
}

void BlobStore::removePosFromStats(BlobFileId blob_id, BlobFileOffset offset, size_t size)
{
    const auto & stat = blob_stats.fileIdToStat(blob_id);
    auto lock = stat->lock();
    stat->removePosFromStat(offset, size);

    if (stat->isReadOnly() && stat->sm_valid_size == 0)
    {
        LOG_FMT_INFO(log, "Removing BlobFile [BlobId={}]", blob_id);
        auto lock_stats = blob_stats.lock();
        blob_stats.eraseStat(std::move(stat), lock_stats);
        getBlobFile(blob_id)->remove();
    }
}


PageMap BlobStore::read(PageIDAndEntriesV3 & entries, const ReadLimiterPtr & read_limiter)
{
    ProfileEvents::increment(ProfileEvents::PSMReadPages, entries.size());

    // Sort in ascending order by offset in file.
    std::sort(entries.begin(), entries.end(), [](const PageIDAndEntryV3 & a, const PageIDAndEntryV3 & b) {
        return a.second.offset < b.second.offset;
    });

    // allocate data_buf that can hold all pages
    size_t buf_size = 0;
    for (const auto & p : entries)
    {
        buf_size += p.second.size;
    }

    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) {
        free(p, buf_size);
    });

    char * pos = data_buf;
    PageMap page_map;
    for (const auto & [page_id, entry] : entries)
    {
        read(entry.file_id, entry.offset, pos, entry.size, read_limiter);

        if constexpr (BLOBSTORE_CHECKSUM_ON_READ)
        {
            ChecksumClass digest;
            digest.update(pos, entry.size);
            auto checksum = digest.checksum();
            if (unlikely(entry.size != 0 && checksum != entry.checksum))
            {
                throw Exception(fmt::format("Page id [{}] checksum not match, broken file: {}, expected: 0x{:X}, but: 0x{:X}",
                                            page_id,
                                            getBlobFilePath(entry.file_id),
                                            entry.checksum,
                                            checksum),
                                ErrorCodes::CHECKSUM_DOESNT_MATCH);
            }
        }

        Page page;
        page.page_id = page_id;
        page.data = ByteBuffer(pos, pos + entry.size);
        page.mem_holder = mem_holder;
        page_map.emplace(page_id, page);

        pos += entry.size;
    }

    if (unlikely(pos != data_buf + buf_size))
        throw Exception(fmt::format("[end_position={}] not match the [current_position={}]",
                                    data_buf + buf_size,
                                    pos),
                        ErrorCodes::LOGICAL_ERROR);

    return page_map;
}

Page BlobStore::read(const PageIDAndEntryV3 & id_entry, const ReadLimiterPtr & read_limiter)
{
    const auto & [page_id, entry] = id_entry;
    size_t buf_size = entry.size;

    char * data_buf = static_cast<char *>(alloc(buf_size));
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) {
        free(p, buf_size);
    });

    read(entry.file_id, entry.offset, data_buf, buf_size, read_limiter);

    Page page;
    page.page_id = page_id;
    page.data = ByteBuffer(data_buf, data_buf + buf_size);
    page.mem_holder = mem_holder;

    return page;
}

void BlobStore::read(BlobFileId blob_id, BlobFileOffset offset, char * buffers, size_t size, const ReadLimiterPtr & read_limiter)
{
    assert(buffers != nullptr);
    getBlobFile(blob_id)->read(buffers, offset, size, read_limiter);
}


std::vector<BlobFileId> BlobStore::getGCStats()
{
    const auto stats_list = blob_stats.getStats();
    std::vector<BlobFileId> blob_need_gc;

    for (const auto & stat : stats_list)
    {
        if (stat->isReadOnly())
        {
            LOG_FMT_TRACE(log, "Current [BlobFileId={}] is read-only", stat->id);
            continue;
        }

        auto lock = stat->lock();
        auto right_margin = stat->smap->getRightMargin();

        stat->sm_valid_rate = stat->sm_valid_size * 1.0 / right_margin;
        if (stat->sm_valid_rate > 1.0)
        {
            LOG_FMT_ERROR(log, "Current blob got an invalid rate {:.2f}, total size is {}, valid size is {}, right margin is {}", stat->sm_valid_rate, stat->sm_total_size, stat->sm_valid_size, right_margin);
            assert(false);
        }

        // Check if GC is required
        if (stat->sm_valid_rate <= config.heavy_gc_valid_rate)
        {
            LOG_FMT_TRACE(log, "Current [BlobFileId={}] valid rate is {:.2f}, Need do compact GC", stat->id, stat->sm_valid_rate);
            stat->sm_total_size = stat->sm_valid_size;
            blob_need_gc.emplace_back(stat->id);

            // Change current stat to read only
            stat->changeToReadOnly();
        }
        else
        {
            LOG_FMT_TRACE(log, "Current [BlobFileId={}] valid rate is {:.2f}, No need to GC.", stat->id, stat->sm_valid_rate);
        }

        if (right_margin != stat->sm_total_size)
        {
            auto blobfile = getBlobFile(stat->id);
            blobfile->truncate(right_margin);
            stat->sm_total_size = right_margin;
        }
    }

    return blob_need_gc;
}


PageEntriesEdit BlobStore::gc(std::map<BlobFileId, PageIdAndVersionedEntries> & entries_need_gc,
                              const PageSize & total_page_size,
                              const WriteLimiterPtr & write_limiter,
                              const ReadLimiterPtr & read_limiter)
{
    std::vector<std::tuple<BlobFileId, BlobFileOffset, PageSize>> written_blobs;
    PageEntriesEdit edit;

    if (total_page_size == 0)
    {
        throw Exception("BlobStore can't do gc if nothing need gc.", ErrorCodes::LOGICAL_ERROR);
    }
    LOG_FMT_INFO(log, "BlobStore will migrate {}M into new Blobs", total_page_size / DB::MB);

    const auto config_file_limit = config.file_limit_size.get();

    auto alloc_size = total_page_size > config_file_limit ? config_file_limit : total_page_size;

    // We could make the memory consumption smooth during GC.
    char * data_buf = static_cast<char *>(alloc(alloc_size));
    SCOPE_EXIT({
        free(data_buf, alloc_size);
    });
    BlobFileOffset remain_page_size = total_page_size - alloc_size;

    char * data_pos = data_buf;
    BlobFileOffset offset_in_data = 0;
    BlobFileId blobfile_id;
    BlobFileOffset file_offset_beg;
    std::tie(blobfile_id, file_offset_beg) = getPosFromStats(alloc_size);


    auto write_blob = [this, written_blobs, total_page_size](BlobFileId blobfile_id_,
                                                             char * data_buf_,
                                                             BlobFileOffset & file_offset_beg_,
                                                             BlobFileOffset & offset_in_data_,
                                                             const WriteLimiterPtr & write_limiter_,
                                                             Poco::Logger * log_) {
        try
        {
            auto blob_file_ = getBlobFile(blobfile_id_);
            LOG_FMT_INFO(log_, "Blob write [blib_id={}, offset={}, size={}]", blobfile_id_, file_offset_beg_, offset_in_data_);
            blob_file_->write(data_buf_, file_offset_beg_, offset_in_data_, write_limiter_);
        }
        catch (DB::Exception & e)
        {
            LOG_FMT_ERROR(log_, "Blob [Blobid={}, offset={}, size={}] write failed.", blobfile_id_, file_offset_beg_, total_page_size);
            for (const auto & [blobfile_id_revert, file_offset_beg_revert, page_size_revert] : written_blobs)
            {
                removePosFromStats(blobfile_id_revert, file_offset_beg_revert, page_size_revert);
            }
            throw e;
        }
    };

    // Although this is a three-layer loop
    // the amount of data can be seen as linear and fixed
    // If it is changed to a single-layer loop, the parameters will change.
    // which will bring more memory waste, and the execution speed will not be faster
    for (const auto & [file_id, versioned_pageid_entry_list] : entries_need_gc)
    {
        for (const auto & [page_id, versioned_entry] : versioned_pageid_entry_list)
        {
            for (const auto & [versioned, entry] : versioned_entry)
            {
                // When we can't load the remaining data.
                // we will use the original buffer to find an area to load the remaining data
                if (offset_in_data + entry.size > config_file_limit)
                {
                    assert(file_offset_beg == 0);
                    if (offset_in_data != alloc_size)
                    {
                        removePosFromStats(blobfile_id, offset_in_data, alloc_size - offset_in_data);
                    }
                    remain_page_size += alloc_size - offset_in_data;

                    // Write data into Blob.
                    written_blobs.emplace_back(std::make_tuple(blobfile_id, file_offset_beg, offset_in_data));
                    write_blob(blobfile_id, data_buf, file_offset_beg, offset_in_data, write_limiter, log);

                    // reset the position
                    data_pos = data_buf;
                    offset_in_data = 0;

                    auto loop_alloc_size = remain_page_size > config_file_limit ? config_file_limit : remain_page_size;

                    remain_page_size -= loop_alloc_size;

                    std::tie(blobfile_id, file_offset_beg) = getPosFromStats(loop_alloc_size);
                }

                PageEntryV3 new_entry;

                read(file_id, entry.offset, data_pos, entry.size, read_limiter);

                // No need do crc again, crc won't be changed.
                new_entry.checksum = entry.checksum;

                // Need copy the field_offsets
                new_entry.field_offsets = entry.field_offsets;

                // Entry size won't be changed.
                new_entry.size = entry.size;

                new_entry.file_id = blobfile_id;
                new_entry.offset = file_offset_beg + offset_in_data;

                offset_in_data += new_entry.size;
                data_pos += new_entry.size;

                edit.upsertPage(page_id, versioned, new_entry);
            }
        }
    }

    if (offset_in_data != 0)
    {
        written_blobs.emplace_back(std::make_tuple(blobfile_id, file_offset_beg, offset_in_data));
        write_blob(blobfile_id, data_buf, file_offset_beg, offset_in_data, write_limiter, log);
    }

    return edit;
}


String BlobStore::getBlobFilePath(BlobFileId blob_id) const
{
    return path + "/blobfile_" + DB::toString(blob_id);
}

BlobFilePtr BlobStore::getBlobFile(BlobFileId blob_id)
{
    return cached_file.getOrSet(blob_id, [this, blob_id]() -> BlobFilePtr {
                          return std::make_shared<BlobFile>(getBlobFilePath(blob_id), file_provider);
                      })
        .first;
}

BlobStore::BlobStats::BlobStats(Poco::Logger * log_, BlobStore::Config config_)
    : log(log_)
    , config(config_)
{
}

std::lock_guard<std::mutex> BlobStore::BlobStats::lock() const
{
    return std::lock_guard(lock_stats);
}


BlobStatPtr BlobStore::BlobStats::createStat(BlobFileId blob_file_id, const std::lock_guard<std::mutex> &)
{
    BlobStatPtr stat = nullptr;

    // New blob file id won't bigger than roll_id
    if (blob_file_id > roll_id)
    {
        throw Exception(fmt::format("BlobStats won't create [BlobFileId={}], which is bigger than [RollMaxId={}]",
                                    blob_file_id,
                                    roll_id),
                        ErrorCodes::LOGICAL_ERROR);
    }

    for (auto & stat : stats_map)
    {
        if (stat->id == blob_file_id)
        {
            throw Exception(fmt::format("BlobStats won't create [BlobFileId={}] which is exist",
                                        blob_file_id),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }

    stat = std::make_shared<BlobStat>();

    LOG_FMT_DEBUG(log, "Created a new BlobStat [BlobFileId= {}]", blob_file_id);
    stat->id = blob_file_id;
    stat->smap = SpaceMap::createSpaceMap(static_cast<SpaceMap::SpaceMapType>(config.spacemap_type.get()), 0, config.file_limit_size);
    stat->sm_max_caps = config.file_limit_size;
    stat->type = BlobStatType::NORMAL;

    stats_map.emplace_back(stat);

    // Roll to the next new blob id
    if (blob_file_id == roll_id)
    {
        roll_id++;
    }

    return stat;
}

void BlobStore::BlobStats::eraseStat(const BlobStatPtr && stat, const std::lock_guard<std::mutex> &)
{
    old_ids.emplace_back(stat->id);
    stats_map.remove(stat);
}

void BlobStore::BlobStats::eraseStat(const BlobFileId blob_file_id, const std::lock_guard<std::mutex> & lock)
{
    BlobStatPtr stat = nullptr;
    bool found = false;

    for (auto & stat_in_map : stats_map)
    {
        stat = stat_in_map;
        if (stat->id == blob_file_id)
        {
            found = true;
            break;
        }
    }

    if (!found)
    {
        LOG_FMT_ERROR(log, "No exist BlobStat [BlobFileId={}]", blob_file_id);
        return;
    }

    LOG_FMT_DEBUG(log, "Erase BlobStat from maps [BlobFileId={}]", blob_file_id);

    eraseStat(std::move(stat), lock);
}

BlobFileId BlobStore::BlobStats::chooseNewStat()
{
    /**
     * If we do have any `old blob id` which may removed by GC.
     * Then we should get a `old blob id` rather than create a new blob id.
     * If `old_ids` is empty , we will use the `roll_id` as the new 
     * id return. After roll_id generate a `BlobStat`, it will been `++`.
     */
    if (old_ids.empty())
    {
        return roll_id;
    }

    auto rv = old_ids.front();
    old_ids.pop_front();
    return rv;
}

std::pair<BlobStatPtr, BlobFileId> BlobStore::BlobStats::chooseStat(size_t buf_size, UInt64 file_limit_size, const std::lock_guard<std::mutex> &)
{
    BlobStatPtr stat_ptr = nullptr;
    double smallest_valid_rate = 2;

    // No stats exist
    if (stats_map.empty())
    {
        return std::make_pair(nullptr, chooseNewStat());
    }

    for (const auto & stat : stats_map)
    {
        if (!stat->isReadOnly()
            && stat->sm_max_caps >= buf_size
            && stat->sm_total_size + buf_size < file_limit_size
            && stat->sm_valid_rate < smallest_valid_rate)
        {
            smallest_valid_rate = stat->sm_valid_size;
            stat_ptr = stat;
        }
    }

    if (!stat_ptr)
    {
        return std::make_pair(nullptr, chooseNewStat());
    }

    return std::make_pair(stat_ptr, INVALID_BLOBFILE_ID);
}

BlobFileOffset BlobStore::BlobStats::BlobStat::getPosFromStat(size_t buf_size)
{
    BlobFileOffset offset = 0;
    UInt64 max_cap = 0;

    std::tie(offset, max_cap) = smap->searchInsertOffset(buf_size);

    /**
     * Whatever `searchInsertOffset` success or failed,
     * Max capability still need update.
     */
    sm_max_caps = max_cap;
    if (offset != INVALID_BLOBFILE_OFFSET)
    {
        if (offset + buf_size > sm_total_size)
        {
            // This file must be expanded
            auto expand_size = buf_size - (sm_total_size - offset);
            sm_total_size += expand_size;
            sm_valid_size += buf_size;
        }
        else
        {
            /**
             * The `offset` reuses the original address. 
             * Current blob file is not expanded.
             * Only update valid size.
             */
            sm_valid_size += buf_size;
        }

        sm_valid_rate = sm_valid_size * 1.0 / sm_total_size;
    }
    return offset;
}

void BlobStore::BlobStats::BlobStat::removePosFromStat(BlobFileOffset offset, size_t buf_size)
{
    if (!smap->markFree(offset, buf_size))
    {
        smap->logStats();
        throw Exception(fmt::format("Remove postion from BlobStat failed, [offset={} , buf_size={}, BlobFileId={}] is invalid.",
                                    offset,
                                    buf_size,
                                    id),
                        ErrorCodes::LOGICAL_ERROR);
    }

    sm_valid_size -= buf_size;
    sm_valid_rate = sm_valid_size * 1.0 / sm_total_size;
}

BlobStatPtr BlobStore::BlobStats::fileIdToStat(BlobFileId file_id)
{
    auto guard = lock();
    for (auto & stat : stats_map)
    {
        if (stat->id == file_id)
        {
            return stat;
        }
    }

    throw Exception(fmt::format("Can't find BlobStat with [BlobFileId={}]",
                                file_id),
                    ErrorCodes::LOGICAL_ERROR);
}

} // namespace PS::V3
} // namespace DB
