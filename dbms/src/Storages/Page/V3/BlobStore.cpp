#include <Common/Checksum.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Storages/Page/V3/BlobStore.h>

#include <ext/scope_guard.h>

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
        case WriteBatch::WriteType::UPSERT:
        {
            ChecksumClass digest;
            PageEntryV3 entry;

            write.read_buffer->readStrict(buffer_pos, write.size);

            entry.file_id = blob_id;
            entry.size = write.size;
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
            break;
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

void BlobStore::remove(std::list<PageEntryV3> del_entries)
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

    {
        auto lock_stats = blob_stats.lock();
        BlobFileId blob_file_id = INVALID_BLOBFILE_ID;
        std::tie(stat, blob_file_id) = blob_stats.chooseStat(size, config.file_limit_size);

        // No valid stat for puting data with `size`, create a new one
        if (stat == nullptr)
        {
            stat = blob_stats.createStat(blob_file_id);
        }
    }

    // Get Postion from single stat
    auto lock_stat = blob_stats.statLock(stat);
    BlobFileOffset offset = stat->getPosFromStat(size);

    // Can't insert into this spacemap
    if (offset == INVALID_BLOBFILE_OFFSET)
    {
        stat->smap->logStats();
        throw Exception(fmt::format("Get postion from BlobStat failed, it may caused by `sm_max_caps` is no corrent. [size={}, max_caps={}, BlobFileId={}]",
                                    size,
                                    stat->sm_max_caps,
                                    stat->id),
                        ErrorCodes::LOGICAL_ERROR);
    }

    return std::make_pair(stat->id, offset);
}

void BlobStore::removePosFromStats(BlobFileId blob_id, BlobFileOffset offset, size_t size)
{
    blob_stats.fileIdToStat(blob_id)->removePosFromStat(offset, size);

    // TBD : consider remove the empty file
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


void BlobStore::getGCStats(std::map<BlobFileId, VersionedPageIdAndEntryList> & blob_need_gc)
{
    auto & stats_list = blob_stats.getStats();

    for (auto & stat : stats_list)
    {
        auto lock = blob_stats.statLock(stat);
        auto right_margin = stat->smap->getRightMargin();

        stat->sm_valid_rate = stat->sm_valid_size * 1.0 / right_margin;
        assert(stat->sm_valid_rate <= 1.0);

        // Check if GC is required
        if (stat->sm_valid_rate <= 0.2)
        {
            LOG_FMT_DEBUG(log, "Current [BlobFileId={}] valid rate is {}, Need do compact GC", stat->id, stat->sm_valid_rate);
            stat->sm_total_size = stat->sm_valid_size;
            VersionedPageIdAndEntryList empty_list;
            blob_need_gc[stat->id] = empty_list;

            // Change current stat to read only
            stat->changeToReadOnly();
        }
        else
        {
            LOG_FMT_DEBUG(log, "Current [BlobFileId={}] valid rate is {}, No need to GC.", stat->id, stat->sm_valid_rate);
        }

        if (right_margin != stat->sm_total_size)
        {
            auto blobfile = getBlobFile(stat->id);
            blobfile->truncate(right_margin);
            stat->sm_total_size = right_margin;
        }
    }
}

std::list<std::pair<PageEntryV3, VersionedPageIdAndEntry>> BlobStore::gc(std::map<BlobFileId, VersionedPageIdAndEntryList> & entries_need_gc,
                                                                         PageSize & total_page_size)
{
    std::list<std::pair<PageEntryV3, VersionedPageIdAndEntry>> copy_list;

    PageEntriesEdit edit;

    if (total_page_size == 0)
    {
        throw Exception("BlobStore can't do gc if nothing need gc.", ErrorCodes::LOGICAL_ERROR);
    }

    // TBD : consider wheather total_page_size > 512M
    char * data_buf = static_cast<char *>(alloc(total_page_size));
    SCOPE_EXIT({
        free(data_buf, total_page_size);
    });

    char * data_pos = data_buf;
    const auto & [blobfile_id, offset_in_file] = getPosFromStats(total_page_size);
    UInt64 offset_in_data = 0;

    for (const auto & [file_id, versioned_pageid_entry_list] : entries_need_gc)
    {
        for (auto versioned_pageid_entry_it = versioned_pageid_entry_list.begin();
             versioned_pageid_entry_it != versioned_pageid_entry_list.end();
             versioned_pageid_entry_it++)
        {
            VersionedPageIdAndEntry versioned_pageid_entry = *versioned_pageid_entry_it;
            PageEntryV3 new_entry;
            PageEntryV3 entry = std::get<2>(versioned_pageid_entry);

            read(file_id, entry.offset, data_pos, entry.size);

            // No need do crc again, crc won't be changed.
            new_entry.checksum = entry.checksum;

            // Need copy the field_offsets
            new_entry.field_offsets = entry.field_offsets;

            // Entry size won't be changed.
            new_entry.size = entry.size;

            new_entry.file_id = blobfile_id;
            new_entry.offset = offset_in_data;

            offset_in_data += new_entry.size;
            data_pos += new_entry.size;

            copy_list.emplace_back(std::make_pair(std::move(new_entry), std::move(versioned_pageid_entry)));
        }
    }

    if (unlikely(data_pos != data_buf + total_page_size))
    {
        removePosFromStats(blobfile_id, offset_in_file, total_page_size);
        throw Exception(fmt::format("[end_position={}] not match the [current_position={}]",
                                    data_buf + total_page_size,
                                    data_pos),
                        ErrorCodes::LOGICAL_ERROR);
    }

    try
    {
        auto blob_file = getBlobFile(blobfile_id);
        blob_file->write(data_buf, offset_in_file, total_page_size, nullptr);
    }
    catch (DB::Exception & e)
    {
        removePosFromStats(blobfile_id, offset_in_file, total_page_size);
        LOG_FMT_ERROR(log, "[Blobid={}, offset_in_file={}, size={}] write failed.", blobfile_id, offset_in_file, total_page_size);
        throw e;
    }

    return copy_list;
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

std::lock_guard<std::mutex> BlobStore::BlobStats::lock()
{
    return std::lock_guard(lock_stats);
}

std::lock_guard<std::mutex> BlobStore::BlobStats::statLock(BlobStatPtr stat)
{
    return std::lock_guard(stat->sm_lock);
}


BlobStatPtr BlobStore::BlobStats::createStat(BlobFileId blob_file_id)
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

void BlobStore::BlobStats::eraseStat(BlobFileId blob_file_id)
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

    stats_map.remove(stat);
    old_ids.emplace_back(blob_file_id);
}

std::pair<BlobStatPtr, BlobFileId> BlobStore::BlobStats::chooseNewStat()
{
    BlobStatPtr stat_ptr = nullptr;

    /**
     * If we do have any `old blob id` which may removed by GC.
     * Then we should get a `old blob id` rather than create a new blob id.
     * If `old_ids` is empty , we will use the `roll_id` as the new 
     * id return. After roll_id generate a `BlobStat`, it will been `++`.
     */
    if (old_ids.empty())
    {
        return std::make_pair(stat_ptr, roll_id);
    }

    auto rv = std::make_pair(stat_ptr, old_ids.front());
    old_ids.pop_front();
    return rv;
}

std::pair<BlobStatPtr, BlobFileId> BlobStore::BlobStats::chooseStat(size_t buf_size, UInt64 file_limit_size)
{
    BlobStatPtr stat_ptr = nullptr;
    BlobFileId smallest_valid_rate = 2;

    do
    {
        // No stats exist
        if (stats_map.empty())
        {
            break;
        }

        for (const auto & stat : stats_map)
        {
            if (stat->type == BlobStatType::NORMAL
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
            break;
        }

        return std::make_pair(stat_ptr, INVALID_BLOBFILE_ID);

    } while (false);

    return chooseNewStat();
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
