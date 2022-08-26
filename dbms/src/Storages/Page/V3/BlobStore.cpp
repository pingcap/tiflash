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
    BlobFileId blob_id;
    BlobFileOffset offset_in_file;
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


String BlobStore::getBlobFilePath(BlobFileId blob_id) const
{
<<<<<<< HEAD
    return path + "/blobfile_" + DB::toString(blob_id);
=======
    String toString() const
    {
        return fmt::format("{}. {}. {}. {}.",
                           toTypeString("Read-Only Blob", 0),
                           toTypeString("No GC Blob", 1),
                           toTypeString("Full GC Blob", 2),
                           toTypeTruncateString("Truncated Blob"));
    }

    void appendToReadOnlyBlob(const BlobFileId blob_id, double valid_rate)
    {
        blob_gc_info[0].emplace_back(std::make_pair(blob_id, valid_rate));
    }

    void appendToNoNeedGCBlob(const BlobFileId blob_id, double valid_rate)
    {
        blob_gc_info[1].emplace_back(std::make_pair(blob_id, valid_rate));
    }

    void appendToNeedGCBlob(const BlobFileId blob_id, double valid_rate)
    {
        blob_gc_info[2].emplace_back(std::make_pair(blob_id, valid_rate));
    }

    void appendToTruncatedBlob(const BlobFileId blob_id, UInt64 origin_size, UInt64 truncated_size, double valid_rate)
    {
        blob_gc_truncate_info.emplace_back(std::make_tuple(blob_id, origin_size, truncated_size, valid_rate));
    }

private:
    // 1. read only blob
    // 2. no need gc blob
    // 3. full gc blob
    std::vector<std::pair<BlobFileId, double>> blob_gc_info[3];

    std::vector<std::tuple<BlobFileId, UInt64, UInt64, double>> blob_gc_truncate_info;

    String toTypeString(const std::string_view prefix, const size_t index) const
    {
        FmtBuffer fmt_buf;

        if (blob_gc_info[index].empty())
        {
            fmt_buf.fmtAppend("{}: [null]", prefix);
        }
        else
        {
            fmt_buf.fmtAppend("{}: [", prefix);
            fmt_buf.joinStr(
                blob_gc_info[index].begin(),
                blob_gc_info[index].end(),
                [](const auto arg, FmtBuffer & fb) {
                    fb.fmtAppend("{}/{:.2f}", arg.first, arg.second);
                },
                ", ");
            fmt_buf.append("]");
        }

        return fmt_buf.toString();
    }

    String toTypeTruncateString(const std::string_view prefix) const
    {
        FmtBuffer fmt_buf;
        if (blob_gc_truncate_info.empty())
        {
            fmt_buf.fmtAppend("{}: [null]", prefix);
        }
        else
        {
            fmt_buf.fmtAppend("{}: [", prefix);
            fmt_buf.joinStr(
                blob_gc_truncate_info.begin(),
                blob_gc_truncate_info.end(),
                [](const auto arg, FmtBuffer & fb) {
                    fb.fmtAppend("{} origin: {} truncate: {} rate: {:.2f}", //
                                 std::get<0>(arg), // blob id
                                 std::get<1>(arg), // origin size
                                 std::get<2>(arg), // truncated size
                                 std::get<3>(arg)); // valid rate
                },
                ", ");
            fmt_buf.append("]");
        }
        return fmt_buf.toString();
    }
};

std::vector<BlobFileId> BlobStore::getGCStats()
{
    // Get a copy of stats map to avoid the big lock on stats map
    const auto stats_list = blob_stats.getStats();
    std::vector<BlobFileId> blob_need_gc;
    BlobStoreGCInfo blobstore_gc_info;

    fiu_do_on(FailPoints::force_change_all_blobs_to_read_only,
              {
                  for (const auto & [path, stats] : stats_list)
                  {
                      (void)path;
                      for (const auto & stat : stats)
                      {
                          stat->changeToReadOnly();
                      }
                  }
                  LOG_FMT_WARNING(log, "enabled force_change_all_blobs_to_read_only. All of BlobStat turn to READ-ONLY");
              });

    for (const auto & [path, stats] : stats_list)
    {
        (void)path;
        for (const auto & stat : stats)
        {
            if (stat->isReadOnly())
            {
                blobstore_gc_info.appendToReadOnlyBlob(stat->id, stat->sm_valid_rate);
                LOG_FMT_TRACE(log, "Current [blob_id={}] is read-only", stat->id);
                continue;
            }

            auto lock = stat->lock();
            auto right_margin = stat->smap->getUsedBoundary();

            // Avoid divide by zero
            if (right_margin == 0)
            {
                // Note `stat->sm_total_size` isn't strictly the same as the actual size of underlying BlobFile after restart tiflash,
                // because some entry may be deleted but the actual disk space is not reclaimed in previous run.
                // TODO: avoid always truncate on empty BlobFile
                RUNTIME_CHECK_MSG(stat->sm_valid_size == 0, "Current blob is empty, but valid size is not 0. [blob_id={}] [valid_size={}] [valid_rate={}]", stat->id, stat->sm_valid_size, stat->sm_valid_rate);

                // If current blob empty, the size of in disk blob may not empty
                // So we need truncate current blob, and let it be reused.
                auto blobfile = getBlobFile(stat->id);
                LOG_FMT_INFO(log, "Current blob file is empty, truncated to zero [blob_id={}] [total_size={}] [valid_rate={}]", stat->id, stat->sm_total_size, stat->sm_valid_rate);
                blobfile->truncate(right_margin);
                blobstore_gc_info.appendToTruncatedBlob(stat->id, stat->sm_total_size, right_margin, stat->sm_valid_rate);
                stat->sm_total_size = right_margin;
                continue;
            }

            stat->sm_valid_rate = stat->sm_valid_size * 1.0 / right_margin;

            if (stat->sm_valid_rate > 1.0)
            {
                LOG_FMT_ERROR(
                    log,
                    "Current blob got an invalid rate {:.2f}, total size is {}, valid size is {}, right margin is {} [blob_id={}]",
                    stat->sm_valid_rate,
                    stat->sm_total_size,
                    stat->sm_valid_size,
                    right_margin,
                    stat->id);
                assert(false);
                continue;
            }

            // Check if GC is required
            if (stat->sm_valid_rate <= config.heavy_gc_valid_rate)
            {
                LOG_FMT_TRACE(log, "Current [blob_id={}] valid rate is {:.2f}, Need do compact GC", stat->id, stat->sm_valid_rate);
                blob_need_gc.emplace_back(stat->id);

                // Change current stat to read only
                stat->changeToReadOnly();
                blobstore_gc_info.appendToNeedGCBlob(stat->id, stat->sm_valid_rate);
            }
            else
            {
                blobstore_gc_info.appendToNoNeedGCBlob(stat->id, stat->sm_valid_rate);
                LOG_FMT_TRACE(log, "Current [blob_id={}] valid rate is {:.2f}, No need to GC.", stat->id, stat->sm_valid_rate);
            }

            if (right_margin != stat->sm_total_size)
            {
                auto blobfile = getBlobFile(stat->id);
                LOG_FMT_TRACE(log, "Truncate blob file [blob_id={}] [origin size={}] [truncated size={}]", stat->id, stat->sm_total_size, right_margin);
                blobfile->truncate(right_margin);
                blobstore_gc_info.appendToTruncatedBlob(stat->id, stat->sm_total_size, right_margin, stat->sm_valid_rate);

                stat->sm_total_size = right_margin;
                stat->sm_valid_rate = stat->sm_valid_size * 1.0 / stat->sm_total_size;
            }
        }
    }

    LOG_FMT_INFO(log, "BlobStore gc get status done. gc info: {}", blobstore_gc_info.toString());

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
    LOG_FMT_INFO(log, "BlobStore gc will migrate {:.2f}MB into new Blobs", (1.0 * total_page_size / DB::MB));

    auto write_blob = [this, total_page_size, &written_blobs, &write_limiter](const BlobFileId & file_id,
                                                                              char * data_begin,
                                                                              const BlobFileOffset & file_offset,
                                                                              const PageSize & data_size) {
        try
        {
            auto blob_file = getBlobFile(file_id);
            // Should append before calling BlobStore::write, so that we can rollback the
            // first allocated span from stats.
            written_blobs.emplace_back(file_id, file_offset, data_size);
            LOG_FMT_INFO(
                log,
                "BlobStore gc write (partially) done [blob_id={}] [file_offset={}] [size={}] [total_size={}]",
                file_id,
                file_offset,
                data_size,
                total_page_size);
            blob_file->write(data_begin, file_offset, data_size, write_limiter, /*background*/ true);
        }
        catch (DB::Exception & e)
        {
            LOG_FMT_ERROR(
                log,
                "BlobStore gc write failed [blob_id={}] [offset={}] [size={}] [total_size={}]",
                file_id,
                file_offset,
                data_size,
                total_page_size);
            for (const auto & [blobfile_id_revert, file_offset_beg_revert, page_size_revert] : written_blobs)
            {
                removePosFromStats(blobfile_id_revert, file_offset_beg_revert, page_size_revert);
            }
            throw e;
        }
    };

    auto alloc_size = config.file_limit_size.get();
    // If `total_page_size` is greater than `config_file_limit`, we will try to write the page data into multiple `BlobFile`s to
    // make the memory consumption smooth during GC.
    if (total_page_size > alloc_size)
    {
        size_t biggest_page_size = 0;
        for (const auto & [file_id, versioned_pageid_entry_list] : entries_need_gc)
        {
            (void)file_id;
            for (const auto & [page_id, version, entry] : versioned_pageid_entry_list)
            {
                (void)page_id;
                (void)version;
                biggest_page_size = std::max(biggest_page_size, entry.size);
            }
        }
        alloc_size = std::max(alloc_size, biggest_page_size);
    }
    else
    {
        alloc_size = total_page_size;
    }

    BlobFileOffset remaining_page_size = total_page_size - alloc_size;

    char * data_buf = static_cast<char *>(alloc(alloc_size));
    SCOPE_EXIT({
        free(data_buf, alloc_size);
    });

    char * data_pos = data_buf;
    BlobFileOffset offset_in_data = 0;
    BlobFileId blobfile_id;
    BlobFileOffset file_offset_begin;
    std::tie(blobfile_id, file_offset_begin) = getPosFromStats(alloc_size);

    // blob_file_0, [<page_id_0, ver0, entry0>,
    //               <page_id_0, ver1, entry1>,
    //               <page_id_1, ver1, entry1>, ... ]
    // blob_file_1, [...]
    // ...
    for (const auto & [file_id, versioned_pageid_entry_list] : entries_need_gc)
    {
        for (const auto & [page_id, version, entry] : versioned_pageid_entry_list)
        {
            /// If `total_page_size` is greater than `config_file_limit`, we need to write the page data into multiple `BlobFile`s.
            /// So there may be some page entry that cannot be fit into the current blob file, and we need to write it into the next one.
            /// And we need perform the following steps before writing data into the current blob file:
            ///   1. reclaim unneeded space allocated from current blob stat if `offset_in_data` < `alloc_size`;
            ///   2. update `remaining_page_size`;
            /// After writing data into the current blob file, we reuse the original buffer for future write.
            if (offset_in_data + entry.size > alloc_size)
            {
                assert(file_offset_begin == 0);
                // Remove the span that is not actually used
                if (offset_in_data != alloc_size)
                {
                    removePosFromStats(blobfile_id, offset_in_data, alloc_size - offset_in_data);
                }
                remaining_page_size += alloc_size - offset_in_data;

                // Write data into Blob.
                write_blob(blobfile_id, data_buf, file_offset_begin, offset_in_data);

                // Reset the position to reuse the buffer allocated
                data_pos = data_buf;
                offset_in_data = 0;

                // Acquire a span from stats for remaining data
                auto next_alloc_size = (remaining_page_size > alloc_size ? alloc_size : remaining_page_size);
                remaining_page_size -= next_alloc_size;
                std::tie(blobfile_id, file_offset_begin) = getPosFromStats(next_alloc_size);
            }
            assert(offset_in_data + entry.size <= alloc_size);

            // Read the data into buffer by old entry
            read(page_id, file_id, entry.offset, data_pos, entry.size, read_limiter, /*background*/ true);

            // Most vars of the entry is not changed, but the file id and offset
            // need to be updated.
            PageEntryV3 new_entry = entry;
            new_entry.file_id = blobfile_id;
            new_entry.offset = file_offset_begin + offset_in_data;
            new_entry.padded_size = 0; // reset padded size to be zero

            offset_in_data += new_entry.size;
            data_pos += new_entry.size;

            edit.upsertPage(page_id, version, new_entry);
        }
    }

    // write remaining data in `data_buf` into BlobFile
    if (offset_in_data != 0)
    {
        write_blob(blobfile_id, data_buf, file_offset_begin, offset_in_data);
    }

    return edit;
}


String BlobStore::getBlobFileParentPath(BlobFileId blob_id)
{
    PageFileIdAndLevel id_lvl{blob_id, 0};
    String parent_path = delegator->getPageFilePath(id_lvl);

    if (auto f = Poco::File(parent_path); !f.exists())
        f.createDirectories();

    return parent_path;
>>>>>>> b109a12ec8 (avoid init useless DeltaMergeStore (#5705))
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
            if (stat->sm_max_caps >= buf_size
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
    else
    {
        auto rv = std::make_pair(stat_ptr, old_ids.front());
        old_ids.pop_front();
        return rv;
    }
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
