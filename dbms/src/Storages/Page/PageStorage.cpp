// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>

namespace DB
{
PageStoragePtr PageStorage::create(
    String name,
    PSDiskDelegatorPtr delegator,
    const PageStorage::Config & config,
    const FileProviderPtr & file_provider,
    bool use_v3)
{
    if (use_v3)
        return std::make_shared<PS::V3::PageStorageImpl>(name, delegator, config, file_provider);
    else
        return std::make_shared<PS::V2::PageStorage>(name, delegator, config, file_provider);
}

/**********************
  * PageReader methods *
  *********************/

PageStorage::SnapshotPtr PageReader::toConcreteV3Snapshot() const
{
    return snap ? toConcreteMixedSnapshot(snap)->getV3Snasphot() : snap;
}

PageStorage::SnapshotPtr PageReader::toConcreteV2Snapshot() const
{
    return snap ? toConcreteMixedSnapshot(snap)->getV2Snasphot() : snap;
}

DB::Page PageReader::read(PageId page_id) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return storage_v2->read(ns_id, page_id, read_limiter, snap);
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return storage_v3->read(ns_id, page_id, read_limiter, snap);
    }
    case PageStorageRunMode::MIX_MODE:
    {
        const auto & page_from_v3 = storage_v3->read(ns_id, page_id, read_limiter, toConcreteV3Snapshot(), false);
        if (page_from_v3.isValid())
        {
            return page_from_v3;
        }
        return storage_v2->read(ns_id, page_id, read_limiter, toConcreteV2Snapshot());
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

PageMap PageReader::read(const PageIds & page_ids) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return storage_v2->read(ns_id, page_ids, read_limiter, snap);
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return storage_v3->read(ns_id, page_ids, read_limiter, snap);
    }
    case PageStorageRunMode::MIX_MODE:
    {
        auto page_maps = storage_v3->read(ns_id, page_ids, read_limiter, toConcreteV3Snapshot(), false);
        PageIds invalid_page_ids;
        for (const auto & [query_page_id, page] : page_maps)
        {
            if (!page.isValid())
            {
                invalid_page_ids.emplace_back(query_page_id);
            }
        }

        if (!invalid_page_ids.empty())
        {
            const auto & page_maps_from_v2 = storage_v2->read(ns_id, invalid_page_ids, read_limiter, toConcreteV2Snapshot());
            for (const auto & [page_id_, page_] : page_maps_from_v2)
            {
                page_maps[page_id_] = page_;
            }
        }

        return page_maps;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}


void PageReader::read(const PageIds & page_ids, PageHandler & handler) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        storage_v2->read(ns_id, page_ids, handler, read_limiter, snap);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        storage_v3->read(ns_id, page_ids, handler, read_limiter, snap);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        const auto & page_ids_not_found = storage_v3->read(ns_id, page_ids, handler, read_limiter, toConcreteV3Snapshot(), false);
        storage_v2->read(ns_id, page_ids_not_found, handler, read_limiter, toConcreteV2Snapshot());
        break;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

PageMap PageReader::read(const std::vector<PageReadFields> & page_fields) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return storage_v2->read(ns_id, page_fields, read_limiter, snap);
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return storage_v3->read(ns_id, page_fields, read_limiter, snap);
    }
    case PageStorageRunMode::MIX_MODE:
    {
        auto page_maps = storage_v3->read(ns_id, page_fields, read_limiter, toConcreteV3Snapshot(), false);

        std::vector<PageReadFields> invalid_page_fields;

        for (const auto & page_field : page_fields)
        {
            if (!page_maps[page_field.first].isValid())
            {
                invalid_page_fields.emplace_back(page_field);
            }
        }

        if (!invalid_page_fields.empty())
        {
            auto page_maps_from_v2 = storage_v2->read(ns_id, invalid_page_fields, read_limiter, toConcreteV2Snapshot());
            for (const auto & page_field_ : invalid_page_fields)
            {
                page_maps[page_field_.first] = page_maps_from_v2[page_field_.first];
            }
        }

        return page_maps;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

PageId PageReader::getNormalPageId(PageId page_id) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return storage_v2->getNormalPageId(ns_id, page_id, snap);
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return storage_v3->getNormalPageId(ns_id, page_id, snap);
    }
    case PageStorageRunMode::MIX_MODE:
    {
        PageId resolved_page_id = storage_v3->getNormalPageId(ns_id, page_id, toConcreteV3Snapshot(), false);
        if (resolved_page_id != INVALID_PAGE_ID)
        {
            return resolved_page_id;
        }
        return storage_v2->getNormalPageId(ns_id, page_id, toConcreteV2Snapshot());
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

UInt64 PageReader::getPageChecksum(PageId page_id) const
{
    return getPageEntry(page_id).checksum;
}

PageEntry PageReader::getPageEntry(PageId page_id) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return storage_v2->getEntry(ns_id, page_id, snap);
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return storage_v3->getEntry(ns_id, page_id, snap);
    }
    case PageStorageRunMode::MIX_MODE:
    {
        PageEntry page_entry = storage_v3->getEntry(ns_id, page_id, toConcreteV3Snapshot());
        if (page_entry.file_id != INVALID_BLOBFILE_ID)
        {
            return page_entry;
        }
        return storage_v2->getEntry(ns_id, page_id, toConcreteV2Snapshot());
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

PageStorage::SnapshotPtr PageReader::getSnapshot(const String & tracing_id) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return storage_v2->getSnapshot(tracing_id);
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return storage_v3->getSnapshot(tracing_id);
    }
    case PageStorageRunMode::MIX_MODE:
    {
        return std::make_shared<PageStorageSnapshotMixed>(storage_v2->getSnapshot(fmt::format("{}-v2", tracing_id)), //
                                                          storage_v3->getSnapshot(fmt::format("{}-v3", tracing_id)));
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}


SnapshotsStatistics PageReader::getSnapshotsStat() const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return storage_v2->getSnapshotsStat();
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return storage_v3->getSnapshotsStat();
    }
    case PageStorageRunMode::MIX_MODE:
    {
        SnapshotsStatistics statistics_total;
        const auto & statistics_from_v2 = storage_v2->getSnapshotsStat();
        const auto & statistics_from_v3 = storage_v3->getSnapshotsStat();

        statistics_total.num_snapshots = statistics_from_v2.num_snapshots + statistics_from_v3.num_snapshots;
        if (statistics_from_v2.longest_living_seconds > statistics_from_v3.longest_living_seconds)
        {
            statistics_total.longest_living_seconds = statistics_from_v2.longest_living_seconds;
            statistics_total.longest_living_from_thread_id = statistics_from_v2.longest_living_from_thread_id;
            statistics_total.longest_living_from_tracing_id = statistics_from_v2.longest_living_from_tracing_id;
        }
        else
        {
            statistics_total.longest_living_seconds = statistics_from_v3.longest_living_seconds;
            statistics_total.longest_living_from_thread_id = statistics_from_v3.longest_living_from_thread_id;
            statistics_total.longest_living_from_tracing_id = statistics_from_v3.longest_living_from_tracing_id;
        }

        return statistics_total;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

void PageReader::traverse(const std::function<void(const DB::Page & page)> & acceptor) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        storage_v2->traverse(acceptor, nullptr);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        storage_v3->traverse(acceptor, nullptr);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        // Used by RegionPersister::restore
        // Must traverse storage_v3 before storage_v2
        storage_v3->traverse(acceptor, toConcreteV3Snapshot());
        storage_v2->traverse(acceptor, toConcreteV2Snapshot());
        break;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}


/**********************
  * PageWriter methods *
  *********************/

void PageWriter::write(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        writeIntoV2(std::move(write_batch), write_limiter);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        writeIntoV3(std::move(write_batch), write_limiter);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        writeIntoMixMode(std::move(write_batch), write_limiter);
        break;
    }
    }
}


void PageWriter::writeIntoV2(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const
{
    storage_v2->write(std::move(write_batch), write_limiter);
}

void PageWriter::writeIntoV3(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const
{
    storage_v3->write(std::move(write_batch), write_limiter);
}

void PageWriter::writeIntoMixMode(WriteBatch && write_batch, WriteLimiterPtr write_limiter) const
{
    const auto & ns_id = write_batch.getNamespaceId();
    WriteBatch wb_for_v2{ns_id};
    WriteBatch wb_for_put_v3{ns_id};

    // If we do need copy entry from V2 into V3
    // We need hold mem from V2 pages after write.
    std::list<MemHolder> mem_holders;

    for (const auto & write : write_batch.getWrites())
    {
        switch (write.type)
        {
        // PUT/PUT_EXTERNAL only for V3
        case WriteBatch::WriteType::PUT:
        case WriteBatch::WriteType::PUT_EXTERNAL:
        {
            break;
        }
        // Both need del in v2 and v3
        case WriteBatch::WriteType::DEL:
        {
            wb_for_v2.copyWrite(write);
            break;
        }
        case WriteBatch::WriteType::REF:
        {
            PageId resolved_page_id = storage_v3->getNormalPageId(ns_id,
                                                                  write.ori_page_id,
                                                                  /*snapshot*/ nullptr,
                                                                  false);
            // If the normal id is not found in v3, read from v2 and create a new put + ref
            if (resolved_page_id == INVALID_PAGE_ID)
            {
                const auto & entry_for_put = storage_v2->getEntry(ns_id, write.ori_page_id, /*snapshot*/ {});
                if (entry_for_put.file_id != 0)
                {
                    auto page_for_put = storage_v2->read(ns_id, write.ori_page_id);

                    // Keep the mem hold, no need create new one.
                    mem_holders.emplace_back(page_for_put.mem_holder);
                    assert(entry_for_put.size == page_for_put.data.size());

                    // Page with fields
                    if (!entry_for_put.field_offsets.empty())
                    {
                        wb_for_put_v3.putPage(write.ori_page_id, //
                                              entry_for_put.tag,
                                              std::make_shared<ReadBufferFromMemory>(page_for_put.data.begin(), page_for_put.data.size()),
                                              page_for_put.data.size(),
                                              Page::fieldOffsetsToSizes(entry_for_put.field_offsets, entry_for_put.size));
                    }
                    else
                    { // Normal page with fields
                        wb_for_put_v3.putPage(write.ori_page_id, //
                                              entry_for_put.tag,
                                              std::make_shared<ReadBufferFromMemory>(page_for_put.data.begin(),
                                                                                     page_for_put.data.size()),
                                              page_for_put.data.size());
                    }

                    LOG_FMT_INFO(Logger::get("PageWriter"), "Can't find [origin_id={}] in v3. Created a new page with [field_offsets={}] into V3", write.ori_page_id, entry_for_put.field_offsets.size());
                }
                else
                {
                    throw Exception(fmt::format("Can't find origin entry in V2 and V3, [ns_id={}, ori_page_id={}]",
                                                ns_id,
                                                write.ori_page_id),
                                    ErrorCodes::LOGICAL_ERROR);
                }
            }
            // else V3 found the origin one.
            // Then do nothing.
            break;
        }
        default:
        {
            throw Exception(fmt::format("Unknown write type: {}", write.type));
        }
        }
    }

    if (!wb_for_put_v3.empty())
    {
        // The `writes` in wb_for_put_v3 must come before the `writes` in write_batch
        wb_for_put_v3.copyWrites(write_batch.getWrites());
        storage_v3->write(std::move(wb_for_put_v3), write_limiter);
    }
    else
    {
        storage_v3->write(std::move(write_batch), write_limiter);
    }

    if (!wb_for_v2.empty())
    {
        storage_v2->write(std::move(wb_for_v2), write_limiter);
    }
}


PageStorage::Config PageWriter::getSettings() const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return storage_v2->getSettings();
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return storage_v3->getSettings();
    }
    case PageStorageRunMode::MIX_MODE:
    {
        throw Exception("Not support.", ErrorCodes::NOT_IMPLEMENTED);
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

void PageWriter::reloadSettings(const PageStorage::Config & new_config) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        storage_v2->reloadSettings(new_config);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        storage_v3->reloadSettings(new_config);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        storage_v2->reloadSettings(new_config);
        storage_v3->reloadSettings(new_config);
        break;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
};

bool PageWriter::gc(bool not_skip, const WriteLimiterPtr & write_limiter, const ReadLimiterPtr & read_limiter) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return storage_v2->gc(not_skip, write_limiter, read_limiter);
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return storage_v3->gc(not_skip, write_limiter, read_limiter);
    }
    case PageStorageRunMode::MIX_MODE:
    {
        bool ok = storage_v2->gc(not_skip, write_limiter, read_limiter);
        ok |= storage_v3->gc(not_skip, write_limiter, read_limiter);
        return ok;
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)), ErrorCodes::LOGICAL_ERROR);
    }
}

} // namespace DB
