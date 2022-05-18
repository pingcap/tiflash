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

/***************************
  * PageReaderImpl methods *
  **************************/

class PageReaderImpl : private boost::noncopyable
{
public:
    static std::unique_ptr<PageReaderImpl> create(
        PageStorageRunMode run_mode_,
        NamespaceId ns_id_,
        PageStoragePtr storage_v2_,
        PageStoragePtr storage_v3_,
        const PageStorage::SnapshotPtr & snap_,
        ReadLimiterPtr read_limiter_);

    virtual ~PageReaderImpl() = default;

    virtual DB::Page read(PageId page_id) const = 0;

    virtual PageMap read(const PageIds & page_ids) const = 0;

    virtual void read(const PageIds & page_ids, PageHandler & handler) const = 0;

    using PageReadFields = PageStorage::PageReadFields;
    virtual PageMap read(const std::vector<PageReadFields> & page_fields) const = 0;

    virtual PageId getNormalPageId(PageId page_id) const = 0;

    virtual PageEntry getPageEntry(PageId page_id) const = 0;

    virtual PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) const = 0;

    // Get some statistics of all living snapshots and the oldest living snapshot.
    virtual SnapshotsStatistics getSnapshotsStat() const = 0;

    virtual void traverse(const std::function<void(const DB::Page & page)> & acceptor, bool only_v2, bool only_v3) const = 0;
};


class PageReaderImplNormal : public PageReaderImpl
{
public:
    /// Not snapshot read.
    explicit PageReaderImplNormal(NamespaceId ns_id_, PageStoragePtr storage_, ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage(storage_)
        , read_limiter(read_limiter_)
    {
    }

    /// Snapshot read.
    PageReaderImplNormal(NamespaceId ns_id_, PageStoragePtr storage_, const PageStorage::SnapshotPtr & snap_, ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage(storage_)
        , snap(snap_)
        , read_limiter(read_limiter_)
    {
    }

    DB::Page read(PageId page_id) const override
    {
        return storage->read(ns_id, page_id, read_limiter, snap);
    }

    PageMap read(const PageIds & page_ids) const override
    {
        return storage->read(ns_id, page_ids, read_limiter, snap);
    }

    void read(const PageIds & page_ids, PageHandler & handler) const override
    {
        storage->read(ns_id, page_ids, handler, read_limiter, snap);
    }

    using PageReadFields = PageStorage::PageReadFields;
    PageMap read(const std::vector<PageReadFields> & page_fields) const override
    {
        return storage->read(ns_id, page_fields, read_limiter, snap);
    }

    PageId getNormalPageId(PageId page_id) const override
    {
        return storage->getNormalPageId(ns_id, page_id, snap);
    }

    PageEntry getPageEntry(PageId page_id) const override
    {
        return storage->getEntry(ns_id, page_id, snap);
    }

    PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) const override
    {
        return storage->getSnapshot(tracing_id);
    }

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const override
    {
        return storage->getSnapshotsStat();
    }

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, bool /*only_v2*/, bool /*only_v3*/) const override
    {
        storage->traverse(acceptor, nullptr);
    }

private:
    NamespaceId ns_id;
    PageStoragePtr storage;
    PageStorage::SnapshotPtr snap;
    ReadLimiterPtr read_limiter;
};


class PageReaderImplMixed : public PageReaderImpl
{
public:
    /// Not snapshot read.
    explicit PageReaderImplMixed(NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , read_limiter(read_limiter_)
    {
    }

    /// Snapshot read.
    PageReaderImplMixed(NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, const PageStorage::SnapshotPtr & snap_, ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , snap(snap_)
        , read_limiter(read_limiter_)
    {
    }

    PageReaderImplMixed(NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, PageStorage::SnapshotPtr && snap_, ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , snap(std::move(snap_))
        , read_limiter(read_limiter_)
    {
    }

    DB::Page read(PageId page_id) const override
    {
        const auto & page_from_v3 = storage_v3->read(ns_id, page_id, read_limiter, toConcreteV3Snapshot(), false);
        if (page_from_v3.isValid())
        {
            return page_from_v3;
        }
        return storage_v2->read(ns_id, page_id, read_limiter, toConcreteV2Snapshot());
    }

    PageMap read(const PageIds & page_ids) const override
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

    void read(const PageIds & page_ids, PageHandler & handler) const override
    {
        const auto & page_ids_not_found = storage_v3->read(ns_id, page_ids, handler, read_limiter, toConcreteV3Snapshot(), false);
        storage_v2->read(ns_id, page_ids_not_found, handler, read_limiter, toConcreteV2Snapshot());
    }

    using PageReadFields = PageStorage::PageReadFields;
    PageMap read(const std::vector<PageReadFields> & page_fields) const override
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
            for (const auto & page_field : invalid_page_fields)
            {
                page_maps[page_field.first] = page_maps_from_v2[page_field.first];
            }
        }

        return page_maps;
    }

    PageId getNormalPageId(PageId page_id) const override
    {
        PageId resolved_page_id = storage_v3->getNormalPageId(ns_id, page_id, toConcreteV3Snapshot(), false);
        if (resolved_page_id != INVALID_PAGE_ID)
        {
            return resolved_page_id;
        }
        return storage_v2->getNormalPageId(ns_id, page_id, toConcreteV2Snapshot());
    }

    PageEntry getPageEntry(PageId page_id) const override
    {
        PageEntry page_entry = storage_v3->getEntry(ns_id, page_id, toConcreteV3Snapshot());
        if (page_entry.file_id != INVALID_BLOBFILE_ID)
        {
            return page_entry;
        }
        return storage_v2->getEntry(ns_id, page_id, toConcreteV2Snapshot());
    }

    PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) const override
    {
        return std::make_shared<PageStorageSnapshotMixed>(
            storage_v2->getSnapshot(fmt::format("{}-v2", tracing_id)),
            storage_v3->getSnapshot(fmt::format("{}-v3", tracing_id)));
    }

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const override
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

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, bool only_v2, bool only_v3) const override
    {
        // Used by RegionPersister::restore
        // Must traverse storage_v3 before storage_v2
        if (only_v3 && only_v2)
        {
            throw Exception("Can't enable both only_v2 and only_v3", ErrorCodes::LOGICAL_ERROR);
        }

        if (only_v3)
        {
            storage_v3->traverse(acceptor, toConcreteV3Snapshot());
        }
        else if (only_v2)
        {
            storage_v2->traverse(acceptor, toConcreteV2Snapshot());
        }
        else
        {
            // Used by RegionPersister::restore
            // Must traverse storage_v3 before storage_v2
            storage_v3->traverse(acceptor, toConcreteV3Snapshot());
            storage_v2->traverse(acceptor, toConcreteV2Snapshot());
        }
    }

private:
    PageStorage::SnapshotPtr toConcreteV3Snapshot() const
    {
        return snap ? toConcreteMixedSnapshot(snap)->getV3Snapshot() : snap;
    }

    PageStorage::SnapshotPtr toConcreteV2Snapshot() const
    {
        return snap ? toConcreteMixedSnapshot(snap)->getV2Snapshot() : snap;
    }

private:
    const NamespaceId ns_id;
    PageStoragePtr storage_v2;
    PageStoragePtr storage_v3;
    PageStorage::SnapshotPtr snap;
    ReadLimiterPtr read_limiter;
};

std::unique_ptr<PageReaderImpl> PageReaderImpl::create(
    PageStorageRunMode run_mode_,
    NamespaceId ns_id_,
    PageStoragePtr storage_v2_,
    PageStoragePtr storage_v3_,
    const PageStorage::SnapshotPtr & snap_,
    ReadLimiterPtr read_limiter_)
{
    switch (run_mode_)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        return std::make_unique<PageReaderImplNormal>(ns_id_, storage_v2_, snap_, read_limiter_);
    }
    case PageStorageRunMode::ONLY_V3:
    {
        return std::make_unique<PageReaderImplNormal>(ns_id_, storage_v3_, snap_, read_limiter_);
    }
    case PageStorageRunMode::MIX_MODE:
    {
        return std::make_unique<PageReaderImplMixed>(ns_id_, storage_v2_, storage_v3_, snap_, read_limiter_);
    }
    default:
        throw Exception(fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode_)), ErrorCodes::LOGICAL_ERROR);
    }
}

/***********************
  * PageReader methods *
  **********************/
/// Not snapshot read.
PageReader::PageReader(const PageStorageRunMode & run_mode_, NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, ReadLimiterPtr read_limiter_)
    : impl(PageReaderImpl::create(run_mode_, ns_id_, storage_v2_, storage_v3_, /*snap_=*/nullptr, read_limiter_))
{
}

/// Snapshot read.
PageReader::PageReader(const PageStorageRunMode & run_mode_, NamespaceId ns_id_, PageStoragePtr storage_v2_, PageStoragePtr storage_v3_, PageStorage::SnapshotPtr snap_, ReadLimiterPtr read_limiter_)
    : impl(PageReaderImpl::create(run_mode_, ns_id_, storage_v2_, storage_v3_, std::move(snap_), read_limiter_))
{
}

PageReader::~PageReader() = default;

DB::Page PageReader::read(PageId page_id) const
{
    return impl->read(page_id);
}

PageMap PageReader::read(const PageIds & page_ids) const
{
    return impl->read(page_ids);
}

void PageReader::read(const PageIds & page_ids, PageHandler & handler) const
{
    impl->read(page_ids, handler);
}

PageMap PageReader::read(const std::vector<PageStorage::PageReadFields> & page_fields) const
{
    return impl->read(page_fields);
}

PageId PageReader::getNormalPageId(PageId page_id) const
{
    return impl->getNormalPageId(page_id);
}

PageEntry PageReader::getPageEntry(PageId page_id) const
{
    return impl->getPageEntry(page_id);
}

PageStorage::SnapshotPtr PageReader::getSnapshot(const String & tracing_id) const
{
    return impl->getSnapshot(tracing_id);
}

// Get some statistics of all living snapshots and the oldest living snapshot.
SnapshotsStatistics PageReader::getSnapshotsStat() const
{
    return impl->getSnapshotsStat();
}

void PageReader::traverse(const std::function<void(const DB::Page & page)> & acceptor, bool only_v2, bool only_v3) const
{
    impl->traverse(acceptor, only_v2, only_v3);
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
                if (entry_for_put.isValid())
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

                    LOG_FMT_INFO(
                        Logger::get("PageWriter"),
                        "Can't find the origin page in v3, migrate a new being ref page into V3 [page_id={}] [origin_id={}] [field_offsets={}]",
                        write.page_id,
                        write.ori_page_id,
                        entry_for_put.field_offsets.size());
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
