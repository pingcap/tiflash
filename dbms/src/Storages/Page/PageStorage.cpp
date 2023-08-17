// Copyright 2023 PingCAP, Inc.
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

#include <Interpreters/Context.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V2/PageStorage.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/Page/WriteBatchWrapperImpl.h>

namespace DB
{
PageStoragePtr PageStorage::create(
    String name,
    PSDiskDelegatorPtr delegator,
    const PageStorageConfig & config,
    const FileProviderPtr & file_provider,
    Context & global_ctx,
    bool use_v3,
    bool no_more_insert_to_v2)
{
    if (use_v3)
        return std::make_shared<PS::V3::PageStorageImpl>(name, delegator, config, file_provider);
    else
        return std::make_shared<PS::V2::PageStorage>(
            name,
            delegator,
            config,
            file_provider,
            global_ctx.getPSBackgroundPool(),
            no_more_insert_to_v2);
}

/***************************
  * PageReaderImpl methods *
  **************************/

class PageReaderImpl : private boost::noncopyable
{
public:
    static std::unique_ptr<PageReaderImpl> create(
        PageStorageRunMode run_mode_,
        KeyspaceID keyspace_id_,
        StorageType tag_,
        NamespaceID ns_id_,
        PageStoragePtr storage_v2_,
        PageStoragePtr storage_v3_,
        UniversalPageStoragePtr uni_ps_,
        const PageStorage::SnapshotPtr & snap_,
        ReadLimiterPtr read_limiter_);

    virtual ~PageReaderImpl() = default;

    virtual DB::Page read(PageIdU64 page_id) const = 0;

    virtual PageMapU64 read(const PageIdU64s & page_ids) const = 0;

    using PageReadFields = PageStorage::PageReadFields;
    virtual PageMapU64 read(const std::vector<PageReadFields> & page_fields) const = 0;

    virtual PageIdU64 getNormalPageId(PageIdU64 page_id) const = 0;

    virtual PageEntry getPageEntry(PageIdU64 page_id) const = 0;

    virtual PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) const = 0;

    // Get some statistics of all living snapshots and the oldest living snapshot.
    virtual SnapshotsStatistics getSnapshotsStat() const = 0;

    virtual FileUsageStatistics getFileUsageStatistics() const = 0;

    virtual void traverse(const std::function<void(const DB::Page & page)> & acceptor, bool only_v2, bool only_v3)
        const = 0;
};


class PageReaderImplNormal : public PageReaderImpl
{
public:
    /// Not snapshot read.
    explicit PageReaderImplNormal(NamespaceID ns_id_, PageStoragePtr storage_, ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage(storage_)
        , read_limiter(read_limiter_)
    {}

    /// Snapshot read.
    PageReaderImplNormal(
        NamespaceID ns_id_,
        PageStoragePtr storage_,
        const PageStorage::SnapshotPtr & snap_,
        ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage(storage_)
        , snap(snap_)
        , read_limiter(read_limiter_)
    {}

    DB::Page read(PageIdU64 page_id) const override { return storage->read(ns_id, page_id, read_limiter, snap); }

    PageMapU64 read(const PageIdU64s & page_ids) const override
    {
        return storage->read(ns_id, page_ids, read_limiter, snap);
    }

    using PageReadFields = PageStorage::PageReadFields;
    PageMapU64 read(const std::vector<PageReadFields> & page_fields) const override
    {
        return storage->read(ns_id, page_fields, read_limiter, snap);
    }

    PageIdU64 getNormalPageId(PageIdU64 page_id) const override
    {
        return storage->getNormalPageId(ns_id, page_id, snap);
    }

    PageEntry getPageEntry(PageIdU64 page_id) const override { return storage->getEntry(ns_id, page_id, snap); }

    PageStorage::SnapshotPtr getSnapshot(const String & tracing_id) const override
    {
        return storage->getSnapshot(tracing_id);
    }

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const override { return storage->getSnapshotsStat(); }

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, bool /*only_v2*/, bool /*only_v3*/)
        const override
    {
        storage->traverse(acceptor, nullptr);
    }

    FileUsageStatistics getFileUsageStatistics() const override { return storage->getFileUsageStatistics(); }

private:
    NamespaceID ns_id;
    PageStoragePtr storage;
    PageStorage::SnapshotPtr snap;
    ReadLimiterPtr read_limiter;
};


class PageReaderImplMixed : public PageReaderImpl
{
public:
    /// Not snapshot read.
    explicit PageReaderImplMixed(
        NamespaceID ns_id_,
        PageStoragePtr storage_v2_,
        PageStoragePtr storage_v3_,
        ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , read_limiter(read_limiter_)
    {}

    /// Snapshot read.
    PageReaderImplMixed(
        NamespaceID ns_id_,
        PageStoragePtr storage_v2_,
        PageStoragePtr storage_v3_,
        const PageStorage::SnapshotPtr & snap_,
        ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , snap(snap_)
        , read_limiter(read_limiter_)
    {}

    PageReaderImplMixed(
        NamespaceID ns_id_,
        PageStoragePtr storage_v2_,
        PageStoragePtr storage_v3_,
        PageStorage::SnapshotPtr && snap_,
        ReadLimiterPtr read_limiter_)
        : ns_id(ns_id_)
        , storage_v2(storage_v2_)
        , storage_v3(storage_v3_)
        , snap(std::move(snap_))
        , read_limiter(read_limiter_)
    {}

    DB::Page read(PageIdU64 page_id) const override
    {
        const auto & page_from_v3 = storage_v3->read(ns_id, page_id, read_limiter, toConcreteV3Snapshot(), false);
        if (page_from_v3.isValid())
        {
            return page_from_v3;
        }
        return storage_v2->read(ns_id, page_id, read_limiter, toConcreteV2Snapshot());
    }

    PageMapU64 read(const PageIdU64s & page_ids) const override
    {
        auto page_maps = storage_v3->read(ns_id, page_ids, read_limiter, toConcreteV3Snapshot(), false);
        PageIdU64s invalid_page_ids;
        for (const auto & [query_page_id, page] : page_maps)
        {
            if (!page.isValid())
            {
                invalid_page_ids.emplace_back(query_page_id);
            }
        }

        if (!invalid_page_ids.empty())
        {
            const auto & page_maps_from_v2
                = storage_v2->read(ns_id, invalid_page_ids, read_limiter, toConcreteV2Snapshot());
            for (const auto & [page_id_, page_] : page_maps_from_v2)
            {
                page_maps.at(page_id_) = page_;
            }
        }

        return page_maps;
    }

    using PageReadFields = PageStorage::PageReadFields;
    PageMapU64 read(const std::vector<PageReadFields> & page_fields) const override
    {
        auto page_maps = storage_v3->read(ns_id, page_fields, read_limiter, toConcreteV3Snapshot(), false);

        std::vector<PageReadFields> invalid_page_fields;

        for (const auto & page_field : page_fields)
        {
            if (!page_maps.at(page_field.first).isValid())
            {
                invalid_page_fields.emplace_back(page_field);
            }
        }

        if (!invalid_page_fields.empty())
        {
            auto page_maps_from_v2 = storage_v2->read(ns_id, invalid_page_fields, read_limiter, toConcreteV2Snapshot());
            for (const auto & page_field : invalid_page_fields)
            {
                page_maps.at(page_field.first) = page_maps_from_v2.at(page_field.first);
            }
        }

        return page_maps;
    }

    PageIdU64 getNormalPageId(PageIdU64 page_id) const override
    {
        PageIdU64 resolved_page_id = storage_v3->getNormalPageId(ns_id, page_id, toConcreteV3Snapshot(), false);
        if (resolved_page_id != INVALID_PAGE_U64_ID)
        {
            return resolved_page_id;
        }
        return storage_v2->getNormalPageId(ns_id, page_id, toConcreteV2Snapshot());
    }

    PageEntry getPageEntry(PageIdU64 page_id) const override
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

    FileUsageStatistics getFileUsageStatistics() const override { return storage_v3->getFileUsageStatistics(); }

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, bool only_v2, bool only_v3)
        const override
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
    const NamespaceID ns_id;
    PageStoragePtr storage_v2;
    PageStoragePtr storage_v3;
    PageStorage::SnapshotPtr snap;
    ReadLimiterPtr read_limiter;
};

class PageReaderImplUniversal : public PageReaderImpl
{
public:
    /// Not snapshot read.
    explicit PageReaderImplUniversal(
        KeyspaceID keyspace_id_,
        StorageType tag_,
        NamespaceID ns_id_,
        UniversalPageStoragePtr storage_,
        ReadLimiterPtr read_limiter_)
        : storage(storage_)
        , prefix(UniversalPageIdFormat::toFullPrefix(keyspace_id_, tag_, ns_id_))
        , read_limiter(read_limiter_)
    {}

    /// Snapshot read.
    PageReaderImplUniversal(
        KeyspaceID keyspace_id_,
        StorageType tag_,
        NamespaceID ns_id_,
        UniversalPageStoragePtr storage_,
        const PageStorage::SnapshotPtr & snap_,
        ReadLimiterPtr read_limiter_)
        : storage(storage_)
        , prefix(UniversalPageIdFormat::toFullPrefix(keyspace_id_, tag_, ns_id_))
        , snap(snap_)
        , read_limiter(read_limiter_)
    {}

    DB::Page read(PageIdU64 page_id) const override
    {
        return storage->read(UniversalPageIdFormat::toFullPageId(prefix, page_id), read_limiter, snap);
    }

    static inline PageMapU64 toPageMap(UniversalPageMap && us_page_map)
    {
        PageMapU64 page_map;
        for (auto & id_and_page : us_page_map)
        {
            page_map.emplace(UniversalPageIdFormat::getU64ID(id_and_page.first), std::move(id_and_page.second));
        }
        return page_map;
    }

    PageMapU64 read(const PageIdU64s & page_ids) const override
    {
        UniversalPageIds us_page_ids;
        for (const auto & page_id : page_ids)
        {
            us_page_ids.emplace_back(UniversalPageIdFormat::toFullPageId(prefix, page_id));
        }
        return PageReaderImplUniversal::toPageMap(storage->read(us_page_ids, read_limiter, snap));
    }

    using PageReadFields = PageStorage::PageReadFields;
    PageMapU64 read(const std::vector<PageReadFields> & page_fields) const override
    {
        std::vector<UniversalPageStorage::PageReadFields> us_page_fields;
        us_page_fields.reserve(page_fields.size());
        for (const auto & f : page_fields)
        {
            us_page_fields.emplace_back(UniversalPageIdFormat::toFullPageId(prefix, f.first), f.second);
        }
        return PageReaderImplUniversal::toPageMap(storage->read(us_page_fields, read_limiter, snap));
    }

    PageIdU64 getNormalPageId(PageIdU64 page_id) const override
    {
        return UniversalPageIdFormat::getU64ID(
            storage->getNormalPageId(UniversalPageIdFormat::toFullPageId(prefix, page_id), snap));
    }

    PageEntry getPageEntry(PageIdU64 page_id) const override
    {
        return storage->getEntry(UniversalPageIdFormat::toFullPageId(prefix, page_id), snap);
    }

    PageStorageSnapshotPtr getSnapshot(const String & tracing_id) const override
    {
        return storage->getSnapshot(tracing_id);
    }

    // Get some statistics of all living snapshots and the oldest living snapshot.
    SnapshotsStatistics getSnapshotsStat() const override { return storage->getSnapshotsStat(); }

    void traverse(const std::function<void(const DB::Page & page)> & acceptor, bool /*only_v2*/, bool /*only_v3*/)
        const override
    {
        auto snapshot = storage->getSnapshot(fmt::format("scan_{}", prefix));
        const auto page_ids = storage->page_directory->getAllPageIdsWithPrefix(prefix, snapshot);
        for (const auto & page_id : page_ids)
        {
            const auto page_id_and_entry = storage->page_directory->getByID(page_id, snapshot);
            acceptor(storage->blob_store->read(page_id_and_entry));
        }
    }

    FileUsageStatistics getFileUsageStatistics() const override { return storage->getFileUsageStatistics(); }

private:
    UniversalPageStoragePtr storage;
    String prefix;
    PageStorageSnapshotPtr snap;
    ReadLimiterPtr read_limiter;
};

std::unique_ptr<PageReaderImpl> PageReaderImpl::create(
    PageStorageRunMode run_mode_,
    KeyspaceID keyspace_id_,
    StorageType tag_,
    NamespaceID ns_id_,
    PageStoragePtr storage_v2_,
    PageStoragePtr storage_v3_,
    UniversalPageStoragePtr uni_ps_,
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
    case PageStorageRunMode::UNI_PS:
    {
        return std::make_unique<PageReaderImplUniversal>(keyspace_id_, tag_, ns_id_, uni_ps_, snap_, read_limiter_);
    }
    default:
        throw Exception(
            fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode_)),
            ErrorCodes::LOGICAL_ERROR);
    }
}

/***********************
  * PageReader methods *
  **********************/
/// Not snapshot read.
PageReader::PageReader(
    const PageStorageRunMode & run_mode_,
    KeyspaceID keyspace_id_,
    StorageType tag_,
    NamespaceID ns_id_,
    PageStoragePtr storage_v2_,
    PageStoragePtr storage_v3_,
    UniversalPageStoragePtr uni_ps_,
    ReadLimiterPtr read_limiter_)
    : impl(PageReaderImpl::create(
        run_mode_,
        keyspace_id_,
        tag_,
        ns_id_,
        storage_v2_,
        storage_v3_,
        uni_ps_,
        /*snap_=*/nullptr,
        read_limiter_))
{}

/// Snapshot read.
PageReader::PageReader(
    const PageStorageRunMode & run_mode_,
    KeyspaceID keyspace_id_,
    StorageType tag_,
    NamespaceID ns_id_,
    PageStoragePtr storage_v2_,
    PageStoragePtr storage_v3_,
    UniversalPageStoragePtr uni_ps_,
    PageStorage::SnapshotPtr snap_,
    ReadLimiterPtr read_limiter_)
    : impl(PageReaderImpl::create(
        run_mode_,
        keyspace_id_,
        tag_,
        ns_id_,
        storage_v2_,
        storage_v3_,
        uni_ps_,
        std::move(snap_),
        read_limiter_))
{}

PageReader::~PageReader() = default;

DB::Page PageReader::read(PageIdU64 page_id) const
{
    return impl->read(page_id);
}

PageMapU64 PageReader::read(const PageIdU64s & page_ids) const
{
    return impl->read(page_ids);
}

PageMapU64 PageReader::read(const std::vector<PageStorage::PageReadFields> & page_fields) const
{
    return impl->read(page_fields);
}

PageIdU64 PageReader::getNormalPageId(PageIdU64 page_id) const
{
    return impl->getNormalPageId(page_id);
}

PageEntry PageReader::getPageEntry(PageIdU64 page_id) const
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


FileUsageStatistics PageReader::getFileUsageStatistics() const
{
    return impl->getFileUsageStatistics();
}

void PageReader::traverse(const std::function<void(const DB::Page & page)> & acceptor, bool only_v2, bool only_v3) const
{
    impl->traverse(acceptor, only_v2, only_v3);
}

/**********************
  * PageWriter methods *
  *********************/

void PageWriter::write(WriteBatchWrapper && write_batch, WriteLimiterPtr write_limiter) const
{
    switch (run_mode)
    {
    case PageStorageRunMode::ONLY_V2:
    {
        writeIntoV2(std::move(write_batch.releaseWriteBatch()), write_limiter);
        break;
    }
    case PageStorageRunMode::ONLY_V3:
    {
        writeIntoV3(std::move(write_batch.releaseWriteBatch()), write_limiter);
        break;
    }
    case PageStorageRunMode::MIX_MODE:
    {
        writeIntoMixMode(std::move(write_batch.releaseWriteBatch()), write_limiter);
        break;
    }
    case PageStorageRunMode::UNI_PS:
    {
        writeIntoUni(std::move(write_batch.releaseUniversalWriteBatch()), write_limiter);
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
    const auto & ns_id = write_batch.getNamespaceID();
    WriteBatch wb_for_v2{ns_id};
    WriteBatch wb_for_put_v3{ns_id};

    // If we do need copy entry from V2 into V3
    // We need hold mem from V2 pages after write.
    std::list<MemHolder> mem_holders;

    PageIdU64Set page_ids_before_ref;

    for (const auto & write : write_batch.getWrites())
    {
        switch (write.type)
        {
        // PUT/PUT_EXTERNAL only for V3
        case WriteBatchWriteType::PUT:
        case WriteBatchWriteType::PUT_EXTERNAL:
        {
            page_ids_before_ref.insert(write.page_id);
            break;
        }
        // Both need del in v2 and v3
        case WriteBatchWriteType::DEL:
        {
            wb_for_v2.copyWrite(write);
            break;
        }
        case WriteBatchWriteType::REF:
        {
            // 1. Try to resolve normal page id
            PageIdU64 resolved_page_id = storage_v3->getNormalPageId(
                ns_id,
                write.ori_page_id,
                /*snapshot*/ nullptr,
                false);

            // If the origin id is found in V3, then just apply the ref to v3
            if (resolved_page_id != INVALID_PAGE_U64_ID)
            {
                break;
            }

            // 2. Check ori_page_id in current writebatch
            if (page_ids_before_ref.count(write.ori_page_id) > 0)
            {
                break;
            }

            // Else the normal id is not found in v3, read from v2 and create a new put + ref

            // 3. Check ori_page_id in V2
            const auto & entry_for_put = storage_v2->getEntry(ns_id, write.ori_page_id, /*snapshot*/ {});

            // If we can't find origin id in V3, must exist in V2.
            if (!entry_for_put.isValid())
            {
                throw Exception(
                    fmt::format(
                        "Can't find origin entry in V2 and V3, [ns_id={}, ori_page_id={}]",
                        ns_id,
                        write.ori_page_id),
                    ErrorCodes::LOGICAL_ERROR);
            }

            if (entry_for_put.size == 0)
            {
                // If the origin page size is 0.
                // That means origin page in V2 is a external page id.
                // Should not run into here after we introduce `StoragePool::forceTransformDataV2toV3`
                throw Exception(
                    fmt::format(
                        "Can't find the origin page in v3. Origin page in v2 size is 0, meaning it's a external id."
                        "Migrate a new being ref page into V3 [page_id={}] [origin_id={}]",
                        write.page_id,
                        write.ori_page_id,
                        entry_for_put.field_offsets.size()),
                    ErrorCodes::LOGICAL_ERROR);
            }

            // Else find out origin page is a normal page in V2
            auto page_for_put = storage_v2->read(ns_id, write.ori_page_id);

            // Keep the mem holder for later write
            mem_holders.emplace_back(page_for_put.mem_holder);
            assert(entry_for_put.size == page_for_put.data.size());

            // Page with fields
            if (!entry_for_put.field_offsets.empty())
            {
                wb_for_put_v3.putPage(
                    write.ori_page_id, //
                    entry_for_put.tag,
                    std::make_shared<ReadBufferFromMemory>(page_for_put.data.begin(), page_for_put.data.size()),
                    page_for_put.data.size(),
                    Page::fieldOffsetsToSizes(entry_for_put.field_offsets, entry_for_put.size));
            }
            else
            { // Normal page without fields
                wb_for_put_v3.putPage(
                    write.ori_page_id, //
                    entry_for_put.tag,
                    std::make_shared<ReadBufferFromMemory>(page_for_put.data.begin(), page_for_put.data.size()),
                    page_for_put.data.size());
            }

            LOG_INFO(
                Logger::get("PageWriter"),
                "Can't find the origin page in v3, migrate a new being ref page into V3 [page_id={}] [origin_id={}] "
                "[field_offsets={}]",
                write.page_id,
                write.ori_page_id,
                entry_for_put.field_offsets.size());

            break;
        }
        default:
        {
            throw Exception(fmt::format("Unknown write type: {}", static_cast<Int32>(write.type)));
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

void PageWriter::writeIntoUni(UniversalWriteBatch && write_batch, WriteLimiterPtr write_limiter) const
{
    uni_ps->write(std::move(write_batch), PageType::Normal, write_limiter);
}

PageStorageConfig PageWriter::getSettings() const
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
    case PageStorageRunMode::UNI_PS:
    {
        throw Exception("Not support.", ErrorCodes::NOT_IMPLEMENTED);
    }
    default:
        throw Exception(
            fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)),
            ErrorCodes::LOGICAL_ERROR);
    }
}

void PageWriter::reloadSettings(const PageStorageConfig & new_config) const
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
    case PageStorageRunMode::UNI_PS:
    {
        // Uni PS will reload config in its gc thread
        break;
    }
    default:
        throw Exception(
            fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)),
            ErrorCodes::LOGICAL_ERROR);
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
    case PageStorageRunMode::UNI_PS:
    {
        return false;
    }
    default:
        throw Exception(
            fmt::format("Unknown PageStorageRunMode {}", static_cast<UInt8>(run_mode)),
            ErrorCodes::LOGICAL_ERROR);
    }
}

} // namespace DB
