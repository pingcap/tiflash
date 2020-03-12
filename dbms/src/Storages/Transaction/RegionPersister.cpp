#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/RegionManager.h>
#include <Storages/Transaction/RegionPersister.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

void RegionPersister::drop(RegionID region_id, const RegionTaskLock &)
{
    stable::WriteBatch wb;
    wb.delPage(region_id);
    page_storage.write(std::move(wb));
}

void RegionPersister::computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer)
{
    auto & [region_id, buffer, region_size, applied_index] = region_write_buffer;

    region_id = region.id();
    std::tie(region_size, applied_index) = region.serialize(buffer);
    if (unlikely(region_size > static_cast<size_t>(std::numeric_limits<UInt32>::max())))
    {
        LOG_WARNING(&Logger::get("RegionPersister"),
            "Persisting big region: " << region.toString() << " with data info: " << region.dataInfo() << ", serialized size "
                                      << region_size);
        //throw Exception("Region is too big to persist", ErrorCodes::LOGICAL_ERROR);
    }
}

void RegionPersister::persist(const Region & region, const RegionTaskLock & lock) { doPersist(region, &lock); }

void RegionPersister::persist(const Region & region) { doPersist(region, nullptr); }

void RegionPersister::doPersist(const Region & region, const RegionTaskLock * lock)
{
    // Support only one thread persist.
    RegionCacheWriteElement region_buffer;
    computeRegionWriteBuffer(region, region_buffer);

    if (lock)
        doPersist(region_buffer, *lock, region);
    else
        doPersist(region_buffer, region_manager.genRegionTaskLock(region.id()), region);

    region.markPersisted();
}

void RegionPersister::doPersist(RegionCacheWriteElement & region_write_buffer, const RegionTaskLock &, const Region & region)
{
    auto & [region_id, buffer, region_size, applied_index] = region_write_buffer;

    std::lock_guard<std::mutex> lock(mutex);

    auto cache = page_storage.getEntry(region_id);
    if (cache.isValid() && cache.tag > applied_index)
        return;

    if (region.isPendingRemove())
    {
        LOG_DEBUG(log, "no need to persist " << region.toString(false) << " because of pending remove");
        return;
    }

    stable::WriteBatch wb;
    auto read_buf = buffer.tryGetReadBuffer();
    wb.putPage(region_id, applied_index, read_buf, region_size);
    page_storage.write(std::move(wb));
}

RegionMap RegionPersister::restore(IndexReaderCreateFunc * func)
{
    // FIXME: if we use DB::PageStorage, we should call `restore`
    // page_storage.restore();

    RegionMap regions;
    auto acceptor = [&](const stable::Page & page) {
        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        auto region = Region::deserialize(buf, func);
        if (page.page_id != region->id())
            throw Exception("region id and page id not match!", ErrorCodes::LOGICAL_ERROR);
        regions.emplace(page.page_id, region);
    };
    page_storage.traverse(acceptor);

    return regions;
}

bool RegionPersister::gc() { return page_storage.gc(); }

} // namespace DB
