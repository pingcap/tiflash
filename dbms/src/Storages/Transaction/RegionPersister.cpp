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

void RegionPersister::drop(RegionID region_id)
{
    WriteBatch wb;
    wb.delPage(region_id);
    page_storage.write(wb);
}

void RegionPersister::computeRegionWriteBuffer(const Region & region, RegionCacheWriteElement & region_write_buffer)
{
    auto & [region_id, buffer, region_size, applied_index] = region_write_buffer;

    region_id = region.id();
    std::tie(region_size, applied_index) = region.serialize(buffer);
    if (unlikely(region_size > std::numeric_limits<UInt32>::max()))
    {
        LOG_ERROR(&Logger::get("RegionPersister"),
            region.toString() << " with data info: " << region.dataInfo() << ", serialized size " << region_size
                              << " is too big to persist");
        throw Exception("Region is too big to persist", ErrorCodes::LOGICAL_ERROR);
    }
}

void RegionPersister::persist(const Region & region, const RegionTaskLock & lock) { doPersist(region, &lock); }

void RegionPersister::persist(const Region & region) { doPersist(region, nullptr); }

void RegionPersister::doPersist(const Region & region, const RegionTaskLock * lock)
{
    // Support only on thread persist.
    size_t dirty_flag = region.dirtyFlag();

    RegionCacheWriteElement region_buffer;
    computeRegionWriteBuffer(region, region_buffer);

    if (lock)
        doPersist(region_buffer, *lock);
    else
        doPersist(region_buffer, region_manager.genRegionTaskLock(region.id()));

    region.markPersisted();
    region.decDirtyFlag(dirty_flag);
}

void RegionPersister::doPersist(RegionCacheWriteElement & region_write_buffer, const RegionTaskLock &)
{
    auto & [region_id, buffer, region_size, applied_index] = region_write_buffer;

    std::lock_guard<std::mutex> lock(mutex);

    auto cache = page_storage.getCache(region_id);
    if (cache.isValid() && cache.tag > applied_index)
    {
        LOG_DEBUG(log, "[region " << region_id << ", applied index " << applied_index << "] have already persisted index " << cache.tag);
        return;
    }

    WriteBatch wb;
    auto read_buf = buffer.tryGetReadBuffer();
    wb.putPage(region_id, applied_index, read_buf, region_size);
    page_storage.write(wb);
}

RegionMap RegionPersister::restore(RegionClientCreateFunc * func)
{
    RegionMap regions;
    auto acceptor = [&](const Page & page) {
        ReadBufferFromMemory buf(page.data.begin(), page.data.size());
        auto region = Region::deserialize(buf, func);
        if (page.page_id != region->id())
            throw Exception("region id and page id not match!", ErrorCodes::LOGICAL_ERROR);
        regions.emplace(page.page_id, region);
    };
    page_storage.traverse(acceptor);

    LOG_INFO(log, "restore " << regions.size() << " regions");
    return regions;
}

bool RegionPersister::gc() { return page_storage.gc(); }

} // namespace DB
