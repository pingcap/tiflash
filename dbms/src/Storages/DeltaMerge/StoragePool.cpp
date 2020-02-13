#include <Storages/DeltaMerge/StoragePool.h>

namespace DB
{
namespace DM
{

// TODO: Load configs from settings.
StoragePool::StoragePool(const String & name, const String & path)
    : log_storage(name + ".log", path + "/log", {}),
      data_storage(name + ".data", path + "/data", {}),
      meta_storage(name + ".meta", path + "/meta", {}),
      max_log_page_id(0),
      max_data_page_id(0),
      max_meta_page_id(0)
{
}

void StoragePool::restore()
{
    log_storage.restore();
    data_storage.restore();
    meta_storage.restore();

    max_log_page_id  = log_storage.getMaxId();
    max_data_page_id = data_storage.getMaxId();
    max_meta_page_id = meta_storage.getMaxId();
}

bool StoragePool::gc(const Seconds & try_gc_period)
{
    std::lock_guard<std::mutex> lock(mutex);

    Timepoint now = Clock::now();
    if (now < (last_try_gc_time.load() + try_gc_period))
        return false;
    last_try_gc_time = now;

    bool ok = false;

    ok |= meta_storage.gc();
    ok |= data_storage.gc();
    ok |= log_storage.gc();

    return ok;
}

} // namespace DM
} // namespace DB
