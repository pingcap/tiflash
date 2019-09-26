#include <Storages/DeltaMerge/StoragePool.h>

namespace DB
{
namespace DM
{

// TODO: Load configs from settings.
StoragePool::StoragePool(const String &name, const String & path)
    : max_log_page_id(0),
      max_data_page_id(0),
      max_meta_page_id(0),
      log_storage(name + ".log", path + "/log", {}, &max_log_page_id),
      data_storage(name + ".data", path + "/data", {}, &max_data_page_id),
      meta_storage(name + ".meta", path + "/meta", {}, &max_meta_page_id)
{
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