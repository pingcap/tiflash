#include <Storages/DeltaMerge/StoragePool.h>

namespace DB
{
namespace DM
{

// TODO: Load configs from settings.
StoragePool::StoragePool(const String & path)
    : log_storage(path + "/log", {}), data_storage(path + "/data", {}), meta_storage(path + "/meta", {})
{
    auto get_max_page_id = [](PageStorage & s) {
        PageId max_page_id = 0;
        s.traversePageCache([&max_page_id](PageId page_id, const PageCache &) { max_page_id = std::max(max_page_id, page_id); });
        return max_page_id;
    };
    max_log_page_id  = get_max_page_id(log_storage);
    max_data_page_id = get_max_page_id(data_storage);
    max_meta_page_id = get_max_page_id(meta_storage);
}

bool StoragePool::gc(const Seconds try_gc_period)
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