#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/PathPool.h>

namespace DB
{
namespace DM
{
enum class StorageType
{
    Log  = 1,
    Data = 2,
    Meta = 3,
};

PageStorage::Config extractConfig(const Settings & settings, StorageType subtype)
{
#define SET_CONFIG(NAME)                                                            \
    config.num_write_slots   = settings.dt_storage_pool_##NAME##_write_slots;       \
    config.gc_min_files      = settings.dt_storage_pool_##NAME##_gc_min_file_num;   \
    config.gc_min_bytes      = settings.dt_storage_pool_##NAME##_gc_min_bytes;      \
    config.gc_min_legacy_num = settings.dt_storage_pool_##NAME##_gc_min_legacy_num; \
    config.gc_max_valid_rate = settings.dt_storage_pool_##NAME##_gc_max_valid_rate;

    PageStorage::Config config;
    config.open_file_max_idle_time = Seconds(settings.dt_open_file_max_idle_seconds);
    {
        // The probability is [0~1000] out of 1000
        Int64 prob                          = settings.dt_page_gc_low_write_prob * 1000;
        prob                                = std::max(0, std::min(1000, prob));
        config.prob_do_gc_when_write_is_low = prob;
    }
    switch (subtype)
    {
    case StorageType::Log:
        SET_CONFIG(log);
        break;
    case StorageType::Data:
        SET_CONFIG(data);
        break;
    case StorageType::Meta:
        SET_CONFIG(meta);
        break;
    default:
        throw Exception("Unknown subtype in extractConfig: " + DB::toString(static_cast<Int32>(subtype)));
    }
#undef SET_CONFIG

    return config;
}

StoragePool::StoragePool(const String & name, StoragePathPool & path_pool, const Context & global_ctx, const Settings & settings)
    : // The iops and bandwidth in log_storage are relatively high, use multi-disks if possible
      log_storage(name + ".log",
                  path_pool.getPSDiskDelegatorMulti("log"),
                  extractConfig(settings, StorageType::Log),
                  global_ctx.getFileProvider(),
                  global_ctx.getTiFlashMetrics()),
      // The iops in data_storage is low, only use the first disk for storing data
      data_storage(name + ".data",
                   path_pool.getPSDiskDelegatorSingle("data"),
                   extractConfig(settings, StorageType::Data),
                   global_ctx.getFileProvider(),
                   global_ctx.getTiFlashMetrics()),
      // The iops in meta_storage is relatively high, use multi-disks if possible
      meta_storage(name + ".meta",
                   path_pool.getPSDiskDelegatorMulti("meta"),
                   extractConfig(settings, StorageType::Meta),
                   global_ctx.getFileProvider(),
                   global_ctx.getTiFlashMetrics()),
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

void StoragePool::drop()
{
    meta_storage.drop();
    data_storage.drop();
    log_storage.drop();
}

bool StoragePool::gc(const Settings & /*settings*/, const Seconds & try_gc_period)
{
    {
        std::lock_guard<std::mutex> lock(mutex);

        Timepoint now = Clock::now();
        if (now < (last_try_gc_time.load() + try_gc_period))
            return false;

        last_try_gc_time = now;
    }

    bool ok = false;

    // FIXME: The global_context.settings is mutable, we need a way to reload thses settings.
    // auto config = extractConfig(settings, StorageType::Meta);
    // meta_storage.reloadSettings(config);
    ok |= meta_storage.gc();

    // config = extractConfig(settings, StorageType::Data);
    // data_storage.reloadSettings(config);
    ok |= data_storage.gc();

    // config = extractConfig(settings, StorageType::Log);
    // log_storage.reloadSettings(config);
    ok |= log_storage.gc();

    return ok;
}

} // namespace DM
} // namespace DB
