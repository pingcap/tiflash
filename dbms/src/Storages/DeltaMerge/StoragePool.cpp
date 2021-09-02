#include <Common/FailPoint.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/Page/ConfigSettings.h>
#include <Storages/PathPool.h>

namespace DB
{
namespace FailPoints
{
extern const char force_set_dtfile_exist_when_acquire_id[];
} // namespace FailPoints
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

    PageStorage::Config config = getConfigFromSettings(settings);

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
      max_meta_page_id(0),
      global_context(global_ctx)
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

PageId StoragePool::newDataPageIdForDTFile(StableDiskDelegator & delegator, const char * who)
{
    // In case that there is a DTFile created on disk but TiFlash crashes without persisting the ID.
    // After TiFlash process restored, the ID will be inserted into the stable delegator, but we may
    // get a duplicated ID from the `storage_pool.data`. (tics#2756)
    PageId dtfile_id;
    do
    {
        dtfile_id = ++max_data_page_id;

        auto existed_path = delegator.getDTFilePath(dtfile_id, /*throw_on_not_exist=*/false);
        fiu_do_on(FailPoints::force_set_dtfile_exist_when_acquire_id, {
            static size_t fail_point_called = 0;
            if (existed_path.empty() && fail_point_called % 10 == 0)
            {
                existed_path = "<mock for existed path>";
            }
            fail_point_called++;
        });
        if (likely(existed_path.empty()))
        {
            break;
        }
        // else there is a DTFile with that id, continue to acquire a new ID.
        LOG_WARNING(&Poco::Logger::get(who),
                    "The DTFile is already exists, continute to acquire another ID. [path=" + existed_path
                        + "] [id=" + DB::toString(dtfile_id) + "]");
    } while (true);
    return dtfile_id;
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

    bool done_anything = false;

    // FIXME: The global_context.settings is mutable, we need a way to reload thses settings.
    // auto config = extractConfig(settings, StorageType::Meta);
    // meta_storage.reloadSettings(config);
    done_anything |= meta_storage.gc();

    // config = extractConfig(settings, StorageType::Data);
    // data_storage.reloadSettings(config);
    done_anything |= data_storage.gc();

    // config = extractConfig(settings, StorageType::Log);
    // log_storage.reloadSettings(config);
    done_anything |= log_storage.gc();

    return done_anything;
}

} // namespace DM
} // namespace DB
