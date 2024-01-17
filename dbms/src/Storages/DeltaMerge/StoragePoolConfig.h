#pragma once

#include <Interpreters/Settings.h>
#include <Storages/Page/Config.h>
#include <Storages/Page/ConfigSettings.h>
#include <Storages/Page/PageConstants.h>

namespace DB::DM
{

inline PageStorageConfig extractConfig(const Settings & settings, StorageType subtype)
{
#define SET_CONFIG(NAME)                                                            \
    config.num_write_slots = settings.dt_storage_pool_##NAME##_write_slots;         \
    config.gc_min_files = settings.dt_storage_pool_##NAME##_gc_min_file_num;        \
    config.gc_min_bytes = settings.dt_storage_pool_##NAME##_gc_min_bytes;           \
    config.gc_min_legacy_num = settings.dt_storage_pool_##NAME##_gc_min_legacy_num; \
    config.gc_max_valid_rate = settings.dt_storage_pool_##NAME##_gc_max_valid_rate; \
    config.blob_heavy_gc_valid_rate = settings.dt_page_gc_threshold;

    PageStorageConfig config = getConfigFromSettings(settings);

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
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unknown subtype in extractConfig: {} ",
            static_cast<Int32>(subtype));
    }
#undef SET_CONFIG

    return config;
}

} // namespace DB::DM
