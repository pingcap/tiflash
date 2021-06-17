#pragma once

#include <Storages/Page/PageStorage.h>

namespace DB
{
struct Settings;

void mergeConfigFromSettings(const DB::Settings & settings, PageStorage::Config & config);

PageStorage::Config getConfigFromSettings(const DB::Settings & settings);

} // namespace DB
