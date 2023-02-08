// Copyright 2022 PingCAP, Ltd.
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
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>

namespace DB
{
UniversalPageStorageServicePtr UniversalPageStorageService::create(
    Context & context,
    const String & name,
    PSDiskDelegatorPtr delegator,
    const PageStorageConfig & config)
{
    auto service = UniversalPageStorageServicePtr(new UniversalPageStorageService(context));
    service->uni_page_storage = UniversalPageStorage::create(name, delegator, config, context.getFileProvider());
    return service;
}

void UniversalPageStorageService::restore()
{
    uni_page_storage->restore();
    gc_handle = global_context.getBackgroundPool().addTask(
        [this] {
            return this->gc();
        },
        false,
        /*interval_ms*/ 30 * 1000);
}

bool UniversalPageStorageService::gc()
{
    Timepoint now = Clock::now();
    const std::chrono::seconds try_gc_period(30);
    if (now < (last_try_gc_time.load() + try_gc_period))
        return false;

    last_try_gc_time = now;
    return this->uni_page_storage->gc();
}

UniversalPageStorageService::~UniversalPageStorageService()
{
    if (gc_handle)
    {
        global_context.getBackgroundPool().removeTask(gc_handle);
        gc_handle = nullptr;
    }
}
} // namespace DB
