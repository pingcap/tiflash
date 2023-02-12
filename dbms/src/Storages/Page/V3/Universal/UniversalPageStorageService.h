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

#pragma once

#include <Storages/BackgroundProcessingPool.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>

namespace DB
{
class UniversalPageStorageService;
using UniversalPageStorageServicePtr = std::shared_ptr<UniversalPageStorageService>;

// This is wrapper class for UniversalPageStorage.
// It mainly manages background tasks like gc for UniversalPageStorage.
// It is like StoragePool for Page V2, and GlobalStoragePool for Page V3.
class UniversalPageStorageService final
{
public:
    static UniversalPageStorageServicePtr
    create(
        Context & context,
        const String & name,
        PSDiskDelegatorPtr delegator,
        const PageStorageConfig & config);

    void restore();
    bool gc();
    UniversalPageStoragePtr getUniversalPageStorage() const { return uni_page_storage; }
    ~UniversalPageStorageService();

private:
    explicit UniversalPageStorageService(Context & global_context_)
        : global_context(global_context_)
        , uni_page_storage(nullptr)
    {
    }

private:
    Context & global_context;
    UniversalPageStoragePtr uni_page_storage;
    BackgroundProcessingPool::TaskHandle gc_handle;

    std::atomic<Timepoint> last_try_gc_time = Clock::now();
};
} // namespace DB
