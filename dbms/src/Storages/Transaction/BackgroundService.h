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
#include <Storages/Transaction/RegionDataRead.h>
#include <Storages/Transaction/Types.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>
#include <memory>
#include <queue>

namespace DB
{
class TMTContext;
class Region;
using RegionPtr = std::shared_ptr<Region>;
using Regions = std::vector<RegionPtr>;
using RegionMap = std::unordered_map<RegionID, RegionPtr>;
class BackgroundProcessingPool;

class BackgroundService : boost::noncopyable
{
public:
    explicit BackgroundService(TMTContext &);

    ~BackgroundService();

private:
    TMTContext & tmt;
    BackgroundProcessingPool & background_pool;

    Poco::Logger * log;

    BackgroundProcessingPool::TaskHandle single_thread_task_handle;
    BackgroundProcessingPool::TaskHandle storage_gc_handle;
};

} // namespace DB
