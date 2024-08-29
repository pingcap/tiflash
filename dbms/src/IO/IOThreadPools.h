// Copyright 2023 PingCAP, Inc.
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

#include <IO/IOThreadPool.h>

namespace DB
{

namespace IOPoolHelper
{
struct S3FileCacheTrait
{
};

struct DataStoreS3Trait
{
};

struct RNWritePageCacheTrait
{
};

struct WNEstablishDisaggTaskTrait
{
};

struct BuildReadTaskForWNTrait
{
};
struct BuildReadTaskForWNTableTrait
{
};
struct BuildReadTaskTrait
{
};

// FutureContainer will wait for all futures finished automatically.
class FutureContainer
{
public:
    FutureContainer(const LoggerPtr & log_, size_t reserve_size = 0)
        : log(log_)
    {
        futures.reserve(reserve_size);
    }

    ~FutureContainer() { waitFinish(); }

    void add(std::future<void> && f) { futures.push_back(std::move(f)); }

    // Get the results of all futures.
    // If exceptions happened, rethrow the first exception.
    void getAllResults()
    {
        std::exception_ptr first_exception;
        for (auto & f : futures)
        {
            try
            {
                f.get();
            }
            catch (...)
            {
                if (!first_exception)
                    first_exception = std::current_exception();
                tryLogCurrentException(log);
            }
        }

        if (first_exception)
            std::rethrow_exception(first_exception);
    }

private:
    // Just wait futures finished. Don't care about the results.
    void waitFinish()
    {
        for (auto & f : futures)
            if (f.valid())
                f.wait();
    }
    std::vector<std::future<void>> futures;
    LoggerPtr log;
};

} // namespace IOPoolHelper

// TODO: Move these out.
using DataStoreS3Pool = IOThreadPool<IOPoolHelper::DataStoreS3Trait>;
using S3FileCachePool = IOThreadPool<IOPoolHelper::S3FileCacheTrait>;
using RNWritePageCachePool = IOThreadPool<IOPoolHelper::RNWritePageCacheTrait>;
using WNEstablishDisaggTaskPool = IOThreadPool<IOPoolHelper::WNEstablishDisaggTaskTrait>;

// The call chain is `buildReadTaskForWriteNode => buildReadTaskForWriteNodeTable => buildRNReadSegmentTask`.
// Each of them will use the corresponding thread pool.
// Cannot share one thread pool. Because in extreme cases, if the previous function exhausts all threads,
// the subsequent functions will wait for idle threads, causing a deadlock.
using BuildReadTaskForWNPool = IOThreadPool<IOPoolHelper::BuildReadTaskForWNTrait>;
using BuildReadTaskForWNTablePool = IOThreadPool<IOPoolHelper::BuildReadTaskForWNTableTrait>;
using BuildReadTaskPool = IOThreadPool<IOPoolHelper::BuildReadTaskTrait>;
} // namespace DB
