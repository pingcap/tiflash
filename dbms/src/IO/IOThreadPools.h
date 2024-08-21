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

namespace io_pool_details
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
} // namespace io_pool_details

// TODO: Move these out.
using DataStoreS3Pool = IOThreadPool<io_pool_details::DataStoreS3Trait>;
using S3FileCachePool = IOThreadPool<io_pool_details::S3FileCacheTrait>;
using RNWritePageCachePool = IOThreadPool<io_pool_details::RNWritePageCacheTrait>;
using WNEstablishDisaggTaskPool = IOThreadPool<io_pool_details::WNEstablishDisaggTaskTrait>;

// The call chain is `buildReadTaskForWriteNode => buildReadTaskForWriteNodeTable => buildRNReadSegmentTask`.
// Each of them will use the corresponding thread pool.
// Cannot share one thread pool. Because in extreme cases, if the previous function exhausts all threads,
// the subsequent functions will wait for idle threads, causing a deadlock.
using BuildReadTaskForWNPool = IOThreadPool<io_pool_details::BuildReadTaskForWNTrait>;
using BuildReadTaskForWNTablePool = IOThreadPool<io_pool_details::BuildReadTaskForWNTableTrait>;
using BuildReadTaskPool = IOThreadPool<io_pool_details::BuildReadTaskTrait>;
} // namespace DB
