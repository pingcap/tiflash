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

struct RemoteReadTaskTrait
{
};

struct RNPreparerTrait
{
};

} // namespace io_pool_details

// TODO: Move these out.
using DataStoreS3Pool = IOThreadPool<io_pool_details::DataStoreS3Trait>;
using S3FileCachePool = IOThreadPool<io_pool_details::S3FileCacheTrait>;
using RNRemoteReadTaskPool = IOThreadPool<io_pool_details::RemoteReadTaskTrait>;
using RNPagePreparerPool = IOThreadPool<io_pool_details::RNPreparerTrait>;
} // namespace DB
