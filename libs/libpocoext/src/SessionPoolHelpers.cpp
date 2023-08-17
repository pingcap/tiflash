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

#include <Poco/Ext/SessionPoolHelpers.h>
#include <Poco/ThreadPool.h>

#include <mutex>


std::shared_ptr<Poco::Data::SessionPool> createAndCheckResizePocoSessionPool(PocoSessionPoolConstructor pool_constr)
{
    static std::mutex mutex;

    Poco::ThreadPool & pool = Poco::ThreadPool::defaultPool();

    /// NOTE: The lock don't guarantee that external users of the pool don't change its capacity
    std::unique_lock lock(mutex);

    if (pool.available() == 0)
        pool.addCapacity(2 * std::max(pool.capacity(), 1));

    return pool_constr();
}
