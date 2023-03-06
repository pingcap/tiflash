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

#include <Core/Field.h>
#include <IO/IOThreadPool.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

std::unique_ptr<ThreadPool> IOThreadPool::instance;

void IOThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The IO thread pool is initialized twice");
    }

    instance = std::make_unique<ThreadPool>(max_threads, max_free_threads, queue_size, false /*shutdown_on_exception*/, "io-pool");
}

ThreadPool & IOThreadPool::get()
{
    if (!instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The IO thread pool is not initialized");
    }

    return *instance;
}

} // namespace DB
