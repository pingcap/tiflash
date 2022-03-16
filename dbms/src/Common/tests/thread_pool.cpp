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

#include <common/ThreadPool.h>

/** Reproduces bug in ThreadPool.
  * It get stuck if we call 'wait' many times from many other threads simultaneously.
  */


int main(int, char **)
{
    auto worker = [] {
        for (size_t i = 0; i < 100000000; ++i)
            __asm__ volatile("nop");
    };

    constexpr size_t num_threads = 4;
    constexpr size_t num_jobs = 4;

    ThreadPool pool(num_threads);

    for (size_t i = 0; i < num_jobs; ++i)
        pool.schedule(worker);

    constexpr size_t num_waiting_threads = 4;

    ThreadPool waiting_pool(num_waiting_threads);

    for (size_t i = 0; i < num_waiting_threads; ++i)
        waiting_pool.schedule([&pool] { pool.wait(); });

    waiting_pool.wait();

    return 0;
}
