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

#include <Common/Stopwatch.h>

#include <iostream>
#include <thread>
#include <vector>


int main(int, char **)
{
    static constexpr size_t num_threads = 10;
    static constexpr size_t num_iterations = 3;

    std::vector<std::thread> threads(num_threads);

    AtomicStopwatch watch;
    Stopwatch total_watch;

    for (size_t i = 0; i < num_threads; ++i)
    {
        threads[i] = std::thread([i, &watch, &total_watch] {
            size_t iteration = 0;
            while (iteration < num_iterations)
            {
                if (auto lock = watch.compareAndRestartDeferred(1))
                {
                    std::cerr << "Thread " << i << ": begin iteration " << iteration
                              << ", elapsed: " << total_watch.elapsedMilliseconds() << " ms.\n";
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    std::cerr << "Thread " << i << ": end iteration " << iteration
                              << ", elapsed: " << total_watch.elapsedMilliseconds() << " ms.\n";
                    ++iteration;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    return 0;
}
