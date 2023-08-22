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

#include <Storages/Page/FileUsage.h>

#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>


namespace DB
{
class Context;


/** Periodically (each minute, starting at 30 seconds offset)
  *  calculates and updates some metrics,
  *  that are not updated automatically (so, need to be asynchronously calculated).
  */
class AsynchronousMetrics
{
public:
    explicit AsynchronousMetrics(Context & context_)
        : context(context_)
        , thread([this] { run(); })
    {}

    ~AsynchronousMetrics();

    using Value = double;
    using Container = std::unordered_map<std::string, Value>;

    /// Returns copy of all values.
    Container getValues() const;

private:
    FileUsageStatistics getPageStorageFileUsage();

private:
    Context & context;

    bool quit{false};
    std::mutex wait_mutex;
    std::condition_variable wait_cond;

    Container container;
    mutable std::mutex container_mutex;

    std::thread thread;

    void run();
    void update();

    void set(const std::string & name, Value value);
};

} // namespace DB
