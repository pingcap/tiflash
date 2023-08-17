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

#include <Common/ThreadFactory.h>
namespace DB
{
class CollectProcInfoBackgroundTask
{
public:
    CollectProcInfoBackgroundTask() { begin(); }
    ~CollectProcInfoBackgroundTask() { end(); }

private:
    void begin();

    void end() noexcept;

    void finish();

private:
    void memCheckJob();

    std::mutex mu;
    std::condition_variable cv;
    bool end_fin = false;
    std::atomic_bool end_syn{false};

    /// In unit test, multiple FlashGrpcServerHolder may be started, leading to the startup of multiple memCheckJob threads.
    /// To ensure that only one memCheckJob thread is running, here we use a static variable.
    static std::atomic_bool is_already_begin;
};
} // namespace DB
