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

#include <Common/setThreadName.h>
#include <Poco/Util/Timer.h>

namespace DB
{
struct Timer : public Poco::Util::Timer
{
    explicit Timer(const char * name)
        : thread_worker_name(name)
    {}

protected:
    void run() override
    {
        setThreadName(thread_worker_name);
        Poco::Util::Timer::run();
    }

private:
    const char * thread_worker_name;
};

} // namespace DB
