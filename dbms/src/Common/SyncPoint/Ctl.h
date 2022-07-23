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

#include <Poco/Logger.h>
#include <common/logger_useful.h>

#include <mutex>
#include <unordered_map>

namespace DB
{

class SyncPointCtl
{
public:
    /**
     * Enable the sync point. After enabling, when executed to the sync point defined with `SYNC_FOR()`,
     * the execution will be suspended, until `waitAndPause()` is called somewhere (e.g. in tests).
     */
    static void enable(const char * name);

    /**
     * Disable the sync point. Existing suspends will be continued.
     */
    static void disable(const char * name);

    /**
     * Suspend the execution, until `waitAndPause()` and `next()` is called somewhere.
     * You should not invoke this function directly. Invoke `SYNC_FOR()` instead.
     */
    static void sync(const char * name);

    /**
     * Wait for the sync point being executed.
     * Usually this function is invoked in tests.
     */
    static void waitAndPause(const char * name);

    /**
     * Continue the execution after the specified sync point.
     * You must first `waitAndPause()` for it, then `next()` it.
     */
    static void next(const char * name);

private:
    class SyncChannel;
    using SyncChannelPtr = std::shared_ptr<SyncChannel>;

    static Poco::Logger * getLogger()
    {
        static Poco::Logger * logger = &Poco::Logger::get("SyncPointCtl");
        return logger;
    }

    static std::pair<SyncChannelPtr, SyncChannelPtr> mustGetChannel(const char * name);

    inline static std::unordered_map<std::string, std::pair<SyncChannelPtr, SyncChannelPtr>>
        channels{};
    inline static std::mutex mu{};
};

} // namespace DB
