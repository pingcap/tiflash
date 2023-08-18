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

#include <Common/SyncPoint/ScopeGuard.h>
#include <Poco/Logger.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

class SyncPointCtl
{
public:
    /**
     * Enable the sync point. After enabling, when executed to the sync point defined with `SYNC_FOR()`,
     * the execution will be suspended, until `waitAndPause()` or `waitAndNext()` is called
     * somewhere (e.g. in tests).
     */
    static void enable(const char * name);

    /**
     * Disable the sync point. Existing suspends will be continued.
     */
    static void disable(const char * name);

    /**
     * Suspend the execution, until `waitAndPause()`, `next()` or `waitAndNext()` is called somewhere.
     * You should not invoke this function directly. Invoke `SYNC_FOR()` instead.
     */
    static void sync(const char * name);

    /**
     * Wait for the sync point being executed. The code at the sync point will keep
     * pausing until you call `next()`.
     */
    static void waitAndPause(const char * name);

    /**
     * Continue the execution after the specified sync point.
     * You must first `waitAndPause()` for it, then `next()` it.
     */
    static void next(const char * name);

    /**
     * Wait for the sync point being executed. After that, continue the execution after the sync point.
     */
    static void waitAndNext(const char * name)
    {
        waitAndPause(name);
        next(name);
    }

    /**
     * Enable the sync point in the current scope. When scope exits, the sync point will be disabled.
     *
     * After enabling, when executed to the sync point defined with `SYNC_FOR()`, the execution
     * will be suspended, until `waitAndPause()` or `waitAndNext()` is called somewhere (e.g. in tests).
     */
    static SyncPointScopeGuard enableInScope(const char * name);

private:
    class SyncChannel;
    using SyncChannelPtr = std::shared_ptr<SyncChannel>;

    static Poco::Logger * getLogger()
    {
        static Poco::Logger * logger = &Poco::Logger::get("SyncPointCtl");
        return logger;
    }

    static std::pair<SyncChannelPtr, SyncChannelPtr> mustGetChannel(const char * name);

    inline static std::unordered_map<std::string, std::pair<SyncChannelPtr, SyncChannelPtr>> channels{};
    inline static std::mutex mu{};
};

} // namespace DB
