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

#include <Common/Exception.h>
#include <Common/SyncPoint/Ctl.h>
#include <Common/SyncPoint/ScopeGuard.h>
#include <Common/SyncPoint/SyncChannel.h>
#include <common/logger_useful.h>
#include <fiu-control.h>

namespace DB
{

#ifdef FIU_ENABLE

void SyncPointCtl::enable(const char * name)
{
    {
        std::unique_lock lock(mu);
        channels.try_emplace(name, std::make_pair(std::make_shared<SyncChannel>(), std::make_shared<SyncChannel>()));
    }
    fiu_enable(name, 1, nullptr, 0);
    LOG_DEBUG(getLogger(), "Enabled: {}", name);
}

void SyncPointCtl::disable(const char * name)
{
    fiu_disable(name);
    {
        std::unique_lock lock(mu);
        if (auto const & iter = channels.find(name); iter != channels.end())
        {
            auto [first_ch, second_ch] = iter->second;
            first_ch->close();
            second_ch->close();
            channels.erase(iter);
        }
    }
    LOG_DEBUG(getLogger(), "Disabled: {}", name);
}

std::pair<SyncPointCtl::SyncChannelPtr, SyncPointCtl::SyncChannelPtr> SyncPointCtl::mustGetChannel(const char * name)
{
    std::unique_lock lock(mu);
    if (auto iter = channels.find(name); iter == channels.end())
    {
        throw Exception(fmt::format("SyncPoint {} is not enabled", name));
    }
    else
    {
        return iter->second;
    }
}

void SyncPointCtl::waitAndPause(const char * name)
{
    auto ch = mustGetChannel(name).first;
    LOG_DEBUG(getLogger(), "waitAndPause({}) waiting...", name);
    auto result = ch->recv();
    LOG_DEBUG(getLogger(), "waitAndPause({}) {}", name, result ? "finished" : "cancelled");
}

void SyncPointCtl::next(const char * name)
{
    auto ch = mustGetChannel(name).second;
    LOG_DEBUG(getLogger(), "next({}) trying...", name);
    auto result = ch->send();
    LOG_DEBUG(getLogger(), "next({}) {}", name, result ? "done" : "cancelled");
}

void SyncPointCtl::sync(const char * name)
{
    auto [ch_1, ch_2] = mustGetChannel(name);
    // Print a stack, which is helpful to know where undesired SYNC_FOR comes from.
    LOG_DEBUG(getLogger(), "SYNC_FOR({}) trying... \n\n# Current Stack: {}", name, StackTrace().toString());
    auto result = ch_1->send();
    LOG_DEBUG(
        getLogger(),
        "SYNC_FOR({}) {}",
        name, //
        result ? "matched waitAndPause(), paused until calling next()..." : "cancelled");
    if (!result)
        return;
    result = ch_2->recv();
    LOG_DEBUG(getLogger(), "SYNC_FOR({}) {}", name, result ? "done" : "cancelled");
}

#else

void SyncPointCtl::enable(const char *) {}

void SyncPointCtl::disable(const char *) {}

void SyncPointCtl::waitAndPause(const char *) {}

void SyncPointCtl::next(const char *) {}

void SyncPointCtl::sync(const char *) {}

#endif

SyncPointScopeGuard SyncPointCtl::enableInScope(const char * name)
{
    return SyncPointScopeGuard(name);
}

} // namespace DB
