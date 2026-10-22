// Copyright 2026 PingCAP, Inc.
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

#include <chrono>
#include <mutex>
#include <optional>

namespace DB
{
// A successful probe promises that tasks dispatched afterwards can still reach this node.
// Serialize that promise with shutdown so an available response cannot escape the protection window.
template <typename Clock = std::chrono::steady_clock>
class MPPTaskShutdownGuard
{
public:
    static constexpr std::chrono::seconds DEFAULT_PROTECTION_WINDOW{60};

    bool checkAvailable()
    {
        std::lock_guard lock(mu);
        if (!is_available)
            return false;
        last_probe_time = Clock::now();
        return true;
    }

    void setUnavailable()
    {
        std::lock_guard lock(mu);
        is_available = false;
    }

    // Once unavailable, failed probes cannot renew this window.
    typename Clock::duration getRemainingProtection(
        std::chrono::seconds protection_window = DEFAULT_PROTECTION_WINDOW) const
    {
        std::lock_guard lock(mu);
        if (!last_probe_time)
            return Clock::duration::zero();
        const auto elapsed = Clock::now() - *last_probe_time;
        return elapsed < protection_window ? protection_window - elapsed : Clock::duration::zero();
    }

private:
    mutable std::mutex mu;
    bool is_available = true;
    std::optional<typename Clock::time_point> last_probe_time;
};
} // namespace DB
