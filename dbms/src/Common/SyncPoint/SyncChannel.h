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

#include <Common/SyncPoint/Ctl.h>
#include <Common/nocopyable.h>

#include <condition_variable>
#include <mutex>

namespace DB
{

class SyncPointCtl::SyncChannel
{
public:
    /**
     * Copy and move are disallowed. A single SyncChannel instance can be shared for multiple threads.
     */
    DISALLOW_COPY_AND_MOVE(SyncChannel);

    explicit SyncChannel() = default;

    ~SyncChannel()
    {
        close();
        // It is possible that there are `recv()` or `send()` running or blocked.
        // They should exit when receiving the close signal from `cv`.
        // Let's simply wait them to finish. This ensures that memory is always released after
        // no existing function is running anymore.
        while (pending_op > 0) {}
    }

    void close()
    {
        pending_op++;
        {
            std::lock_guard lock_cv(m_cv);
            is_closing = true;
            cv.notify_all();
        }
        pending_op--;
    }

    /**
     * Blocked until one send() is called, or channel is closed.
     */
    bool recv()
    {
        pending_op++;
        // wrap a scope for locks to ensure no more access to the member after pending_op--
        auto is_wait_fulfilled = [this]() {
            std::unique_lock lock_recv(m_recv);
            std::unique_lock lock_cv(m_cv);
            has_receiver = true;
            cv.notify_all();
            cv.wait(lock_cv, [this] { return has_data || is_closing; });
            if (is_closing)
                return false;
            has_data = false; // consumes one data
            has_receiver = false;
            return true;
        }();
        pending_op--;
        return is_wait_fulfilled;
    }

    /**
     * Blocked until there is a receiver, or channel is closed.
     * Queued if multiple send() is called concurrently.
     */
    bool send()
    {
        pending_op++;
        auto is_wait_fulfilled = [this]() {
            std::unique_lock lock_send(m_send);
            std::unique_lock lock_cv(m_cv);
            cv.wait(lock_cv, [this] { return (has_receiver && !has_data) || is_closing; });
            if (is_closing)
                return false;
            has_data = true;
            cv.notify_all();
            return true;
        }();
        pending_op--;
        return is_wait_fulfilled;
    }

private:
    bool has_receiver = false;
    bool has_data = false;
    bool is_closing = false;

    std::atomic<int64_t> pending_op = 0;

    std::mutex m_send;
    std::mutex m_recv;
    std::mutex m_cv;
    std::condition_variable cv;
};

} // namespace DB
