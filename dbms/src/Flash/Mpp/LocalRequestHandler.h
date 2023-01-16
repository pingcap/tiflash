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

#include <Flash/Mpp/ReceiverChannelWriter.h>

namespace DB
{
struct LocalRequestHandler
{
    LocalRequestHandler(
        MemoryTracker * recv_mem_tracker_,
        std::function<void(bool, const String &)> && notify_receiver_,
        std::function<void()> && notify_receiver_close_,
        ReceiverChannelWriter && channel_writer_)
        : recv_mem_tracker(recv_mem_tracker_)
        , notify_receiver_prepare(std::move(notify_receiver_))
        , notify_receiver_close(std::move(notify_receiver_close_))
        , channel_writer(std::move(channel_writer_))
    {}

    template <bool enable_fine_grained_shuffle>
    bool write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
    {
        return channel_writer.write<enable_fine_grained_shuffle>(source_index, tracked_packet);
    }

    void prepareToCloseLocalConnection(bool meet_error, const String & local_err_msg) const
    {
        notify_receiver_prepare(meet_error, local_err_msg);
    }

    void closeLocalConnection() const
    {
        notify_receiver_close();
    }

    MemoryTracker * recv_mem_tracker;
    std::function<void(bool, const String &)> notify_receiver_prepare;
    std::function<void()> notify_receiver_close;
    ReceiverChannelWriter channel_writer;
};
} // namespace DB
