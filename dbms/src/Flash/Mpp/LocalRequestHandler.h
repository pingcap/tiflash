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
        std::function<void(bool, const String &)> && notify_write_done_,
        std::function<void()> && notify_close_,
        std::function<void()> && add_local_conn_num_,
        ReceiverChannelWriter && channel_writer_)
        : recv_mem_tracker(recv_mem_tracker_)
        , notify_write_done(std::move(notify_write_done_))
        , notify_close(std::move(notify_close_))
        , add_local_conn_num(std::move(add_local_conn_num_))
        , channel_writer(std::move(channel_writer_))
    {}

    template <bool enable_fine_grained_shuffle, bool non_blocking>
    bool write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
    {
        return channel_writer.write<enable_fine_grained_shuffle, non_blocking>(source_index, tracked_packet);
    }

    bool isReadyForWrite() const
    {
        return channel_writer.isReadyForWrite();
    }

    void writeDone(bool meet_error, const String & local_err_msg) const
    {
        notify_write_done(meet_error, local_err_msg);
    }

    void closeConnection() const
    {
        notify_close();
    }

    void setAlive() const
    {
        add_local_conn_num();
    }

    MemoryTracker * recv_mem_tracker;
    std::function<void(bool, const String &)> notify_write_done;
    std::function<void()> notify_close;
    std::function<void()> add_local_conn_num;
    ReceiverChannelWriter channel_writer;
};
} // namespace DB
