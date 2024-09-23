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

#include <Common/Stopwatch.h>
#include <Flash/Mpp/ReceivedMessageQueue.h>

namespace DB
{
struct LocalRequestHandler
{
    LocalRequestHandler(
        std::function<void(bool, const String &)> && notify_write_done_,
        std::function<void()> && notify_close_,
        std::function<void()> && add_local_conn_num_,
        const std::string & req_info_,
        ReceivedMessageQueue * msg_queue_)
        : notify_write_done(std::move(notify_write_done_))
        , notify_close(std::move(notify_close_))
        , add_local_conn_num(std::move(add_local_conn_num_))
        , req_info(req_info_)
        , msg_queue(msg_queue_)
    {}

    template <bool is_force>
    bool write(size_t source_index, const TrackedMppDataPacketPtr & tracked_packet)
    {
        return msg_queue->pushPacket<is_force>(source_index, req_info, tracked_packet, ReceiverMode::Local);
    }

    bool isWritable() const { return msg_queue->isWritable(); }
    void notifyNextPipelineWriter() const { return msg_queue->notifyNextPipelineWriter(); }

    void registerPipeWriteTask(TaskPtr && task) const { msg_queue->registerPipeWriteTask(std::move(task)); }

    void writeDone(bool meet_error, const String & local_err_msg) const
    {
        notify_write_done(meet_error, local_err_msg);
    }

    void closeConnection() const { notify_close(); }

    void setAlive() const { add_local_conn_num(); }

    void recordWaitingTaskTime() { waiting_task_time = watch.elapsedMilliseconds(); }

    UInt64 getTotalElapsedTime() const { return watch.elapsedMilliseconds(); }

    UInt64 getWaitingTaskTime() const { return waiting_task_time; }

    std::function<void(bool, const String &)> notify_write_done;
    std::function<void()> notify_close;
    std::function<void()> add_local_conn_num;
    const std::string req_info;
    ReceivedMessageQueue * msg_queue;
    UInt64 waiting_task_time = 0;
    Stopwatch watch;
};
} // namespace DB
