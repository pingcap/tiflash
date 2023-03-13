// Copyright 2023 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Disaggregated/RNPageReceiver.h>

#include <future>

namespace DB
{
class RNPagePreparer;
using RNPagePreparerPtr = std::shared_ptr<RNPagePreparer>;

// `RNPagePreparer` starts background threads to keep
// - popping message from `RNPageReceiver.msg_channel`
// - persisting pages for ColumnFileTiny into RN's LocalPageCache
// - decoding blocks for ColumnFileInMemory into RN's memory
// - do some extra preparation work after all data received from
//   all write nodes
class RNPagePreparer
{
public:
    RNPagePreparer(
        DM::RNRemoteReadTaskPtr remote_read_tasks_,
        std::shared_ptr<RNPageReceiver> receiver_,
        const DM::ColumnDefinesPtr & columns_to_read,
        size_t max_streams_,
        const String & req_id,
        const String & executor_id,
        bool do_prepare_);

    ~RNPagePreparer() noexcept;

private:
    void prepareLoop(size_t idx);

    bool consumeOneResult(const LoggerPtr & log);

    void downloadDone(bool meet_error, const String & local_err_msg, const LoggerPtr & log);

private:
    const size_t threads_num;
    const bool do_prepare;
    DM::RNRemoteReadTaskPtr remote_read_tasks;
    std::shared_ptr<RNPageReceiver> receiver;
    std::vector<std::future<void>> persist_threads;

    std::unique_ptr<CHBlockChunkCodec> decoder_ptr;

    std::mutex mu;
    Int32 live_persisters;
    PageReceiverState state;
    String err_msg;

    std::atomic<size_t> total_rows;
    std::atomic<size_t> total_pages;

    Stopwatch watch;
    LoggerPtr exc_log;
};

} // namespace DB
