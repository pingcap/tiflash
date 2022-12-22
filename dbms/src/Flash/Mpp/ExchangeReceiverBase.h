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

// #pragma once

// #include <Common/FailPoint.h>
// #include <Common/MPMCQueue.h>
// #include <Common/MemoryTracker.h>
// #include <Common/ThreadManager.h>
// #include <Flash/Coprocessor/CHBlockChunkCodec.h>
// #include <Flash/Coprocessor/ChunkCodec.h>
// #include <Flash/Coprocessor/DecodeDetail.h>
// #include <Flash/Coprocessor/FineGrainedShuffle.h>
// #include <Flash/Mpp/ReceiverChannelWriter.h>
// #include <Storages/StorageDisaggregated.h>
// #include <common/defines.h>
// #include <kvproto/mpp.pb.h>
// #include <tipb/executor.pb.h>
// #include <tipb/select.pb.h>

// #include <cerrno>


// namespace DB
// {

// class ExchangeReceiverBase
// {
// public:
//     static constexpr bool is_streaming_reader = true;
//     static constexpr auto name = "ExchangeReceiver";

// public:
//     ExchangeReceiverBase(
//         size_t source_num_,
//         size_t max_streams_,
//         const String & req_id,
//         const String & executor_id,
//         uint64_t fine_grained_shuffle_stream_count_,
//         const std::vector<StorageDisaggregated::RequestAndRegionIDs> & disaggregated_dispatch_reqs_ = {})
//         : source_num(source_num_)
//         , enable_fine_grained_shuffle_flag(enableFineGrainedShuffle(fine_grained_shuffle_stream_count_))
//         , output_stream_count(enable_fine_grained_shuffle_flag ? std::min(max_streams_, fine_grained_shuffle_stream_count_) : max_streams_)
//         , max_buffer_size(std::max<size_t>(batch_packet_count, std::max(source_num, max_streams_) * 2))
//         , thread_manager(newThreadManager())
//         , live_connections(source_num)
//         , state(ExchangeReceiverState::NORMAL)
//         , exc_log(Logger::get(req_id, executor_id))
//         , local_conn_num(0)
//         , collected(false)
//         , memory_tracker(current_memory_tracker)
//         , disaggregated_dispatch_reqs(disaggregated_dispatch_reqs_)
//     {}

//     ~ExchangeReceiverBase();

//     void close();

//     const DAGSchema & getOutputSchema() const { return schema; }

//     ExchangeReceiverResult nextResult(
//         std::queue<Block> & block_queue,
//         const Block & header,
//         size_t stream_id,
//         std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

//     size_t getSourceNum() const { return source_num; }
//     uint64_t getFineGrainedShuffleStreamCount() const { return enable_fine_grained_shuffle_flag ? output_stream_count : 0; }
//     int getExternalThreadCnt() const { return thread_count; }
//     std::vector<MsgChannelPtr> & getMsgChannels() { return msg_channels; }

//     bool isFineGrainedShuffleEnabled() const { return enable_fine_grained_shuffle_flag; }

//     void connectionLocalDone(
//         bool meet_error,
//         const String & local_err_msg,
//         const LoggerPtr & log);

//     MemoryTracker * getMemoryTracker() const { return memory_tracker; }

//     std::atomic<Int64> * getDataSizeInQueue() { return &data_size_in_queue; }

// protected:
//     std::shared_ptr<MemoryTracker> mem_tracker;

//     bool setEndState(ExchangeReceiverState new_state);
//     String getStatusString();

//     ExchangeReceiverResult handleUnnormalChannel(
//         std::queue<Block> & block_queue,
//         std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

//     static DecodeDetail decodeChunks(
//         const std::shared_ptr<ReceivedMessage> & recv_msg,
//         std::queue<Block> & block_queue,
//         std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

//     void connectionDone(
//         bool meet_error,
//         const String & local_err_msg,
//         const LoggerPtr & log);

//     // Local tunnel sender holds the pointer of ExchangeReceiverBase, so we must
//     // ensure that ExchangeReceiverBase is destructed after all tunnel senders.
//     void waitAllLocalConnDone();

//     void finishAllMsgChannels();
//     void cancelAllMsgChannels();

//     ExchangeReceiverResult toDecodeResult(
//         std::queue<Block> & block_queue,
//         const Block & header,
//         const std::shared_ptr<ReceivedMessage> & recv_msg,
//         std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

// protected:
//     void prepareMsgChannels();

//     bool isReceiverForTiFlashStorage()
//     {
//         // If not empty, need to send MPPTask to tiflash_storage.
//         return !disaggregated_dispatch_reqs.empty();
//     }

//     const tipb::ExchangeReceiver pb_exchange_receiver;
//     const size_t source_num;
//     const ::mpp::TaskMeta task_meta;
//     const bool enable_fine_grained_shuffle_flag;
//     const size_t output_stream_count;
//     const size_t max_buffer_size;

//     std::shared_ptr<ThreadManager> thread_manager;
//     DAGSchema schema;

//     std::vector<MsgChannelPtr> msg_channels;

//     std::mutex mu;
//     /// should lock `mu` when visit these members
//     Int32 live_connections;
//     ExchangeReceiverState state;
//     String err_msg;

//     LoggerPtr exc_log;

//     Int32 local_conn_num;
//     std::condition_variable local_conn_cv;

//     bool collected = false;
//     int thread_count = 0;
//     MemoryTracker * memory_tracker;

//     std::atomic<Int64> data_size_in_queue{};

//     // For tiflash_compute node, need to send MPPTask to tiflash_storage node.
//     std::vector<StorageDisaggregated::RequestAndRegionIDs> disaggregated_dispatch_reqs;
// };


// } // namespace DB
