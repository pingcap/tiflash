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

#include <Common/ThreadManager.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Mpp/ExchangeReceiverCommon.h>
#include <Flash/Mpp/GRPCReceiverContext.h>
#include <Interpreters/Context.h>

#include <future>
#include <mutex>
#include <thread>

namespace DB
{
template <typename RPCContext>
class ExchangeReceiverWithRPCContext : public ExchangeReceiverBase
{
public:
    ExchangeReceiverWithRPCContext(
        std::shared_ptr<RPCContext> rpc_context_,
        size_t source_num_,
        size_t max_streams_,
        const String & req_id,
        const String & executor_id,
        uint64_t fine_grained_shuffle_stream_count,
        const std::vector<StorageDisaggregated::RequestAndRegionIDs> & disaggregated_dispatch_reqs_ = {});

    ~ExchangeReceiverWithRPCContext() = default;

    void cancel();

private:
    using Request = typename RPCContext::Request;

    // Template argument enable_fine_grained_shuffle will be setup properly in setUpConnection().
    template <bool enable_fine_grained_shuffle>
    void readLoop(const Request & req);
    template <bool enable_fine_grained_shuffle>
    void reactor(const std::vector<Request> & async_requests);
    void setUpConnection();

private:
    std::shared_ptr<RPCContext> rpc_context;
};

class ExchangeReceiver : public ExchangeReceiverWithRPCContext<GRPCReceiverContext>
{
public:
    using Base = ExchangeReceiverWithRPCContext<GRPCReceiverContext>;
    using Base::Base;
};

} // namespace DB
