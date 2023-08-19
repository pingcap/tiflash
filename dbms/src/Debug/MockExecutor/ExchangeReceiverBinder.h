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

#include <Debug/MockExecutor/ExecutorBinder.h>

namespace DB::mock
{
class ExchangeReceiverBinder : public ExecutorBinder
{
public:
    ExchangeReceiverBinder(size_t & index, const DAGSchema & output, uint64_t fine_grained_shuffle_stream_count_ = 0)
        : ExecutorBinder(index, "exchange_receiver_" + std::to_string(index), output)
        , fine_grained_shuffle_stream_count(fine_grained_shuffle_stream_count_)
    {}

    bool toTiPBExecutor(tipb::Executor * tipb_executor, int32_t collator_id, const MPPInfo & mpp_info, const Context &) override;

    void columnPrune(std::unordered_set<String> &) override {}

private:
    TaskMetas task_metas;
    uint64_t fine_grained_shuffle_stream_count;
};

ExecutorBinderPtr compileExchangeReceiver(size_t & executor_index, DAGSchema schema, uint64_t fine_grained_shuffle_stream_count);
} // namespace DB::mock
