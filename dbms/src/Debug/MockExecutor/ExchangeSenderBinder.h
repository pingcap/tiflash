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

#include <Debug/MockExecutor/ExecutorBinder.h>

namespace DB::mock
{
class ExchangeSenderBinder : public ExecutorBinder
{
public:
    ExchangeSenderBinder(size_t & index, const DAGSchema & output, tipb::ExchangeType type_, const std::vector<size_t> & partition_keys_ = {})
        : ExecutorBinder(index, "exchange_sender_" + std::to_string(index), output)
        , type(type_)
        , partition_keys(partition_keys_)
    {}

    bool toTiPBExecutor(tipb::Executor * tipb_executor, int32_t collator_id, const MPPInfo & mpp_info, const Context & context) override;

    void columnPrune(std::unordered_set<String> &) override {}

    tipb::ExchangeType getType() const;

private:
    tipb::ExchangeType type;
    TaskMetas task_metas;
    std::vector<size_t> partition_keys;
};

ExecutorBinderPtr compileExchangeSender(ExecutorBinderPtr input, size_t & executor_index, tipb::ExchangeType exchange_type);
} // namespace DB::mock
