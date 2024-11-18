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

#include <Flash/Statistics/ConnectionProfileInfo.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct ExchangeReceiveDetail : public ConnectionProfileInfo
{
    Int64 receiver_source_task_id;

    explicit ExchangeReceiveDetail(Int64 receiver_source_task_id_)
        : receiver_source_task_id(receiver_source_task_id_)
    {}

    String toJson() const;
};

struct ExchangeReceiverImpl
{
    static constexpr bool has_extra_info = true;

    static constexpr auto type = "ExchangeReceiver";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_exchange_receiver(); }

    static bool isSourceExecutor() { return true; }
};

using ExchangeReceiverStatisticsBase = ExecutorStatistics<ExchangeReceiverImpl>;

class ExchangeReceiverStatistics : public ExchangeReceiverStatisticsBase
{
public:
    ExchangeReceiverStatistics(const tipb::Executor * executor, DAGContext & dag_context_);

private:
    std::vector<Int64> receiver_source_task_ids;
    size_t partition_num;

    std::vector<ExchangeReceiveDetail> exchange_receive_details;

protected:
    void appendExtraJson(FmtBuffer &) const override;
    void collectExtraRuntimeDetail() override;

private:
    void updateExchangeReceiveDetail(const std::vector<ConnectionProfileInfo> & connection_profile_infos);
};
} // namespace DB
