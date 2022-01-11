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

    static bool isMatch(const tipb::Executor * executor)
    {
        return executor->has_exchange_receiver();
    }
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
};
} // namespace DB