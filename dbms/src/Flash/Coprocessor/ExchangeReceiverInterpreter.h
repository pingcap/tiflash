#pragma once

#include <Flash/Coprocessor/DAGInterpreterBase.h>
#include <Flash/Mpp/ExchangeReceiver.h>

namespace DB
{
class ExchangeReceiverInterpreter : public DAGInterpreterBase
{
public:
    ExchangeReceiverInterpreter(
        Context & context_,
        const DAGQueryBlock & query_block_,
        size_t max_streams_,
        bool keep_session_timezone_info_,
        const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map,
        const LogWithPrefixPtr & log_);

private:
    void executeImpl(DAGPipelinePtr & pipeline) override;

    const std::unordered_map<String, std::shared_ptr<ExchangeReceiver>> & exchange_receiver_map;
    const LogWithPrefixPtr log;
};
} // namespace DB
