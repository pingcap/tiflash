#pragma once

#include <Flash/Coprocessor/DAGInterpreterBase.h>

namespace DB
{
class ProjectionInterpreter : public DAGInterpreterBase
{
public:
    ProjectionInterpreter(
        Context & context_,
        const DAGPipelinePtr & input_pipeline_,
        const DAGQueryBlock & query_block_,
        size_t max_streams_,
        bool keep_session_timezone_info_,
        const LogWithPrefixPtr & log_);

private:
    void executeImpl(DAGPipelinePtr & pipeline) override;

    DAGPipelinePtr input_pipeline;
};
} // namespace DB
