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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Interpreters/Context.h>

namespace DB
{
void Pipeline::execute()
{
    stream->readPrefix();
    while (stream->read())
    {
    }
    stream->readSuffix();
}

void Pipeline::prepare(Context & context, size_t max_streams)
{
    assert(plan_node);
    DAGPipeline pipeline;
    plan_node->transform(pipeline, context, max_streams);
    executeUnion(pipeline, max_streams, log, /*ignore_block=*/true, "for pipeline");
    stream = pipeline.firstStream();
    assert(stream);
    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()); p_stream)
    {
        p_stream->setProgressCallback(context.getProgressCallback());
        p_stream->setProcessListElement(context.getProcessListElement());
    }

    auto stream_str = [&]() {
        FmtBuffer fb;
        stream->dumpTree(fb);
        return fb.toString();
    };
    LOG_FMT_DEBUG(log, "pipeline stream:\n{}", stream_str());
}

void Pipeline::cancel(bool is_kill)
{
    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()); p_stream)
    {
        p_stream->cancel(is_kill);
    }
}
} // namespace DB
