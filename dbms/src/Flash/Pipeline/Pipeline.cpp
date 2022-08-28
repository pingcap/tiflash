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

#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>

namespace DB
{
void Pipeline::execute(Context & context, size_t max_streams)
{
    assert(plan_node);
    DAGPipeline pipeline;
    plan_node->transform(pipeline, context, max_streams);
    executeUnion(pipeline, max_streams, log, /*ignore_block=*/true, "for pipeline");
    auto stream = pipeline.firstStream();
    assert(stream);

    stream->readPrefix();
    while (stream->read());
    stream->readSuffix();
}
}
