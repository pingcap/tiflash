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

#include <Common/Exception.h>
#include <Flash/Pipeline/PipelineExecStatus.h>
#include <assert.h>

namespace DB
{
std::string PipelineExecStatus::getErrMsg()
{
    std::lock_guard lock(mu);
    return err_msg;
}

void PipelineExecStatus::toError(std::string && err_msg_)
{
    {
        std::lock_guard lock(mu);
        err_msg = err_msg_.empty() ? "error without err msg" : std::move(err_msg_);
    }
    cancel();
}

void PipelineExecStatus::wait()
{
    while (active_pipeline_count != 0)
    {
        std::unique_lock lock(mu);
        cv.wait(lock);
    }
}

void PipelineExecStatus::addActivePipeline()
{
    ++active_pipeline_count;
}

void PipelineExecStatus::completePipeline()
{
    auto pre_sub_count = active_pipeline_count.fetch_sub(1);
    assert(pre_sub_count >= 1);
    if (1 == pre_sub_count)
        cv.notify_one();
}

void PipelineExecStatus::cancel()
{
    is_cancelled = true;
}
} // namespace DB
