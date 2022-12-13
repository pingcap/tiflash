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

#include <atomic>
#include <mutex>

namespace DB
{
struct PipelineExecStatus
{
    std::mutex mu;
    std::condition_variable cv;
    std::atomic_int64_t active_pipeline_count{0};
    std::atomic_bool is_cancelled{false};
    std::string err_msg;

    std::string getErrMsg();

    void addActivePipeline();

    void toError(std::string && err_msg_);

    void wait();

    void completePipeline();

    void cancel();

    bool isCancelled();
};
} // namespace DB
