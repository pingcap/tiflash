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

#include <Flash/Coprocessor/ExecutionSummary.h>

namespace DB
{

void ExecutionSummary::merge(const ExecutionSummary & other, bool streaming_call)
{
    if (streaming_call)
    {
        time_processed_ns = std::max(time_processed_ns, other.time_processed_ns);
        num_produced_rows = std::max(num_produced_rows, other.num_produced_rows);
        num_iterations = std::max(num_iterations, other.num_iterations);
        concurrency = std::max(concurrency, other.concurrency);
        scan_context->merge(*other.scan_context);
    }
    else
    {
        time_processed_ns = std::max(time_processed_ns, other.time_processed_ns);
        num_produced_rows += other.num_produced_rows;
        num_iterations += other.num_iterations;
        concurrency += other.concurrency;
        scan_context->merge(*other.scan_context);
    }
}
} // namespace DB
