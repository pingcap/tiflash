// Copyright 2025 PingCAP, Inc.
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

#include <Interpreters/CTESpillContext.h>
#include <Common/Exception.h>

#include <mutex>

namespace DB
{
SpillerSharedPtr CTESpillContext::getSpillAt(size_t idx)
{
    std::lock_guard<std::mutex> lock(this->mu);
    auto spiller_num = this->spillers.size();

    // The spiller whose idx is lower that the parameter idx must have been created
    RUNTIME_CHECK_MSG(idx <= spiller_num, "idx: {}, spiller_num: {}", idx, spiller_num);

    if (idx < spiller_num)
        return this->spillers[idx];
    
    
    return this->spillers[idx];
}
} // namespace DB
