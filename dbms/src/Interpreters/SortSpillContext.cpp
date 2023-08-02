// Copyright 2023 PingCAP, Ltd.
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

#include <Interpreters/SortSpillContext.h>

namespace DB
{
SortSpillContext::SortSpillContext(const SpillConfig & spill_config_, UInt64 operator_spill_threshold_, const LoggerPtr & log)
    : OperatorSpillContext(operator_spill_threshold_, "sort", log)
    , spill_config(spill_config_)
{}

void SortSpillContext::buildSpiller(const Block & input_schema)
{
    spiller = std::make_unique<Spiller>(spill_config, true, 1, input_schema, log);
}

bool SortSpillContext::updateRevocableMemory(Int64 new_value)
{
    if (!in_spillable_stage)
        return false;
    revocable_memory = new_value;
    if (enable_spill && operator_spill_threshold > 0 && revocable_memory > static_cast<Int64>(operator_spill_threshold))
    {
        revocable_memory = 0;
        return true;
    }
    return false;
}
} // namespace DB
