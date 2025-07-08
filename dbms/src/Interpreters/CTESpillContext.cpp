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

#include <Common/Exception.h>
#include <Interpreters/CTESpillContext.h>

#include <mutex>

namespace DB
{
SpillerPtr CTESpillContext::getSpiller(size_t partition_id, size_t spill_id)
{
    SpillConfig config(
        this->spill_config.spill_dir,
        fmt::format("cte_spill_{}_{}", partition_id, spill_id),
        this->spill_config.max_cached_data_bytes_in_spiller,
        this->spill_config.max_spilled_rows_per_file,
        this->spill_config.max_spilled_bytes_per_file,
        this->spill_config.file_provider,
        this->spill_config.for_all_constant_max_streams,
        this->spill_config.for_all_constant_block_size);

    return std::make_unique<Spiller>(config, false, this->partition_num, this->spill_block_schema, this->log);
}
} // namespace DB
