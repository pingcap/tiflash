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

#pragma once

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/Block.h>
#include <Core/OperatorSpillContext.h>
#include <Core/SpillConfig.h>
#include <Core/Spiller.h>

namespace DB
{
class CTESpillContext
{
public:
    CTESpillContext(
        size_t partition_num_,
        const SpillConfig & spill_config_,
        const Block & spill_block_schema_,
        const String & query_id_and_cte_id_)
        : partition_num(partition_num_)
        , spill_config(spill_config_)
        , spill_block_schema(spill_block_schema_)
        , query_id_and_cte_id(query_id_and_cte_id_)
        , log(Logger::get(query_id_and_cte_id_))
    {}

    SpillerSharedPtr getSpillAt(size_t idx);

private:
    std::mutex mu;
    size_t partition_num;
    SpillConfig spill_config;
    Block spill_block_schema;

    std::vector<SpillerSharedPtr> spillers;
    String query_id_and_cte_id;
    LoggerPtr log;
};
} // namespace DB
