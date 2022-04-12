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

#include <Core/Block.h>
#include <Core/Names.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/Collator.h>
#include <tipb/executor.pb.h>

namespace DB::AggregationInterpreterHelper
{
Aggregator::Params buildAggregatorParams(
    const Context & context,
    const Block & before_agg_header,
    size_t before_agg_streams_size,
    Names & key_names,
    TiDB::TiDBCollators & collators,
    AggregateDescriptions & aggregate_descriptions,
    bool is_final_agg);

bool isFinalAgg(const tipb::Aggregation & aggregation);

bool isGroupByCollationSensitive(const Context & context);
} // namespace DB::AggregationInterpreterHelper
