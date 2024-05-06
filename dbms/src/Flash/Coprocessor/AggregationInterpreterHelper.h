// Copyright 2023 PingCAP, Inc.
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
#include <tipb/executor.pb.h>

namespace DB
{
class Context;

namespace AggregationInterpreterHelper
{
// Judge if the input of this sum is partial result.
bool isSumOnPartialResults(const tipb::Expr & expr);

bool isFinalAgg(const tipb::Aggregation & aggregation);

bool isGroupByCollationSensitive(const Context & context);

std::shared_ptr<Aggregator::Params> buildParams(
    const Context & context,
    const Block & before_agg_header,
    size_t before_agg_streams_size,
    size_t agg_streams_size,
    const Names & key_names,
    const std::unordered_map<String, String> & key_ref_agg_func,
    const std::unordered_map<String, String> & agg_func_ref_key,
    const TiDB::TiDBCollators & collators,
    const AggregateDescriptions & aggregate_descriptions,
    bool is_final_agg,
    const SpillConfig & spill_config);

void fillArgColumnNumbers(AggregateDescriptions & aggregate_descriptions, const Block & before_agg_header);
} // namespace AggregationInterpreterHelper
} // namespace DB
