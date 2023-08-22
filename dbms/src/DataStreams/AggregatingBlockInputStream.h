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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Aggregator.h>

namespace DB
{
/** Aggregates the stream of blocks using the specified key columns and aggregate functions.
  * Columns with aggregate functions adds to the end of the block.
  * If final = false, the aggregate functions are not finalized, that is, they are not replaced by their value, but contain an intermediate state of calculations.
  * This is necessary so that aggregation can continue (for example, by combining streams of partially aggregated data).
  */
class AggregatingBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "Aggregating";

public:
    /** keys are taken from the GROUP BY part of the query
      * Aggregate functions are searched everywhere in the expression.
      * Columns corresponding to keys and arguments of aggregate functions must already be computed.
      */
    AggregatingBlockInputStream(
        const BlockInputStreamPtr & input,
        const Aggregator::Params & params_,
        bool final_,
        const String & req_id,
        const RegisterOperatorSpillContext & register_operator_spill_context)
        : log(Logger::get(req_id))
        , params(params_)
        , aggregator(params, req_id, 1, register_operator_spill_context)
        , final(final_)
    {
        children.push_back(input);
    }

    String getName() const override { return NAME; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

    LoggerPtr log;

    Aggregator::Params params;
    Aggregator aggregator;
    bool final;

    bool executed = false;

    /** From here we will get the completed blocks after the aggregation. */
    std::unique_ptr<IBlockInputStream> impl;
};

} // namespace DB
