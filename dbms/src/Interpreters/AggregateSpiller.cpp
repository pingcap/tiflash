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

#include <Interpreters/AggregateSpiller.h>

namespace DB
{
AggregateSpiller::AggregateSpiller(
    const Aggregator::Params & params, 
    const Block & header_, 
    const AggregatedDataVariants::Type & type)
    : is_local_agg(params.is_local_agg)
    , header(header_)
    // for local agg, use sort base spiller.
    // for non local agg, use partition base spiller.
    , partition_num(params.is_local_agg ? 1 : AggregatedDataVariants::getBucketNumberForTwoLevelHashTable(method_chosen))
    , spiller(std::make_unique<Spiller>(params.spill_config, /*is_input_sorted=*/is_local_agg, partition_num, header, log))
{}

void AggregateSpiller::finishSpill()
{
    spiller->finishSpill();
}

bool AggregateSpiller::hasSpilledData()
{
    return spiller->hasSpilledData();
}


} // namespace DB
