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

#include <Columns/IColumn.h>


namespace DB
{
/** Create a 'selector' to be used in IColumn::scatter method
  *  according to sharding scheme and values of column with sharding key.
  *
  * Each of num_shards has its weight. Weight must be small.
  * 'slots' contains weight elements for each shard, in total - sum of all weight elements.
  *
  * Values of column get divided to sum_weight, and modulo of division
  *  will map to corresponding shard through 'slots' array.
  *
  * Column must have integer type.
  * T is type of column elements.
  */
template <typename T>
IColumn::Selector createBlockSelector(const IColumn & column, const std::vector<UInt64> & slots);

} // namespace DB
