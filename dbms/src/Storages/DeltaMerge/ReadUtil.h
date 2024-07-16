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

#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{

/** Read the next block.
  * Read from the stable first, then read from the delta.
  * 
  * Return: <Block, from_delta>
  * the block and a flag indicating whether the block is from the delta.
  */
std::pair<Block, bool> readBlock(SkippableBlockInputStreamPtr & stable, SkippableBlockInputStreamPtr & delta);
std::pair<Block, bool> readBlock(
    SkippableBlockInputStreamPtr & stable,
    SkippableBlockInputStreamPtr & delta,
    FilterPtr & res_filter,
    bool return_filter);

/** Skip the next block.
  * Return the number of rows of the next block.
  */
size_t skipBlock(SkippableBlockInputStreamPtr & stable, SkippableBlockInputStreamPtr & delta);

/** Read the next block with filter.
  * Read from the stable first, then read from the delta.
  * 
  * Return: <Block, from_delta>
  * The block containing only the rows that pass the filter and a flag indicating whether the block is from the delta.
  */
std::pair<Block, bool> readBlockWithFilter(
    SkippableBlockInputStreamPtr & stable,
    SkippableBlockInputStreamPtr & delta,
    const IColumn::Filter & filter,
    FilterPtr & res_filter,
    bool return_filter);
} // namespace DB::DM
