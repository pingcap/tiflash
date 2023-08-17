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

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

/** If the number of sources of `inputs` is greater than `width`,
  *  then glues the sources to each other (using ConcatBlockInputStream),
  *  so that the number of sources becomes no more than `width`.
  *
  * Trying to glue the sources with each other uniformly randomly.
  *  (to avoid overweighting if the distribution of the amount of data in different sources is subject to some pattern)
  */
BlockInputStreams narrowBlockInputStreams(BlockInputStreams & inputs, size_t width);

} // namespace DB
