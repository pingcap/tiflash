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

#include <Core/Block.h>
#include <Core/SortDescription.h>

namespace DB::SortHelper
{
/** Remove constant columns from block.
  */
void removeConstantsFromBlock(Block & block);

/** Remove constant columns from description.
  */
void removeConstantsFromSortDescription(const Block & header, SortDescription & description);

/** Returns true if the columns in description are all constants.
  */
bool isSortByConstants(const Block & header, const SortDescription & description);

/** Add into block, whose constant columns was removed by previous function,
  *  constant columns from header (which must have structure as before removal of constants from block).
  */
void enrichBlockWithConstants(Block & block, const Block & header);

} // namespace DB::SortHelper
