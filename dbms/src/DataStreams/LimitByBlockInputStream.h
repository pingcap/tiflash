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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

/** Implements LIMIT BY clause witch can be used to obtain a "top N by subgroup".
  *
  * For example, if you have table T like this (Num: 1 1 3 3 3 4 4 5 7 7 7 7),
  * the query SELECT Num FROM T LIMIT 2 BY Num
  *    will give you the following result: (Num: 1 1 3 3 4 4 5 7 7).
  */
class LimitByBlockInputStream : public IProfilingBlockInputStream
{
public:
    LimitByBlockInputStream(const BlockInputStreamPtr & input, size_t group_size_, const Names & columns);

    String getName() const override { return "LimitBy"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    ColumnRawPtrs getKeyColumns(Block & block) const;

private:
    using MapHashed = HashMap<UInt128, UInt64, TrivialHash>;

    const Names columns_names;
    const size_t group_size;
    MapHashed keys_counts;
};

}
