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

#include <unordered_map>


namespace DB
{

/** Convert one block structure to another:
  *
  * Leaves only necessary columns;
  *
  * Columns are searched in source first by name;
  *  and if there is no column with same name, then by position.
  *
  * Converting types of matching columns (with CAST function).
  *
  * Materializing columns which are const in source and non-const in result,
  *  throw if they are const in result and non const in source,
  *   or if they are const and have different values.
  */
class ConvertingBlockInputStream : public IProfilingBlockInputStream
{
public:
    enum class MatchColumnsMode
    {
        /// Require same number of columns in source and result. Match columns by corresponding positions, regardless to names.
        Position,
        /// Find columns in source by their names. Allow excessive columns in source.
        Name
    };

    ConvertingBlockInputStream(
        const Context & context,
        const BlockInputStreamPtr & input,
        const Block & result_header,
        MatchColumnsMode mode);

    String getName() const override { return "Converting"; }
    Block getHeader() const override { return header; }

private:
    Block readImpl() override;

    const Context & context;
    Block header;

    /// How to construct result block. Position in source block, where to get each column.
    using Conversion = std::vector<size_t>;
    Conversion conversion;
};

} // namespace DB
