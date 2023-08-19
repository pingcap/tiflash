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

#include <Common/Stopwatch.h>
#include <Core/Types.h>

#include <vector>

#if __APPLE__
#include <common/apple_rt.h>
#endif

namespace DB
{
class Block;
class ReadBuffer;
class WriteBuffer;
class IProfilingBlockInputStream;

/// Information for profiling. See IProfilingBlockInputStream.h
struct BlockStreamProfileInfo
{
    /// Info about stream object this profile info refers to.
    IProfilingBlockInputStream * parent = nullptr;

    bool started = false;
    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE}; /// Time with waiting time

    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;
    size_t allocated_bytes = 0;
    // execution time is the total time spent on current stream and all its children streams
    // note that it is different from total_stopwatch.elapsed(), which includes not only the
    // time spent on current stream and all its children streams, but also the time of its
    // parent streams
    UInt64 execution_time = 0;

    using BlockStreamProfileInfos = std::vector<const BlockStreamProfileInfo *>;

    /// Collect BlockStreamProfileInfo for the nearest sources in the tree named `name`. Example; collect all info for PartialSorting streams.
    void collectInfosForStreamsWithName(const char * name, BlockStreamProfileInfos & res) const;

    /** Get the number of rows if there were no LIMIT.
      * If there is no LIMIT, 0 is returned.
      * If the query does not contain ORDER BY, the number can be underestimated - return the number of rows in blocks that were read before LIMIT reached.
      * If the query contains an ORDER BY, then returns the exact number of rows as if LIMIT is removed from query.
      */
    size_t getRowsBeforeLimit() const;
    bool hasAppliedLimit() const;

    void update(Block & block);

    void updateExecutionTime(UInt64 time) { execution_time += time; }

    /// Binary serialization and deserialization of main fields.
    /// Writes only main fields i.e. fields that required by internal transmission protocol.
    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

    /// Sets main fields from other object (see methods above).
    /// If skip_block_size_info if true, then rows, bytes and block fields are ignored.
    void setFrom(const BlockStreamProfileInfo & rhs, bool skip_block_size_info);

private:
    void calculateRowsBeforeLimit() const;

    /// For these fields we make accessors, because they must be calculated beforehand.
    mutable bool applied_limit = false; /// Whether LIMIT was applied
    mutable size_t rows_before_limit = 0;
    mutable bool calculated_rows_before_limit = false; /// Whether the field rows_before_limit was calculated
};

} // namespace DB
