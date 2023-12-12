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

#include <Core/Defines.h>
#include <common/types.h>

#include <atomic>


namespace DB
{
class ReadBuffer;
class WriteBuffer;


/// See Progress.
struct ProgressValues
{
    size_t rows;
    size_t bytes;
    size_t total_rows;

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
};


/** Progress of query execution.
  * Values, transferred over network are deltas - how much was done after previously sent value.
  * The same struct is also used for summarized values.
  */
struct Progress
{
    std::atomic<size_t> rows{0}; /// Rows (source) processed.
    std::atomic<size_t> bytes{0}; /// Bytes (uncompressed, source) processed.

    /** How much rows must be processed, in total, approximately. Non-zero value is sent when there is information about some new part of job.
      * Received values must be summed to get estimate of total rows to process.
      * Used for rendering progress bar on client.
      */
    std::atomic<size_t> total_rows{0};

    Progress() = default;
    Progress(size_t rows_, size_t bytes_, size_t total_rows_ = 0)
        : rows(rows_)
        , bytes(bytes_)
        , total_rows(total_rows_)
    {}

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

    /// Each value separately is changed atomically (but not whole object).
    void incrementPiecewiseAtomically(const Progress & rhs)
    {
        rows += rhs.rows;
        bytes += rhs.bytes;
        total_rows += rhs.total_rows;
    }

    void reset()
    {
        rows = 0;
        bytes = 0;
        total_rows = 0;
    }

    ProgressValues getValues() const
    {
        ProgressValues res{};

        res.rows = rows.load(std::memory_order_relaxed);
        res.bytes = bytes.load(std::memory_order_relaxed);
        res.total_rows = total_rows.load(std::memory_order_relaxed);

        return res;
    }

    ProgressValues fetchAndResetPiecewiseAtomically()
    {
        ProgressValues res{};

        res.rows = rows.fetch_and(0);
        res.bytes = bytes.fetch_and(0);
        res.total_rows = total_rows.fetch_and(0);

        return res;
    }

    Progress & operator=(Progress && other)
    {
        rows = other.rows.load(std::memory_order_relaxed);
        bytes = other.bytes.load(std::memory_order_relaxed);
        total_rows = other.total_rows.load(std::memory_order_relaxed);

        return *this;
    }

    Progress(Progress && other) { *this = std::move(other); }
};


} // namespace DB
