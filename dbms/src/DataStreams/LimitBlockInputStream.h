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
#include <DataStreams/LimitTransformAction.h>

namespace DB
{
/** Implements the LIMIT relational operation.
  */
class LimitBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "Limit";

public:
    /** If always_read_till_end = false (by default), then after reading enough data,
      *  returns an empty block, and this causes the query to be canceled.
      * If always_read_till_end = true - reads all the data to the end, but ignores them. This is necessary in rare cases:
      *  when otherwise, due to the cancellation of the request, we would not have received the data for GROUP BY WITH TOTALS from the remote server.
      */
    LimitBlockInputStream(const BlockInputStreamPtr & input, size_t limit_, size_t offset_, const String & req_id);

    String getName() const override { return NAME; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;
    void appendInfo(FmtBuffer & buffer) const override;

private:
    LoggerPtr log;
    size_t limit;
    size_t offset;
    /// how many lines were read, including the last read block
    size_t pos = 0;
};

} // namespace DB
