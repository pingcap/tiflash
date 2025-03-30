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

#include <Core/Block.h>
#include <Flash/Coprocessor/WaitResult.h>
#include <common/types.h>
#include <tipb/select.pb.h>

namespace DB
{
class DAGContext;

enum class WriteResult
{
    Done,
    NeedWaitForPolling,
    NeedWaitForNotify,
};

class DAGResponseWriter
{
public:
    DAGResponseWriter(Int64 records_per_chunk_, DAGContext & dag_context_);
    /// prepared with sample block
    virtual void prepare(const Block &){};
    virtual WriteResult write(const Block & block) = 0;

    virtual WaitResult waitForWritable() const { return WaitResult::Ready; }

    /// flush cached blocks for batch writer
    virtual WriteResult flush() = 0;

    /// if hasPendingFlush is true, need to flush before write
    // hasPendingFlush can be true only in pipeline mode
    bool hasPendingFlush() const { return has_pending_flush; }

    virtual ~DAGResponseWriter() = default;

protected:
    Int64 records_per_chunk;
    DAGContext & dag_context;
    bool has_pending_flush = false;
};

} // namespace DB
