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

#include <Flash/Coprocessor/DAGContext.h>
#include <tipb/select.pb.h>

namespace DB
{
class DAGResponseWriter
{
public:
    DAGResponseWriter(
        Int64 records_per_chunk_,
        DAGContext & dag_context_);
    /// prepared with sample block
    virtual void prepare(const Block &){};
    virtual void write(const Block & block) = 0;
    /// flush cached blocks for batch writer
    virtual void flush() = 0;
    virtual ~DAGResponseWriter() = default;
    const DAGContext & dagContext() const { return dag_context; }

protected:
    Int64 records_per_chunk;
    DAGContext & dag_context;
};

} // namespace DB
