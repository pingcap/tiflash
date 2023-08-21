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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>

namespace DB
{
DAGResponseWriter::DAGResponseWriter(Int64 records_per_chunk_, DAGContext & dag_context_)
    : records_per_chunk(records_per_chunk_)
    , dag_context(dag_context_)
{
    if (dag_context.encode_type == tipb::EncodeType::TypeCHBlock)
    {
        records_per_chunk = -1;
    }
    if (dag_context.encode_type != tipb::EncodeType::TypeCHBlock
        && dag_context.encode_type != tipb::EncodeType::TypeChunk
        && dag_context.encode_type != tipb::EncodeType::TypeDefault)
    {
        throw TiFlashException(
            "Only Default/Arrow/CHBlock encode type is supported in DAGResponseWriter.",
            Errors::Coprocessor::Unimplemented);
    }
}

} // namespace DB
