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

#include <DataStreams/BlockIO.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <IO/Buffer/ConcatReadBuffer.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


InputStreamFromASTInsertQuery::InputStreamFromASTInsertQuery(
    const ASTPtr & ast,
    ReadBuffer & input_buffer_tail_part,
    const BlockIO & streams,
    Context & context)
{
    const auto * ast_insert_query = dynamic_cast<const ASTInsertQuery *>(ast.get());

    if (!ast_insert_query)
        throw Exception(
            "Logical error: query requires data to insert, but it is not INSERT query",
            ErrorCodes::LOGICAL_ERROR);

    String format = ast_insert_query->format;
    if (format.empty())
        format = "Values";

    /// Data could be in parsed (ast_insert_query.data) and in not parsed yet (input_buffer_tail_part) part of query.

    input_buffer_ast_part = std::make_unique<ReadBufferFromMemory>(
        ast_insert_query->data,
        ast_insert_query->data ? ast_insert_query->end - ast_insert_query->data : 0);

    ConcatReadBuffer::ReadBuffers buffers;
    if (ast_insert_query->data)
        buffers.push_back(input_buffer_ast_part.get());
    buffers.push_back(&input_buffer_tail_part);

    /** NOTE Must not read from 'input_buffer_tail_part' before read all between 'ast_insert_query.data' and 'ast_insert_query.end'.
        * - because 'query.data' could refer to memory piece, used as buffer for 'input_buffer_tail_part'.
        */

    input_buffer_contacenated = std::make_unique<ConcatReadBuffer>(buffers);

    res_stream = context.getInputFormat(
        format,
        *input_buffer_contacenated,
        streams.out->getHeader(),
        context.getSettingsRef().max_insert_block_size);
}

} // namespace DB
