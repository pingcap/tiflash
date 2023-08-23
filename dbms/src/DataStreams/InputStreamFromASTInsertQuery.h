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
#include <Parsers/IAST.h>

#include <cstddef>
#include <memory>


namespace DB
{

struct BlockIO;
class Context;

/** Prepares an input stream which produce data containing in INSERT query
  * Head of inserting data could be stored in INSERT ast directly
  * Remaining (tail) data could be stored in input_buffer_tail_part
  */
class InputStreamFromASTInsertQuery : public IProfilingBlockInputStream
{
public:
    InputStreamFromASTInsertQuery(
        const ASTPtr & ast,
        ReadBuffer & input_buffer_tail_part,
        const BlockIO & streams,
        Context & context);

    Block readImpl() override { return res_stream->read(); }
    void readPrefixImpl() override { return res_stream->readPrefix(); }
    void readSuffixImpl() override { return res_stream->readSuffix(); }

    String getName() const override { return "InputStreamFromASTInsertQuery"; }

    Block getHeader() const override { return res_stream->getHeader(); }

private:
    std::unique_ptr<ReadBuffer> input_buffer_ast_part;
    std::unique_ptr<ReadBuffer> input_buffer_contacenated;

    BlockInputStreamPtr res_stream;
};

} // namespace DB
