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

#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/SQLQuerySource.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>


namespace DB
{
SQLQuerySource::SQLQuerySource(const char * begin_, const char * end_)
    : begin(begin_)
    , end(end_)
{}

std::tuple<std::string, ASTPtr> SQLQuerySource::parse(size_t max_query_size)
{
    ParserQuery parser(end);
    size_t query_size;
    /// TODO Parser should fail early when max_query_size limit is reached.
    ast = parseQuery(parser, begin, end, "", max_query_size);

    /// Copy query into string. It will be written to log and presented in processlist. If an INSERT query, string will not include data to insertion.
    if (!(begin <= ast->range.first && ast->range.second <= end))
        throw Exception("Unexpected behavior: AST chars range is not inside source range", ErrorCodes::LOGICAL_ERROR);
    query_size = ast->range.second - begin;
    query = String(begin, begin + query_size);
    return std::make_tuple(query, ast);
}

String SQLQuerySource::str(size_t max_query_size)
{
    return String(begin, begin + std::min(end - begin, static_cast<ptrdiff_t>(max_query_size)));
}

std::unique_ptr<IInterpreter> SQLQuerySource::interpreter(Context & context, QueryProcessingStage::Enum stage)
{
    return InterpreterFactory::get(ast, context, stage);
}
} // namespace DB
