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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Dictionaries/readInvalidateQuery.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_MANY_COLUMNS;
extern const int TOO_MANY_ROWS;
extern const int RECEIVED_EMPTY_DATA;
} // namespace ErrorCodes

std::string readInvalidateQuery(IProfilingBlockInputStream & block_input_stream)
{
    block_input_stream.readPrefix();
    std::string response;

    Block block = block_input_stream.read();
    if (!block)
        throw Exception("Empty response", ErrorCodes::RECEIVED_EMPTY_DATA);

    auto columns = block.columns();
    if (columns > 1)
        throw Exception(
            "Expected single column in resultset, got " + std::to_string(columns),
            ErrorCodes::TOO_MANY_COLUMNS);

    auto rows = block.rows();
    if (rows == 0)
        throw Exception("Expected single row in resultset, got 0", ErrorCodes::RECEIVED_EMPTY_DATA);
    if (rows > 1)
        throw Exception(
            "Expected single row in resultset, got at least " + std::to_string(rows),
            ErrorCodes::TOO_MANY_ROWS);

    auto column = block.getByPosition(0).column;
    response = column->getDataAt(0).toString();

    while ((block = block_input_stream.read()))
    {
        if (block.rows() > 0)
            throw Exception(
                "Expected single row in resultset, got at least " + std::to_string(rows + 1),
                ErrorCodes::TOO_MANY_ROWS);
    }

    block_input_stream.readSuffix();

    return response;
}

} // namespace DB
