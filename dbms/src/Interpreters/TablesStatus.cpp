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

#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TablesStatus.h>

namespace DB
{
void TableStatus::write(WriteBuffer & out) const
{
    writeBinary(is_replicated, out);
    if (is_replicated)
    {
        writeVarUInt(absolute_delay, out);
    }
}

void TableStatus::read(ReadBuffer & in)
{
    absolute_delay = 0;
    readBinary(is_replicated, in);
    if (is_replicated)
    {
        readVarUInt(absolute_delay, in);
    }
}

void TablesStatusRequest::write(WriteBuffer & out) const
{
    writeVarUInt(tables.size(), out);
    for (const auto & table_name : tables)
    {
        writeBinary(table_name.database, out);
        writeBinary(table_name.table, out);
    }
}

void TablesStatusRequest::read(ReadBuffer & in)
{
    size_t size = 0;
    readVarUInt(size, in);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception("Too large collection size.", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

    for (size_t i = 0; i < size; ++i)
    {
        QualifiedTableName table_name;
        readBinary(table_name.database, in);
        readBinary(table_name.table, in);
        tables.emplace(std::move(table_name));
    }
}

void TablesStatusResponse::write(WriteBuffer & out) const
{
    writeVarUInt(table_states_by_id.size(), out);
    for (const auto & kv : table_states_by_id)
    {
        const QualifiedTableName & table_name = kv.first;
        writeBinary(table_name.database, out);
        writeBinary(table_name.table, out);

        const TableStatus & status = kv.second;
        status.write(out);
    }
}

void TablesStatusResponse::read(ReadBuffer & in)
{
    size_t size = 0;
    readVarUInt(size, in);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception("Too large collection size.", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

    for (size_t i = 0; i < size; ++i)
    {
        QualifiedTableName table_name;
        readBinary(table_name.database, in);
        readBinary(table_name.table, in);

        TableStatus status;
        status.read(in);
        table_states_by_id.emplace(std::move(table_name), std::move(status));
    }
}

} // namespace DB
