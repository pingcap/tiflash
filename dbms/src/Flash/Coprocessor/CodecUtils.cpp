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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/CodecUtils.h>
#include <Flash/Coprocessor/DAGUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace CodecUtils
{
void checkColumnSize(const String & identifier, size_t expected, size_t actual)
{
    if unlikely (expected != actual)
        throw Exception(
            fmt::format("{} schema mismatch, expected {}, actual {}.", identifier, expected, actual),
            ErrorCodes::LOGICAL_ERROR);
}

void checkDataTypeName(const String & identifier, size_t column_index, const String & expected, const String & actual)
{
    if unlikely (expected != actual)
        throw Exception(
            fmt::format(
                "{} schema mismatch at column {}, expected {}, actual {}",
                identifier,
                column_index,
                expected,
                actual),
            ErrorCodes::LOGICAL_ERROR);
}

} // namespace CodecUtils
} // namespace DB
