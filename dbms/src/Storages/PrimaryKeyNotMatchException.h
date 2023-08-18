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

#include <Core/Types.h>

namespace Poco
{
class Logger;
}

namespace DB
{
class Context;

struct PrimaryKeyNotMatchException : public std::exception
{
    // The primary key name in definition
    const String pri_key;
    // The actual primary key name in TiDB::TableInfo
    const String actual_pri_key;
    PrimaryKeyNotMatchException(const String & pri_key_, const String & actual_pri_key_)
        : pri_key(pri_key_)
        , actual_pri_key(actual_pri_key_)
    {}
};

// This function will replace the primary key and update statement in `table_metadata_path`. The correct statement will be return.
String fixCreateStatementWithPriKeyNotMatchException(
    Context & context,
    const String old_definition,
    const String & table_metadata_path,
    const PrimaryKeyNotMatchException & ex,
    Poco::Logger * log);

} // namespace DB
