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

#include <Debug/MockKVStore/MockUtils.h>
#include <Parsers/IAST.h>
#include <Storages/DeltaMerge/DeltaMergeInterfaces.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/Region_fwd.h>
#include <TiDB/Schema/TiDB_fwd.h>
#include <kvproto/raft_cmdpb.pb.h>

#include <optional>

namespace TiDB
{
struct TableInfo;
}

namespace DB
{
class Context;
class KVStore;
class TMTContext;
} // namespace DB

namespace DB
{
using QualifiedName = std::pair<String, String>;
String mappedDatabase(Context & context, const String & database_name);
std::optional<String> mappedDatabaseWithOptional(Context & context, const String & database_name);
std::optional<QualifiedName> mappedTableWithOptional(
    Context & context,
    const String & database_name,
    const String & table_name);
QualifiedName mappedTable(
    Context & context,
    const String & database_name,
    const String & table_name,
    bool include_tombstone = false);

} // namespace DB
