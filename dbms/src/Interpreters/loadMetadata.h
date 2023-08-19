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


namespace DB
{
class Context;

/// Load tables from system database. Only real tables like query_log, part_log.
/// You should first load system database, then attach system tables that you need into it, then load other databases.
void loadMetadataSystem(Context & context);

/// Load tables from databases and add them to context. Database 'system' is ignored. Use separate function to load system tables.
void loadMetadata(Context & context);

} // namespace DB
