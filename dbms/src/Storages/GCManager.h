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

#include <Common/Stopwatch.h>
#include <Storages/KVStore/Types.h>

namespace DB
{
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
class Context;

class GCManager
{
public:
    explicit GCManager(Context & context);

    ~GCManager() = default;

    bool work();

private:
    Context & global_context;

    KeyspaceTableID next_keyspace_table_id = KeyspaceTableID{NullspaceID, InvalidTableID};

    AtomicStopwatch gc_check_stop_watch;

    LoggerPtr log;
};
} // namespace DB
