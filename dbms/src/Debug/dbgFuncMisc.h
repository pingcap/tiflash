// Copyright 2022 PingCAP, Ltd.
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

#include <Debug/DBGInvoker.h>

namespace DB
{

class Context;

// Find the last occurence of `key` in log file and extract the first number follow the key.
// Usage:
//   ./storage-client.sh "DBGInvoke search_log_for_key(key)"
void dbgFuncSearchLogForKey(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Trigger the gc process of global storage pool. Used to remove obsolete entry left by previous dropped table
// Usage:
//   ./storage-client.sh "DBGInvoke trigger_global_storage_pool_gc()"
void dbgFuncTriggerGlobalPageStorageGC(Context & context, const ASTs & args, DBGInvoker::Printer output);

} // namespace DB
