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

// Expose publicly
#include <Common/SyncPoint/Ctl.h>
#include <Common/SyncPoint/ScopeGuard.h>
// =======

#include <fiu.h>

namespace DB
{

/**
 * Suspend the execution (when enabled), until `SyncPointCtl::waitAndPause()`,
 * `SyncPointCtl::next()` or `SyncPointCtl::waitAndNext()` is called somewhere
 * (e.g. in tests).
 *
 * Usually this is invoked in actual business logics.
 */
#define SYNC_FOR(name) fiu_do_on(name, SyncPointCtl::sync(name);)

} // namespace DB
