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

#include <Common/SyncPoint/Ctl.h>
#include <Common/SyncPoint/ScopeGuard.h>

namespace DB
{

SyncPointScopeGuard::SyncPointScopeGuard(const char * name_)
    : name(name_)
{
    SyncPointCtl::enable(name_);
}

void SyncPointScopeGuard::disable()
{
    if (disabled)
        return;
    SyncPointCtl::disable(name.c_str());
    disabled = true;
}

void SyncPointScopeGuard::waitAndPause()
{
    SyncPointCtl::waitAndPause(name.c_str());
}

void SyncPointScopeGuard::next()
{
    SyncPointCtl::next(name.c_str());
}

void SyncPointScopeGuard::waitAndNext()
{
    SyncPointCtl::waitAndNext(name.c_str());
}

} // namespace DB
