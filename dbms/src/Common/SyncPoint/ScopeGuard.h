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

#include <string>

namespace DB
{

class SyncPointScopeGuard
{
public:
    explicit SyncPointScopeGuard(const char * name_);

    ~SyncPointScopeGuard() { disable(); }

    /**
     * Disable this sync point beforehand, instead of at the moment when
     * this scope guard is destructed.
     */
    void disable();

    /**
     * Wait for the sync point being executed. The code at the sync point will keep
     * pausing until you call `next()`.
     */
    void waitAndPause();

    /**
     * Continue the execution after the specified sync point.
     * You must first `waitAndPause()` for it, then `next()` it.
     */
    void next();

    /**
     * Wait for the sync point being executed. After that, continue the execution after the sync point.
     */
    void waitAndNext();

private:
    std::string name;
    bool disabled = false;
};

} // namespace DB
