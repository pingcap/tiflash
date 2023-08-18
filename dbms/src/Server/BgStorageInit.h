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

#include <Common/Logger.h>
#include <Common/nocopyable.h>
#include <Interpreters/Context_fwd.h>

#include <memory>

namespace DB
{
struct BgStorageInitHolder
{
    bool need_join = false;
    std::unique_ptr<std::thread> init_thread;

    void start(Context & global_context, const LoggerPtr & log, bool lazily_init_store, bool is_s3_enabled);

    // wait until finish if need
    void waitUntilFinish();

    BgStorageInitHolder() = default;

    // Exception safe for joining the init_thread
    ~BgStorageInitHolder() { waitUntilFinish(); }

    DISALLOW_COPY_AND_MOVE(BgStorageInitHolder);
};

} // namespace DB
