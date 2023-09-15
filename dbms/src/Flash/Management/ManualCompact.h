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

#include <Interpreters/Context_fwd.h>
#include <Interpreters/Settings_fwd.h>
#include <Storages/KVStore/Types.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/server_context.h>
#include <kvproto/tikvpb.pb.h>
#pragma GCC diagnostic pop

#include <atomic>
#include <boost/noncopyable.hpp>
#include <cstddef>
#include <memory>
#include <set>

namespace DB
{

namespace Management
{

/// Serves manual compact requests. Notice that the "compact" term here has different meanings compared to
/// the word "compact" in the DeltaMerge Store. The "compact request" here refer to the "delta merge" process
/// (major compact), while the "compact" in delta merge store refers to the "compact delta layer" process
/// (minor compact).
/// This class is thread safe. Every public member function can be called without synchronization.
class ManualCompactManager : private boost::noncopyable
{
public:
    explicit ManualCompactManager(const Context & global_context_, const Settings & settings_);

    ~ManualCompactManager() = default;

    /// Handles a request in the worker pool and wait for the result.
    grpc::Status handleRequest(const ::kvrpcpb::CompactRequest * request, ::kvrpcpb::CompactResponse * response);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    /// Returns manual_compact_more_until_ms.
    uint64_t getSettingCompactMoreUntilMs() const;

    /// Returns manual_compact_max_concurrency.
    uint64_t getSettingMaxConcurrency() const;

    /// Process a single manual compact request, with exception handlers wrapped.
    /// Should be called in the worker pool.
    grpc::Status doWorkWithCatch(const ::kvrpcpb::CompactRequest * request, ::kvrpcpb::CompactResponse * response);

    /// Process a single manual compact request.
    /// Should be called in the worker pool.
    grpc::Status doWork(const ::kvrpcpb::CompactRequest * request, ::kvrpcpb::CompactResponse * response);

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    std::mutex mutex;
    // == Accessing members below must be synchronized ==

    /// When there is a task containing the same logical_table running,
    /// the task will be rejected.
    std::unordered_set<DB::KeyspaceTableID, boost::hash<DB::KeyspaceTableID>> unsync_active_logical_table_ids = {};

    size_t unsync_running_or_pending_tasks = 0;

    // == Accessing members above must be synchronized ==

#ifndef DBMS_PUBLIC_GTEST
private:
#endif
    const Context & global_context;
    const Settings & settings;
    LoggerPtr log;

    /// Placed last to be destroyed first.
    std::unique_ptr<legacy::ThreadPool> worker_pool;
};

} // namespace Management

} // namespace DB
