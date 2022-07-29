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

#include <grpcpp/impl/codegen/status_code_enum.h>

namespace DB
{

grpc::StatusCode tiflashErrorCodeToGrpcStatusCode(int error_code);

#define MANAGE_COP_METRICS()                                                                                \
    GET_METRIC(tiflash_coprocessor_request_count, type_cop).Increment();                                    \
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Increment();                           \
    Stopwatch watch;                                                                                        \
    SCOPE_EXIT({                                                                                            \
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cop).Decrement();                       \
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cop).Observe(watch.elapsedSeconds()); \
        GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());                 \
    });

#define MANAGE_BATCH_COP_METRICS()                                                                                  \
    GET_METRIC(tiflash_coprocessor_request_count, type_super_batch).Increment();                                    \
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_super_batch).Increment();                           \
    Stopwatch watch;                                                                                                \
    SCOPE_EXIT({                                                                                                    \
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_super_batch).Decrement();                       \
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_super_batch).Observe(watch.elapsedSeconds()); \
        /* TODO: update the value of metric tiflash_coprocessor_response_bytes.*/                                   \
    });

#define MANAGE_ESTABLISH_MPP_CONNECTION_METRICS()                                                                                                                                                                                                       \
    GET_METRIC(tiflash_coprocessor_request_count, type_mpp_establish_conn).Increment();                                                                                                                                                                 \
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Increment();                                                                                                                                                        \
    GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Increment();                                                                                                                                                                 \
    GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Increment();                                                                                                                                                                            \
    if (!tryToResetMaxThreadsMetrics())                                                                                                                                                                                                                 \
    {                                                                                                                                                                                                                                                   \
        GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_establish_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Value())); \
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Value(), GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value()));                                \
    }                                                                                                                                                                                                                                                   \
    Stopwatch watch;                                                                                                                                                                                                                                    \
    SCOPE_EXIT({                                                                                                                                                                                                                                        \
        GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Decrement();                                                                                                                                                                        \
        GET_METRIC(tiflash_thread_count, type_active_threads_of_establish_mpp).Decrement();                                                                                                                                                             \
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_mpp_establish_conn).Decrement();                                                                                                                                                    \
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_mpp_establish_conn).Observe(watch.elapsedSeconds());                                                                                                                              \
        /* TODO: update the value of metric tiflash_coprocessor_response_bytes.*/                                                                                                                                                                       \
    });

#define MANAGE_CANCEL_TASK_METRICS()                                                                                    \
    GET_METRIC(tiflash_coprocessor_request_count, type_cancel_mpp_task).Increment();                                    \
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Increment();                           \
    Stopwatch watch;                                                                                                    \
    SCOPE_EXIT({                                                                                                        \
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_cancel_mpp_task).Decrement();                       \
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_cancel_mpp_task).Observe(watch.elapsedSeconds()); \
        GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());                             \
    });

#define MANAGE_DISPATCH_MPP_TASK_METRICS()                                                                                                                                                                                                           \
    GET_METRIC(tiflash_coprocessor_request_count, type_dispatch_mpp_task).Increment();                                                                                                                                                               \
    GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Increment();                                                                                                                                                      \
    GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Increment();                                                                                                                                                               \
    GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Increment();                                                                                                                                                                         \
    if (!tryToResetMaxThreadsMetrics())                                                                                                                                                                                                              \
    {                                                                                                                                                                                                                                                \
        GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_dispatch_mpp).Value(), GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Value())); \
        GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Set(std::max(GET_METRIC(tiflash_thread_count, type_max_threads_of_raw).Value(), GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Value()));                             \
    }                                                                                                                                                                                                                                                \
    Stopwatch watch;                                                                                                                                                                                                                                 \
    SCOPE_EXIT({                                                                                                                                                                                                                                     \
        GET_METRIC(tiflash_thread_count, type_total_threads_of_raw).Decrement();                                                                                                                                                                     \
        GET_METRIC(tiflash_thread_count, type_active_threads_of_dispatch_mpp).Decrement();                                                                                                                                                           \
        GET_METRIC(tiflash_coprocessor_handling_request_count, type_dispatch_mpp_task).Decrement();                                                                                                                                                  \
        GET_METRIC(tiflash_coprocessor_request_duration_seconds, type_dispatch_mpp_task).Observe(watch.elapsedSeconds());                                                                                                                            \
        GET_METRIC(tiflash_coprocessor_response_bytes).Increment(response->ByteSizeLong());                                                                                                                                                          \
    });

#define HANDLER_PREPROCESS(msg, request)                           \
    CPUAffinityManager::getInstance().bindSelfGrpcThread();        \
    LOG_FMT_DEBUG(log, "#(msg) {}", (request)->DebugString());       \
    if (!security_config.checkGrpcContext(grpc_context))           \
    {                                                              \
        return grpc::Status(grpc::PERMISSION_DENIED, tls_err_msg); \
    }
} // namespace DB