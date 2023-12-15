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

#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/ProxyFFICommon.h>

#include <string>

namespace DB::MockFFIImpls
{
extern void mock_set_rust_gc_helper(void (*)(RawVoidPtr, RawRustPtrType));

RawRustPtr fn_make_read_index_task(RaftStoreProxyPtr ptr, BaseBuffView view);
RawRustPtr fn_make_async_waker(void (*wake_fn)(RawVoidPtr), RawCppPtr data);
uint8_t fn_poll_read_index_task(RaftStoreProxyPtr, RawVoidPtr task, RawVoidPtr resp, RawVoidPtr waker);
void fn_handle_batch_read_index(
    RaftStoreProxyPtr,
    CppStrVecView,
    RawVoidPtr,
    uint64_t,
    void (*)(RawVoidPtr, BaseBuffView, uint64_t));
kvrpcpb::ReadIndexRequest make_read_index_reqs(uint64_t region_id, uint64_t start_ts);
void fn_gc_rust_ptr(RawVoidPtr ptr, RawRustPtrType type_);
KVGetStatus fn_get_region_local_state(
    RaftStoreProxyPtr ptr,
    uint64_t region_id,
    RawVoidPtr data,
    RawCppStringPtr * error_msg);
void fn_notify_compact_log(
    RaftStoreProxyPtr ptr,
    uint64_t region_id,
    uint64_t compact_index,
    uint64_t compact_term,
    uint64_t applied_index);
RaftstoreVer fn_get_cluster_raftstore_version(RaftStoreProxyPtr ptr, uint8_t, int64_t);
RustStrWithView fn_get_config_json(RaftStoreProxyPtr ptr, ConfigJsonType);

} // namespace DB::MockFFIImpls