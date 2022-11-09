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

#include <Common/nocopyable.h>
#include <RaftStoreProxyFFI/EncryptionFFI.h>
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <RaftStoreProxyFFI/VersionCheck.h>
#include <Storages/Transaction/ColumnFamily.h>

#include <atomic>
#include <memory>
#include <optional>
#include <vector>

namespace kvrpcpb
{
class ReadIndexResponse;
class ReadIndexRequest;
} // namespace kvrpcpb
namespace raft_serverpb
{
class RegionLocalState;
}

namespace DB
{
class TMTContext;
struct EngineStoreServerWrap;
struct TiFlashRaftProxyHelper;

struct EngineStoreServerWrap
{
    TMTContext * tmt{nullptr};
    TiFlashRaftProxyHelper * proxy_helper{nullptr};
    std::atomic<EngineStoreServerStatus> status{EngineStoreServerStatus::Idle};
};

using BatchReadIndexRes = std::vector<std::pair<kvrpcpb::ReadIndexResponse, uint64_t>>;

struct FileEncryptionInfo;

enum class RawCppPtrTypeImpl : RawCppPtrType
{
    None = 0,
    String,
    PreHandledSnapshotWithFiles,
    WakerNotifier,
};

RawCppPtr GenRawCppPtr(RawVoidPtr ptr_ = nullptr, RawCppPtrTypeImpl type_ = RawCppPtrTypeImpl::None);

struct ReadIndexTask;
struct RawRustPtrWrap;

struct RawRustPtrWrap : RawRustPtr
{
    DISALLOW_COPY(RawRustPtrWrap);

    explicit RawRustPtrWrap(RawRustPtr inner);
    ~RawRustPtrWrap();
    RawRustPtrWrap(RawRustPtrWrap &&);
};

struct ReadIndexTask : RawRustPtrWrap
{
    explicit ReadIndexTask(RawRustPtr inner_)
        : RawRustPtrWrap(inner_)
    {}
};

struct TimerTask : RawRustPtrWrap
{
    explicit TimerTask(RawRustPtr inner_)
        : RawRustPtrWrap(inner_)
    {}
};

class MockSetFFI
{
    friend struct MockRaftStoreProxy;
    static void MockSetRustGcHelper(void (*)(RawVoidPtr, RawRustPtrType));
};

struct TiFlashRaftProxyHelper : RaftStoreProxyFFIHelper
{
    RaftProxyStatus getProxyStatus() const;
    bool checkEncryptionEnabled() const;
    EncryptionMethod getEncryptionMethod() const;
    FileEncryptionInfo getFile(const std::string &) const;
    FileEncryptionInfo newFile(const std::string &) const;
    FileEncryptionInfo deleteFile(const std::string &) const;
    FileEncryptionInfo linkFile(const std::string &, const std::string &) const;
    BatchReadIndexRes batchReadIndex_v1(const std::vector<kvrpcpb::ReadIndexRequest> &, uint64_t) const;
    BatchReadIndexRes batchReadIndex(const std::vector<kvrpcpb::ReadIndexRequest> &, uint64_t) const;
    BatchReadIndexRes batchReadIndex_v2(const std::vector<kvrpcpb::ReadIndexRequest> &, uint64_t) const;
    // return null if meet error `Full` or `Disconnected`
    std::optional<ReadIndexTask> makeReadIndexTask(const kvrpcpb::ReadIndexRequest & req) const;
    bool pollReadIndexTask(ReadIndexTask & task, kvrpcpb::ReadIndexResponse & resp, RawVoidPtr waker = nullptr) const;
    RawRustPtr makeAsyncWaker(void (*wake_fn)(RawVoidPtr), RawCppPtr data) const;
    TimerTask makeTimerTask(uint64_t time_ms) const;
    bool pollTimerTask(TimerTask & task, RawVoidPtr waker = nullptr) const;
    raft_serverpb::RegionLocalState getRegionLocalState(uint64_t region_id) const;
};

extern "C" {
RawCppPtr GenCppRawString(BaseBuffView);
EngineStoreApplyRes HandleAdminRaftCmd(
    const EngineStoreServerWrap * server,
    BaseBuffView req_buff,
    BaseBuffView resp_buff,
    RaftCmdHeader header);
EngineStoreApplyRes HandleWriteRaftCmd(const EngineStoreServerWrap * server,
                                       WriteCmdsView cmds,
                                       RaftCmdHeader header);
uint8_t NeedFlushData(EngineStoreServerWrap * server, uint64_t region_id);
// `flush_pattern` values:
// 0: try, but can fail.
// 1: try until succeed.
uint8_t TryFlushData(EngineStoreServerWrap * server, uint64_t region_id, uint8_t flush_pattern, uint64_t index, uint64_t term);
void AtomicUpdateProxy(EngineStoreServerWrap * server, RaftStoreProxyFFIHelper * proxy);
void HandleDestroy(EngineStoreServerWrap * server, uint64_t region_id);
EngineStoreApplyRes HandleIngestSST(EngineStoreServerWrap * server, SSTViewVec snaps, RaftCmdHeader header);
StoreStats HandleComputeStoreStats(EngineStoreServerWrap * server);
EngineStoreServerStatus HandleGetTiFlashStatus(EngineStoreServerWrap * server);
RawCppPtr PreHandleSnapshot(
    EngineStoreServerWrap * server,
    BaseBuffView region_buff,
    uint64_t peer_id,
    SSTViewVec,
    uint64_t index,
    uint64_t term);
void ApplyPreHandledSnapshot(EngineStoreServerWrap * server, void * res, RawCppPtrType type);
HttpRequestRes HandleHttpRequest(EngineStoreServerWrap *, BaseBuffView path, BaseBuffView query, BaseBuffView body);
uint8_t CheckHttpUriAvailable(BaseBuffView);
void GcRawCppPtr(void * ptr, RawCppPtrType type);
BaseBuffView strIntoView(const std::string * str_ptr);
CppStrWithView GetConfig(EngineStoreServerWrap *, uint8_t full);
void SetStore(EngineStoreServerWrap *, BaseBuffView);
void SetPBMsByBytes(MsgPBType type, RawVoidPtr ptr, BaseBuffView view);
void HandleSafeTSUpdate(EngineStoreServerWrap * server, uint64_t region_id, uint64_t self_safe_ts, uint64_t leader_safe_ts);
}

inline EngineStoreServerHelper GetEngineStoreServerHelper(
    EngineStoreServerWrap * tiflash_instance_wrap)
{
    return EngineStoreServerHelper{
        // a special number, also defined in proxy
        .magic_number = RAFT_STORE_PROXY_MAGIC_NUMBER,
        .version = RAFT_STORE_PROXY_VERSION,
        .inner = tiflash_instance_wrap,
        .fn_gen_cpp_string = GenCppRawString,
        .fn_handle_write_raft_cmd = HandleWriteRaftCmd,
        .fn_handle_admin_raft_cmd = HandleAdminRaftCmd,
        .fn_need_flush_data = NeedFlushData,
        .fn_try_flush_data = TryFlushData,
        .fn_atomic_update_proxy = AtomicUpdateProxy,
        .fn_handle_destroy = HandleDestroy,
        .fn_handle_ingest_sst = HandleIngestSST,
        .fn_handle_compute_store_stats = HandleComputeStoreStats,
        .fn_handle_get_engine_store_server_status = HandleGetTiFlashStatus,
        .fn_pre_handle_snapshot = PreHandleSnapshot,
        .fn_apply_pre_handled_snapshot = ApplyPreHandledSnapshot,
        .fn_handle_http_request = HandleHttpRequest,
        .fn_check_http_uri_available = CheckHttpUriAvailable,
        .fn_gc_raw_cpp_ptr = GcRawCppPtr,
        .fn_get_config = GetConfig,
        .fn_set_store = SetStore,
        .fn_set_pb_msg_by_bytes = SetPBMsByBytes,
        .fn_handle_safe_ts_update = HandleSafeTSUpdate,
    };
}

std::string_view buffToStrView(const BaseBuffView & buf);

} // namespace DB
