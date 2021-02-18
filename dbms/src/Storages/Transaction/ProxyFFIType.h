#pragma once

#include <Storages/Transaction/ColumnFamily.h>
#include <Storages/Transaction/RaftStoreProxyFFI/EncryptionFFI.h>
#include <Storages/Transaction/RaftStoreProxyFFI/ProxyFFI.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <vector>

namespace kvrpcpb
{
class ReadIndexResponse;
class ReadIndexRequest;
} // namespace kvrpcpb

namespace DB
{

class TMTContext;
struct EngineStoreServerWrap;

struct CppStrVec
{
    std::vector<std::string> data;
    std::vector<BaseBuffView> view;
    CppStrVec(std::vector<std::string> && data_) : data(std::move(data_)) { updateView(); }
    CppStrVec(const CppStrVec &) = delete;
    void updateView();
    CppStrVecView intoOuterView() const { return {view.data(), view.size()}; }
};

struct TiFlashRaftProxyHelper;

struct EngineStoreServerWrap
{
    TMTContext * tmt{nullptr};
    TiFlashRaftProxyHelper * proxy_helper{nullptr};
    std::atomic<EngineStoreServerStatus> status{EngineStoreServerStatus::Idle};
};

using BatchReadIndexRes = std::unique_ptr<std::vector<std::pair<kvrpcpb::ReadIndexResponse, uint64_t>>>;
static_assert(std::is_same_v<BatchReadIndexRes::pointer, BatchReadIndexRes::element_type *>);

struct FileEncryptionInfo;

struct TiFlashRaftProxyHelper : RaftStoreProxyFFIHelper
{
    RaftProxyStatus getProxyStatus() const;
    bool checkEncryptionEnabled() const;
    EncryptionMethod getEncryptionMethod() const;
    FileEncryptionInfo getFile(const std::string &) const;
    FileEncryptionInfo newFile(const std::string &) const;
    FileEncryptionInfo deleteFile(const std::string &) const;
    FileEncryptionInfo linkFile(const std::string &, const std::string &) const;
    kvrpcpb::ReadIndexResponse readIndex(const kvrpcpb::ReadIndexRequest &) const;
    BatchReadIndexRes batchReadIndex(const std::vector<kvrpcpb::ReadIndexRequest> &) const;
};

RawCppPtr GenCppRawString(BaseBuffView);
EngineStoreApplyRes HandleAdminRaftCmd(
    const EngineStoreServerWrap * server, BaseBuffView req_buff, BaseBuffView resp_buff, RaftCmdHeader header);
EngineStoreApplyRes HandleWriteRaftCmd(const EngineStoreServerWrap * server, WriteCmdsView req_buff, RaftCmdHeader header);
void AtomicUpdateProxy(EngineStoreServerWrap * server, RaftStoreProxyFFIHelper * proxy);
void HandleDestroy(EngineStoreServerWrap * server, uint64_t region_id);
EngineStoreApplyRes HandleIngestSST(EngineStoreServerWrap * server, SSTViewVec snaps, RaftCmdHeader header);
uint8_t HandleCheckTerminated(EngineStoreServerWrap * server);
StoreStats HandleComputeStoreStats(EngineStoreServerWrap * server);
EngineStoreServerStatus HandleGetTiFlashStatus(EngineStoreServerWrap * server);
RawCppPtr PreHandleSnapshot(
    EngineStoreServerWrap * server, BaseBuffView region_buff, uint64_t peer_id, SSTViewVec, uint64_t index, uint64_t term);
void ApplyPreHandledSnapshot(EngineStoreServerWrap * server, void * res, RawCppPtrType type);
HttpRequestRes HandleHttpRequest(EngineStoreServerWrap *, BaseBuffView);
uint8_t CheckHttpUriAvailable(BaseBuffView);
void GcRawCppPtr(EngineStoreServerWrap *, void * ptr, RawCppPtrType type);
RawVoidPtr GenBatchReadIndexRes(uint64_t cap);
void InsertBatchReadIndexResp(RawVoidPtr, BaseBuffView, uint64_t);
} // namespace DB
