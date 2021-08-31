#pragma once

#include <RaftStoreProxyFFI/EncryptionFFI.h>
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/Transaction/ColumnFamily.h>

#include <atomic>
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

enum class RawCppPtrTypeImpl : RawCppPtrType
{
    None = 0,
    String,
    PreHandledSnapshotWithBlock,
    PreHandledSnapshotWithFiles,
};

RawCppPtr GenRawCppPtr(RawVoidPtr ptr_ = nullptr, RawCppPtrTypeImpl type_ = RawCppPtrTypeImpl::None);

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
    BatchReadIndexRes batchReadIndex(const std::vector<kvrpcpb::ReadIndexRequest> &, uint64_t) const;
};

extern "C" {
RawCppPtr GenCppRawString(BaseBuffView);
EngineStoreApplyRes HandleAdminRaftCmd(
    const EngineStoreServerWrap * server,
    BaseBuffView req_buff,
    BaseBuffView resp_buff,
    RaftCmdHeader header);
EngineStoreApplyRes HandleWriteRaftCmd(const EngineStoreServerWrap * server, WriteCmdsView req_buff, RaftCmdHeader header);
void AtomicUpdateProxy(EngineStoreServerWrap * server, RaftStoreProxyFFIHelper * proxy);
void HandleDestroy(EngineStoreServerWrap * server, uint64_t region_id);
EngineStoreApplyRes HandleIngestSST(EngineStoreServerWrap * server, SSTViewVec snaps, RaftCmdHeader header);
uint8_t HandleCheckTerminated(EngineStoreServerWrap * server);
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
HttpRequestRes HandleHttpRequest(EngineStoreServerWrap *, BaseBuffView);
uint8_t CheckHttpUriAvailable(BaseBuffView);
void GcRawCppPtr(EngineStoreServerWrap *, void * ptr, RawCppPtrType type);
RawVoidPtr GenBatchReadIndexRes(uint64_t cap);
void InsertBatchReadIndexResp(RawVoidPtr, BaseBuffView, uint64_t);
void SetServerInfoResp(BaseBuffView, RawVoidPtr);
BaseBuffView strIntoView(const std::string & view);
}
} // namespace DB
