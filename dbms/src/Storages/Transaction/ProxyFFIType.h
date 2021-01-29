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
struct TiFlashServer;

extern "C" {

struct CppStrVec
{
    std::vector<std::string> data;
    std::vector<BaseBuffView> view;
    CppStrVec(std::vector<std::string> && data_) : data(std::move(data_)) { updateView(); }
    CppStrVec(const CppStrVec &) = delete;
    void updateView();
    CppStrVecView intoOuterView() const { return {view.data(), view.size()}; }
};

void run_tiflash_proxy_ffi(int argc, const char ** argv, const EngineStoreServerHelper *);
}

struct TiFlashRaftProxyHelper;

struct TiFlashServer
{
    TMTContext * tmt{nullptr};
    TiFlashRaftProxyHelper * proxy_helper{nullptr};
    std::atomic<TiFlashStatus> status{TiFlashStatus::Idle};
};

using BatchReadIndexRes = std::unique_ptr<std::vector<std::pair<kvrpcpb::ReadIndexResponse, uint64_t>>>;
static_assert(std::is_same_v<BatchReadIndexRes::pointer, BatchReadIndexRes::element_type *>);

struct FileEncryptionInfo;

struct TiFlashRaftProxyHelper : TiFlashRaftProxyHelperFFI
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
TiFlashApplyRes HandleAdminRaftCmd(const TiFlashServer * server, BaseBuffView req_buff, BaseBuffView resp_buff, RaftCmdHeader header);
TiFlashApplyRes HandleWriteRaftCmd(const TiFlashServer * server, WriteCmdsView req_buff, RaftCmdHeader header);
void AtomicUpdateProxy(TiFlashServer * server, TiFlashRaftProxyHelperFFI * proxy);
void HandleDestroy(TiFlashServer * server, uint64_t region_id);
TiFlashApplyRes HandleIngestSST(TiFlashServer * server, SSTViewVec snaps, RaftCmdHeader header);
uint8_t HandleCheckTerminated(TiFlashServer * server);
StoreStats HandleComputeStoreStats(TiFlashServer * server);
TiFlashStatus HandleGetTiFlashStatus(TiFlashServer * server);
RawCppPtr PreHandleSnapshot(TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SSTViewVec, uint64_t index, uint64_t term);
void ApplyPreHandledSnapshot(TiFlashServer * server, void * res, RawCppPtrType type);
CppStrWithView HandleGetTableSyncStatus(TiFlashServer *, uint64_t);
void GcRawCppPtr(TiFlashServer *, void * ptr, RawCppPtrType type);
RawVoidPtr GenBatchReadIndexRes(uint64_t cap);
void InsertBatchReadIndexResp(RawVoidPtr, BaseBuffView, uint64_t);
} // namespace DB
