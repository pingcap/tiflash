#pragma once

#include <Storages/Transaction/ColumnFamily.h>
#include <Storages/Transaction/FileEncryption.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <string>

namespace kvrpcpb
{
class ReadIndexResponse;
class ReadIndexRequest;
} // namespace kvrpcpb

namespace DB
{

using RegionId = uint64_t;
using AppliedIndex = uint64_t;
using TiFlashRaftProxyPtr = void *;

class TMTContext;
struct TiFlashServer;

enum class TiFlashApplyRes : uint32_t
{
    None = 0,
    Persist,
    NotFound,
};

enum class WriteCmdType : uint8_t
{
    Put = 0,
    Del,
};

extern "C" {

struct BaseBuffView
{
    const char * data;
    const uint64_t len;

    BaseBuffView(const std::string & s) : data(s.data()), len(s.size()) {}
    BaseBuffView(const char * data_, const uint64_t len_) : data(data_), len(len_) {}
    BaseBuffView(std::string_view view) : data(view.data()), len(view.size()) {}
    operator std::string_view() { return std::string_view(data, len); }
};

struct SnapshotView
{
    const BaseBuffView * keys;
    const BaseBuffView * vals;
    const ColumnFamilyType cf;
    const uint64_t len = 0;
};

struct SnapshotViewArray
{
    const SnapshotView * views;
    const uint64_t len = 0;
};

struct RaftCmdHeader
{
    uint64_t region_id;
    uint64_t index;
    uint64_t term;
};

struct WriteCmdsView
{
    const BaseBuffView * keys;
    const BaseBuffView * vals;
    const WriteCmdType * cmd_types;
    const ColumnFamilyType * cmd_cf;
    const uint64_t len;
};

struct FsStats
{
    uint64_t used_size;
    uint64_t avail_size;
    uint64_t capacity_size;

    uint8_t ok;

    FsStats() { memset(this, 0, sizeof(*this)); }
};

struct StoreStats
{
    FsStats fs_stats;

    uint64_t engine_bytes_written;
    uint64_t engine_keys_written;
    uint64_t engine_bytes_read;
    uint64_t engine_keys_read;

    StoreStats() : fs_stats(), engine_bytes_written(0), engine_keys_written(0), engine_bytes_read(0), engine_keys_read(0) {}
};

enum class RaftProxyStatus : uint8_t
{
    Idle = 0,
    Running = 1,
    Stopped = 2,
};

enum class TiFlashStatus : uint8_t
{
    Idle = 0,
    Running,
    Stopped,
};

enum class RawCppPtrType : uint32_t
{
    None = 0,
    String,
    PreHandledSnapshot,
};

struct RawCppPtr
{
    void * ptr;
    RawCppPtrType type;

    RawCppPtr(void * ptr_, RawCppPtrType type_) : ptr(ptr_), type(type_) {}
    RawCppPtr(const RawCppPtr &) = delete;
    RawCppPtr(RawCppPtr &&) = delete;
};

struct CppStrWithView
{
    RawCppPtr inner;
    BaseBuffView view;

    CppStrWithView() : inner(nullptr, RawCppPtrType::None), view(nullptr, 0) {}
    CppStrWithView(std::string && v)
        : inner(new std::string(std::move(v)), RawCppPtrType::String), view(*reinterpret_cast<TiFlashRawString>(inner.ptr))
    {}
    CppStrWithView(const CppStrWithView &) = delete;
};

struct CppStrVecView
{
    const BaseBuffView * view;
    uint64_t len;
};

struct CppStrVec
{
    std::vector<std::string> data;
    std::vector<BaseBuffView> view;
    CppStrVec(std::vector<std::string> && data_) : data(std::move(data_)) { updateView(); }
    CppStrVec(const CppStrVec &) = delete;
    void updateView()
    {
        view.clear();
        view.reserve(data.size());
        for (const auto & e : data)
        {
            view.emplace_back(e);
        }
    }
    CppStrVecView intoOuterView() const { return {view.data(), view.size()}; }
};

using BatchReadIndexRes = std::unique_ptr<std::vector<std::pair<kvrpcpb::ReadIndexResponse, uint64_t>>>;
static_assert(std::is_same_v<BatchReadIndexRes::pointer, BatchReadIndexRes::element_type *>);

struct TiFlashRaftProxyHelper
{
public:
    RaftProxyStatus getProxyStatus() const;
    bool checkEncryptionEnabled() const;
    EncryptionMethod getEncryptionMethod() const;
    FileEncryptionInfo getFile(std::string_view) const;
    FileEncryptionInfo newFile(std::string_view) const;
    FileEncryptionInfo deleteFile(std::string_view) const;
    FileEncryptionInfo linkFile(std::string_view, std::string_view) const;
    FileEncryptionInfo renameFile(std::string_view, std::string_view) const;
    kvrpcpb::ReadIndexResponse readIndex(const kvrpcpb::ReadIndexRequest &) const;
    BatchReadIndexRes batchReadIndex(const std::vector<kvrpcpb::ReadIndexRequest> &) const;

private:
    TiFlashRaftProxyPtr proxy_ptr;
    uint8_t (*fn_handle_get_proxy_status)(TiFlashRaftProxyPtr);
    uint8_t (*fn_is_encryption_enabled)(TiFlashRaftProxyPtr);
    EncryptionMethod (*fn_encryption_method)(TiFlashRaftProxyPtr);
    FileEncryptionInfoRaw (*fn_handle_get_file)(TiFlashRaftProxyPtr, BaseBuffView);
    FileEncryptionInfoRaw (*fn_handle_new_file)(TiFlashRaftProxyPtr, BaseBuffView);
    FileEncryptionInfoRaw (*fn_handle_delete_file)(TiFlashRaftProxyPtr, BaseBuffView);
    FileEncryptionInfoRaw (*fn_handle_link_file)(TiFlashRaftProxyPtr, BaseBuffView, BaseBuffView);
    FileEncryptionInfoRaw (*fn_handle_rename_file)(TiFlashRaftProxyPtr, BaseBuffView, BaseBuffView);
    BatchReadIndexRes::pointer (*fn_handle_batch_read_index)(TiFlashRaftProxyPtr, CppStrVecView);
};

struct TiFlashServerHelper
{
    uint32_t magic_number; // use a very special number to check whether this struct is legal
    uint32_t version;      // version of function interface
    //

    TiFlashServer * inner;
    RawCppPtr (*fn_gen_cpp_string)(BaseBuffView);
    TiFlashApplyRes (*fn_handle_write_raft_cmd)(const TiFlashServer *, WriteCmdsView, RaftCmdHeader);
    TiFlashApplyRes (*fn_handle_admin_raft_cmd)(const TiFlashServer *, BaseBuffView, BaseBuffView, RaftCmdHeader);
    void (*fn_atomic_update_proxy)(TiFlashServer *, TiFlashRaftProxyHelper *);
    void (*fn_handle_destroy)(TiFlashServer *, RegionId);
    TiFlashApplyRes (*fn_handle_ingest_sst)(TiFlashServer *, SnapshotViewArray, RaftCmdHeader);
    uint8_t (*fn_handle_check_terminated)(TiFlashServer *);
    StoreStats (*fn_handle_compute_store_stats)(TiFlashServer *);
    TiFlashStatus (*fn_handle_get_tiflash_status)(TiFlashServer *);
    RawCppPtr (*fn_pre_handle_snapshot)(TiFlashServer *, BaseBuffView, uint64_t, SnapshotViewArray, uint64_t, uint64_t);
    void (*fn_apply_pre_handled_snapshot)(TiFlashServer *, void *, RawCppPtrType);
    CppStrWithView (*fn_handle_get_table_sync_status)(TiFlashServer *, uint64_t);
    void (*gc_raw_cpp_ptr)(TiFlashServer *, void *, RawCppPtrType);
    BatchReadIndexRes::pointer (*fn_gen_batch_read_index_res)(uint64_t);
    void (*fn_insert_batch_read_index_resp)(BatchReadIndexRes::pointer, BaseBuffView, uint64_t);
};

void run_tiflash_proxy_ffi(int argc, const char ** argv, const TiFlashServerHelper *);
}

struct TiFlashServer
{
    TMTContext * tmt{nullptr};
    TiFlashRaftProxyHelper * proxy_helper{nullptr};
    std::atomic<TiFlashStatus> status{TiFlashStatus::Idle};
};

RawCppPtr GenCppRawString(BaseBuffView);
TiFlashApplyRes HandleAdminRaftCmd(const TiFlashServer * server, BaseBuffView req_buff, BaseBuffView resp_buff, RaftCmdHeader header);
TiFlashApplyRes HandleWriteRaftCmd(const TiFlashServer * server, WriteCmdsView req_buff, RaftCmdHeader header);
void AtomicUpdateProxy(TiFlashServer * server, TiFlashRaftProxyHelper * proxy);
void HandleDestroy(TiFlashServer * server, RegionId region_id);
TiFlashApplyRes HandleIngestSST(TiFlashServer * server, SnapshotViewArray snaps, RaftCmdHeader header);
uint8_t HandleCheckTerminated(TiFlashServer * server);
StoreStats HandleComputeStoreStats(TiFlashServer * server);
TiFlashStatus HandleGetTiFlashStatus(TiFlashServer * server);
RawCppPtr PreHandleSnapshot(
    TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotViewArray snaps, uint64_t index, uint64_t term);
void ApplyPreHandledSnapshot(TiFlashServer * server, void * res, RawCppPtrType type);
CppStrWithView HandleGetTableSyncStatus(TiFlashServer *, uint64_t);
void GcRawCppPtr(TiFlashServer *, void * ptr, RawCppPtrType type);
BatchReadIndexRes::pointer GenBatchReadIndexRes(uint64_t cap);
void InsertBatchReadIndexResp(BatchReadIndexRes::pointer, BaseBuffView, uint64_t);
} // namespace DB
