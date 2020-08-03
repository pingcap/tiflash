#pragma once

#include <Storages/Transaction/ColumnFamily.h>

#include <atomic>
#include <cstdint>
#include <string>

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

using TiFlashRawString = std::string *;

struct BaseBuffView
{
    const char * data;
    const uint64_t len;

    BaseBuffView(const std::string & s) : data(s.data()), len(s.size()) {}
    BaseBuffView(const char * data_, const uint64_t len_) : data(data_), len(len_) {}
    BaseBuffView(std::string_view view) : data(view.data()), len(view.size()) {}
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

enum class FileEncryptionRes : uint8_t
{
    Disabled = 0,
    Ok,
    Error,
};

enum class EncryptionMethod : uint8_t
{
    Unknown = 0,
    Plaintext = 1,
    Aes128Ctr = 2,
    Aes192Ctr = 3,
    Aes256Ctr = 4,
};

struct FileEncryptionInfo
{
    FileEncryptionRes res;
    EncryptionMethod method;
    TiFlashRawString key;
    TiFlashRawString iv;
    TiFlashRawString erro_msg;

    ~FileEncryptionInfo()
    {
        if (key)
        {
            delete key;
            key = nullptr;
        }
        if (iv)
        {
            delete iv;
            iv = nullptr;
        }
        if (erro_msg)
        {
            delete erro_msg;
            erro_msg = nullptr;
        }
    }

    FileEncryptionInfo(const FileEncryptionRes & res_, const EncryptionMethod & method_,
            TiFlashRawString key_, TiFlashRawString iv_, TiFlashRawString erro_msg_)
            :res{res_}, method{method_}, key{key_}, iv{iv_}, erro_msg{erro_msg_}
    {}
    FileEncryptionInfo(const FileEncryptionInfo &) = delete;
    FileEncryptionInfo(FileEncryptionInfo && src)
    {
        memcpy(this, &src, sizeof(src));
        memset(&src, 0, sizeof(src));
    }
    FileEncryptionInfo & operator=(FileEncryptionInfo && src)
    {
        if (this == &src)
            return *this;
        this->~FileEncryptionInfo();
        memcpy(this, &src, sizeof(src));
        memset(&src, 0, sizeof(src));
        return *this;
    }
};

struct TiFlashRaftProxyHelper
{
public:
    bool checkServiceStopped() const;
    bool checkEncryptionEnabled() const;
    FileEncryptionInfo getFile(std::string_view) const;
    FileEncryptionInfo newFile(std::string_view) const;
    FileEncryptionInfo deleteFile(std::string_view) const;
    FileEncryptionInfo linkFile(std::string_view, std::string_view) const;
    FileEncryptionInfo renameFile(std::string_view, std::string_view) const;

private:
    TiFlashRaftProxyPtr proxy_ptr;
    uint8_t (*fn_handle_check_service_stopped)(TiFlashRaftProxyPtr);
    uint8_t (*fn_handle_enable_encryption)(TiFlashRaftProxyPtr);
    FileEncryptionInfo (*fn_handle_get_file)(TiFlashRaftProxyPtr, BaseBuffView);
    FileEncryptionInfo (*fn_handle_new_file)(TiFlashRaftProxyPtr, BaseBuffView);
    FileEncryptionInfo (*fn_handle_delete_file)(TiFlashRaftProxyPtr, BaseBuffView);
    FileEncryptionInfo (*fn_handle_link_file)(TiFlashRaftProxyPtr, BaseBuffView, BaseBuffView);
    FileEncryptionInfo (*fn_handle_rename_file)(TiFlashRaftProxyPtr, BaseBuffView, BaseBuffView);
};

enum class TiFlashStatus : uint8_t
{
    IDLE = 0,
    Running,
    Stopped,
};

struct TiFlashServerHelper
{
    uint32_t magic_number; // use a very special number to check whether this struct is legal
    uint32_t version;      // version of function interface
    //

    TiFlashServer * inner;
    TiFlashRawString (*fn_gen_cpp_string)(BaseBuffView);
    TiFlashApplyRes (*fn_handle_write_raft_cmd)(const TiFlashServer *, WriteCmdsView, RaftCmdHeader);
    TiFlashApplyRes (*fn_handle_admin_raft_cmd)(const TiFlashServer *, BaseBuffView, BaseBuffView, RaftCmdHeader);
    void (*fn_handle_apply_snapshot)(const TiFlashServer *, BaseBuffView, uint64_t, SnapshotViewArray, uint64_t, uint64_t);
    void (*fn_atomic_update_proxy)(TiFlashServer *, TiFlashRaftProxyHelper *);
    void (*fn_handle_destroy)(TiFlashServer *, RegionId);
    TiFlashApplyRes (*fn_handle_ingest_sst)(TiFlashServer *, SnapshotViewArray, RaftCmdHeader);
    uint8_t (*fn_handle_check_terminated)(TiFlashServer *);
    FsStats (*fn_handle_compute_fs_stats)(TiFlashServer *);
    TiFlashStatus (*fn_handle_get_tiflash_status)(TiFlashServer *);
    void * (*fn_pre_handle_snapshot)(TiFlashServer *, BaseBuffView, uint64_t, SnapshotViewArray, uint64_t, uint64_t);
    void (*fn_apply_pre_handled_snapshot)(TiFlashServer *, void *);
    void (*fn_gc_pre_handled_snapshot)(TiFlashServer *, void *);
};

void run_tiflash_proxy_ffi(int argc, const char ** argv, const TiFlashServerHelper *);
}

struct TiFlashServer
{
    TMTContext * tmt{nullptr};
    TiFlashRaftProxyHelper * proxy_helper{nullptr};
    std::atomic<TiFlashStatus> status{TiFlashStatus::IDLE};
};

TiFlashRawString GenCppRawString(BaseBuffView);
TiFlashApplyRes HandleAdminRaftCmd(const TiFlashServer * server, BaseBuffView req_buff, BaseBuffView resp_buff, RaftCmdHeader header);
void HandleApplySnapshot(
    const TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotViewArray snaps, uint64_t index, uint64_t term);
TiFlashApplyRes HandleWriteRaftCmd(const TiFlashServer * server, WriteCmdsView req_buff, RaftCmdHeader header);
void AtomicUpdateProxy(TiFlashServer * server, TiFlashRaftProxyHelper * proxy);
void HandleDestroy(TiFlashServer * server, RegionId region_id);
TiFlashApplyRes HandleIngestSST(TiFlashServer * server, SnapshotViewArray snaps, RaftCmdHeader header);
uint8_t HandleCheckTerminated(TiFlashServer * server);
FsStats HandleComputeFsStats(TiFlashServer * server);
TiFlashStatus HandleGetTiFlashStatus(TiFlashServer * server);
void * PreHandleSnapshot(
    TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotViewArray snaps, uint64_t index, uint64_t term);
void ApplyPreHandledSnapshot(TiFlashServer * server, void * res);
void GcPreHandledSnapshot(TiFlashServer * server, void * res);
} // namespace DB
