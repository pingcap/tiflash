#pragma once

#include <Storages/Transaction/ColumnFamily.h>

#include <cstdint>
#include <string>

namespace DB
{

using RegionId = uint64_t;
using AppliedIndex = uint64_t;

class TMTContext;
struct TiFlashServer;

enum TiFlashApplyRes : uint32_t
{
    None = 0,
    Persist,
    NotFound,
};

enum WriteCmdType : uint8_t
{
    Put = 0,
    Del,
};

extern "C" {

struct TiFlashRaftProxy
{
    uint64_t check_sum;
};

struct BaseBuff
{
    std::string * inner;
    const char * data;
    const uint64_t len;
};

struct BaseBuffView
{
    const char * data;
    const uint64_t len;
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
    uint64_t capacity_size = 0;

    uint8_t ok = 0;
};

struct TiFlashServerHelper
{
    uint32_t magic_number; // use a very special number to check whether this struct is legal
    uint32_t version;      // version of function interface
    //

    TiFlashServer * inner;
    void (*fn_gc_buff)(BaseBuff *);
    TiFlashApplyRes (*fn_handle_write_raft_cmd)(const TiFlashServer *, WriteCmdsView, RaftCmdHeader);
    TiFlashApplyRes (*fn_handle_admin_raft_cmd)(const TiFlashServer *, BaseBuffView, BaseBuffView, RaftCmdHeader);
    void (*fn_handle_apply_snapshot)(const TiFlashServer *, BaseBuffView, uint64_t, SnapshotViewArray, uint64_t, uint64_t);
    void (*fn_atomic_update_proxy)(TiFlashServer *, TiFlashRaftProxy *);
    void (*fn_handle_destroy)(TiFlashServer *, RegionId);
    void (*fn_handle_ingest_sst)(TiFlashServer *, SnapshotViewArray, RaftCmdHeader);
    uint8_t (*fn_handle_check_terminated)(TiFlashServer *);
    FsStats (*fn_handle_compute_fs_stats)(TiFlashServer * server);
};

void run_tiflash_proxy_ffi(int argc, const char ** argv, const TiFlashServerHelper *);
}

struct TiFlashServer
{
    TMTContext & tmt;
    std::atomic<TiFlashRaftProxy *> proxy{nullptr};
};

void GcBuff(BaseBuff * buff);
TiFlashApplyRes HandleAdminRaftCmd(const TiFlashServer * server, BaseBuffView req_buff, BaseBuffView resp_buff, RaftCmdHeader header);
void HandleApplySnapshot(
    const TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotViewArray snaps, uint64_t index, uint64_t term);
TiFlashApplyRes HandleWriteRaftCmd(const TiFlashServer * server, WriteCmdsView req_buff, RaftCmdHeader header);
void AtomicUpdateProxy(TiFlashServer * server, TiFlashRaftProxy * proxy);
void HandleDestroy(TiFlashServer * server, RegionId region_id);
void HandleIngestSST(TiFlashServer * server, SnapshotViewArray snaps, RaftCmdHeader header);
uint8_t HandleCheckTerminated(TiFlashServer * server);
FsStats HandleComputeFsStats(TiFlashServer * server);
} // namespace DB
