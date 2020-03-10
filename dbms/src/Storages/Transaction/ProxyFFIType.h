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

struct SnapshotDataView
{
    const BaseBuffView * keys;
    const BaseBuffView * vals;
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

struct TiFlashServerHelper
{
    TiFlashServer * inner;
    void (*fn_gc_buff)(BaseBuff *);
    TiFlashApplyRes (*fn_handle_write_raft_cmd)(const TiFlashServer *, WriteCmdsView, RaftCmdHeader);
    TiFlashApplyRes (*fn_handle_admin_raft_cmd)(const TiFlashServer *, BaseBuffView, BaseBuffView, RaftCmdHeader);
    void (*fn_handle_apply_snapshot)(
        const TiFlashServer *, BaseBuffView, uint64_t, SnapshotDataView, SnapshotDataView, SnapshotDataView, uint64_t, uint64_t);
    void (*fn_atomic_update_proxy)(TiFlashServer *, TiFlashRaftProxy *);
    void (*fn_handle_destroy)(TiFlashServer *, RegionId);
    //
    uint32_t magic_number; // use a very special number to check whether this struct is legal
    uint32_t version;      // version of function interface
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
void HandleApplySnapshot(const TiFlashServer * server, BaseBuffView region_buff, uint64_t peer_id, SnapshotDataView lock_buff,
    SnapshotDataView write_buff, SnapshotDataView default_buff, uint64_t index, uint64_t term);
TiFlashApplyRes HandleWriteRaftCmd(const TiFlashServer * server, WriteCmdsView req_buff, RaftCmdHeader header);
void AtomicUpdateProxy(TiFlashServer * server, TiFlashRaftProxy * proxy);
void HandleDestroy(TiFlashServer * server, RegionId region_id);
} // namespace DB
