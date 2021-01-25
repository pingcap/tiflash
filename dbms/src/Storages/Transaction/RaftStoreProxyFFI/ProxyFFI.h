#pragma once

#include "Common.h"

extern "C" {

namespace DB
{

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

struct BaseBuffView
{
    const char * data;
    const uint64_t len;

    BaseBuffView(const char * data_, const uint64_t len_) : data(data_), len(len_) {}
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

    FsStats() : used_size(0), avail_size(0), capacity_size(0), ok(0) {}
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
    Running,
    Stopped,
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
    RawVoidPtr ptr;
    RawCppPtrType type;

    RawCppPtr(const RawCppPtr &) = delete;
    RawCppPtr(RawCppPtr &&) = delete;
};

struct CppStrWithView
{
    RawCppPtr inner;
    BaseBuffView view;

    CppStrWithView(const CppStrWithView &) = delete;
};

struct CppStrVecView
{
    const BaseBuffView * view;
    uint64_t len;
};

struct FileEncryptionInfoRaw;
enum class EncryptionMethod : uint8_t;

struct TiFlashRaftProxyHelperFFI
{
    RaftStoreProxyPtr proxy_ptr;
    RaftProxyStatus (*fn_handle_get_proxy_status)(RaftStoreProxyPtr);
    uint8_t (*fn_is_encryption_enabled)(RaftStoreProxyPtr);
    EncryptionMethod (*fn_encryption_method)(RaftStoreProxyPtr);
    FileEncryptionInfoRaw (*fn_handle_get_file)(RaftStoreProxyPtr, BaseBuffView);
    FileEncryptionInfoRaw (*fn_handle_new_file)(RaftStoreProxyPtr, BaseBuffView);
    FileEncryptionInfoRaw (*fn_handle_delete_file)(RaftStoreProxyPtr, BaseBuffView);
    FileEncryptionInfoRaw (*fn_handle_link_file)(RaftStoreProxyPtr, BaseBuffView, BaseBuffView);
    RawVoidPtr (*fn_handle_batch_read_index)(RaftStoreProxyPtr, CppStrVecView);
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
    void (*fn_atomic_update_proxy)(TiFlashServer *, TiFlashRaftProxyHelperFFI *);
    void (*fn_handle_destroy)(TiFlashServer *, RegionId);
    TiFlashApplyRes (*fn_handle_ingest_sst)(TiFlashServer *, SnapshotViewArray, RaftCmdHeader);
    uint8_t (*fn_handle_check_terminated)(TiFlashServer *);
    StoreStats (*fn_handle_compute_store_stats)(TiFlashServer *);
    TiFlashStatus (*fn_handle_get_tiflash_status)(TiFlashServer *);
    RawCppPtr (*fn_pre_handle_snapshot)(TiFlashServer *, BaseBuffView, uint64_t, SnapshotViewArray, uint64_t, uint64_t);
    void (*fn_apply_pre_handled_snapshot)(TiFlashServer *, RawVoidPtr, RawCppPtrType);
    CppStrWithView (*fn_handle_get_table_sync_status)(TiFlashServer *, uint64_t);
    void (*fn_gc_raw_cpp_ptr)(TiFlashServer *, RawVoidPtr, RawCppPtrType);
    RawVoidPtr (*fn_gen_batch_read_index_res)(uint64_t);
    void (*fn_insert_batch_read_index_resp)(RawVoidPtr, BaseBuffView, uint64_t);
};
} // namespace DB
}
