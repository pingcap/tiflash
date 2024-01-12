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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Debug/MockKVStore/MockRaftStoreProxy.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/FFI/ProxyFFICommon.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionMeta.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/tests/region_helper.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <TiDB/Decode/RowCodec.h>
#include <TiDB/Schema/TiDBSchemaManager.h>

namespace DB
{
raft_serverpb::RegionLocalState MockProxyRegion::getState() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return state;
}

raft_serverpb::RegionLocalState & MockProxyRegion::mutState() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return state;
}

raft_serverpb::RaftApplyState MockProxyRegion::getApply() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return apply;
}

void MockProxyRegion::updateAppliedIndex(uint64_t index, bool persist_at_once) NO_THREAD_SAFETY_ANALYSIS
{
    auto l = genLockGuard();
    this->apply.set_applied_index(index);
    if (persist_at_once)
    {
        persistAppliedIndex(l);
    }
}

void MockProxyRegion::persistAppliedIndex() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    this->persisted_apply = apply;
}

void MockProxyRegion::persistAppliedIndex(const std::lock_guard<Mutex> &)
{
    this->persisted_apply = apply;
}

uint64_t MockProxyRegion::getPersistedAppliedIndex() NO_THREAD_SAFETY_ANALYSIS
{
    // Assume persist after every advance for simplicity.
    auto _ = genLockGuard();
    return this->persisted_apply.applied_index();
}

uint64_t MockProxyRegion::getLatestAppliedIndex() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return this->apply.applied_index();
}

uint64_t MockProxyRegion::getLatestCommitTerm() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return this->apply.commit_term();
}

uint64_t MockProxyRegion::getLatestCommitIndex() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return this->apply.commit_index();
}

void MockProxyRegion::tryUpdateTruncatedState(uint64_t index, uint64_t term)
{
    if (index > this->apply.truncated_state().index())
    {
        this->apply.mutable_truncated_state()->set_index(index);
        this->apply.mutable_truncated_state()->set_term(term);
    }
}

void MockProxyRegion::updateCommitIndex(uint64_t index) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    this->apply.set_commit_index(index);
}

void MockProxyRegion::setState(raft_serverpb::RegionLocalState s) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    this->state = s;
}

void MockProxyRegion::reload() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    this->apply.set_applied_index(this->persisted_apply.applied_index());
}

MockProxyRegion::MockProxyRegion(uint64_t id_)
    : id(id_)
{
    apply.set_commit_index(RAFT_INIT_LOG_INDEX);
    apply.set_commit_term(RAFT_INIT_LOG_TERM);
    apply.set_applied_index(RAFT_INIT_LOG_INDEX);
    persisted_apply = apply;
    apply.mutable_truncated_state()->set_index(RAFT_INIT_LOG_INDEX);
    apply.mutable_truncated_state()->set_term(RAFT_INIT_LOG_TERM);
    state.mutable_region()->set_id(id);
}

UniversalWriteBatch MockProxyRegion::persistMeta() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    auto wb = UniversalWriteBatch();

    auto region_key = UniversalPageIdFormat::toRegionLocalStateKeyInKVEngine(this->id);
    auto region_local_state = this->state.SerializeAsString();
    MemoryWriteBuffer buf(0, region_local_state.size());
    buf.write(region_local_state.data(), region_local_state.size());
    wb.putPage(
        UniversalPageId(region_key.data(), region_key.size()),
        0,
        buf.tryGetReadBuffer(),
        region_local_state.size());

    auto apply_key = UniversalPageIdFormat::toRaftApplyStateKeyInKVEngine(this->id);
    auto raft_apply_state = this->apply.SerializeAsString();
    MemoryWriteBuffer buf2(0, raft_apply_state.size());
    buf2.write(raft_apply_state.data(), raft_apply_state.size());
    wb.putPage(
        UniversalPageId(apply_key.data(), apply_key.size()),
        0,
        buf2.tryGetReadBuffer(),
        raft_apply_state.size());

    raft_serverpb::RegionLocalState restored_region_state;
    raft_serverpb::RaftApplyState restored_apply_state;
    restored_region_state.ParseFromArray(region_local_state.data(), region_local_state.size());
    restored_apply_state.ParseFromArray(raft_apply_state.data(), raft_apply_state.size());
    return wb;
}

void MockProxyRegion::addPeer(uint64_t store_id, uint64_t peer_id, metapb::PeerRole role) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    auto & peer = *state.mutable_region()->mutable_peers()->Add();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer.set_role(role);
}
} // namespace DB