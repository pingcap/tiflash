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

#include <Common/Exception.h>
#include <Debug/MockRaftStoreProxy.h>
#include <Storages/Transaction/ProxyFFICommon.h>

namespace DB
{
kvrpcpb::ReadIndexRequest make_read_index_reqs(uint64_t region_id, uint64_t start_ts)
{
    kvrpcpb::ReadIndexRequest req;
    req.set_start_ts(start_ts);
    req.mutable_context()->set_region_id(region_id);
    return req;
}

MockRaftStoreProxy & as_ref(RaftStoreProxyPtr ptr)
{
    return *reinterpret_cast<MockRaftStoreProxy *>(reinterpret_cast<size_t>(ptr.inner));
}

extern void mock_set_rust_gc_helper(void (*)(RawVoidPtr, RawRustPtrType));

RawRustPtr fn_make_read_index_task(RaftStoreProxyPtr ptr, BaseBuffView view)
{
    auto & x = as_ref(ptr);
    kvrpcpb::ReadIndexRequest req;
    req.ParseFromArray(view.data, view.len);
    auto * task = x.makeReadIndexTask(req);
    if (task)
        GCMonitor::instance().add(RawObjType::MockReadIndexTask, 1);
    return RawRustPtr{task, static_cast<uint32_t>(RawObjType::MockReadIndexTask)};
}

RawRustPtr fn_make_async_waker(void (*wake_fn)(RawVoidPtr),
                               RawCppPtr data)
{
    auto * p = new MockAsyncWaker{std::make_shared<MockAsyncNotifier>()};
    p->data->data = data;
    p->data->wake_fn = wake_fn;
    GCMonitor::instance().add(RawObjType::MockAsyncWaker, 1);
    return RawRustPtr{p, static_cast<uint32_t>(RawObjType::MockAsyncWaker)};
}

uint8_t fn_poll_read_index_task(RaftStoreProxyPtr, RawVoidPtr task, RawVoidPtr resp, RawVoidPtr waker)
{
    auto & read_index_task = *reinterpret_cast<MockReadIndexTask *>(task);
    auto * async_waker = reinterpret_cast<MockAsyncWaker *>(waker);
    auto res = read_index_task.data->poll(async_waker ? async_waker->data : nullptr);
    if (res)
    {
        auto buff = res->SerializePartialAsString();
        SetPBMsByBytes(MsgPBType::ReadIndexResponse, resp, BaseBuffView{buff.data(), buff.size()});
        return 1;
    }
    else
    {
        return 0;
    }
}

void fn_gc_rust_ptr(RawVoidPtr ptr, RawRustPtrType type_)
{
    if (!ptr)
        return;
    auto type = static_cast<RawObjType>(type_);
    GCMonitor::instance().add(type, -1);
    switch (type)
    {
    case RawObjType::None:
        break;
    case RawObjType::MockReadIndexTask:
        delete reinterpret_cast<MockReadIndexTask *>(ptr);
        break;
    case RawObjType::MockAsyncWaker:
        delete reinterpret_cast<MockAsyncWaker *>(ptr);
        break;
    }
}

void fn_handle_batch_read_index(RaftStoreProxyPtr, CppStrVecView, RawVoidPtr, uint64_t, void (*)(RawVoidPtr, BaseBuffView, uint64_t))
{
    throw Exception("`fn_handle_batch_read_index` is deprecated");
}

KVGetStatus fn_get_region_local_state(RaftStoreProxyPtr ptr, uint64_t region_id, RawVoidPtr data, RawCppStringPtr * error_msg)
{
    if (!ptr.inner)
    {
        *error_msg = RawCppString::New("RaftStoreProxyPtr is none");
        return KVGetStatus::Error;
    }
    auto & x = as_ref(ptr);
    auto region = x.getRegion(region_id);
    if (region)
    {
        auto state = region->getState();
        auto buff = state.SerializePartialAsString();
        SetPBMsByBytes(MsgPBType::RegionLocalState, data, BaseBuffView{buff.data(), buff.size()});
        return KVGetStatus::Ok;
    }
    else
        return KVGetStatus::NotFound;
}

TiFlashRaftProxyHelper MockRaftStoreProxy::SetRaftStoreProxyFFIHelper(RaftStoreProxyPtr proxy_ptr)
{
    TiFlashRaftProxyHelper res{};
    res.proxy_ptr = proxy_ptr;
    res.fn_make_read_index_task = fn_make_read_index_task;
    res.fn_poll_read_index_task = fn_poll_read_index_task;
    res.fn_make_async_waker = fn_make_async_waker;
    res.fn_handle_batch_read_index = fn_handle_batch_read_index;
    res.fn_get_region_local_state = fn_get_region_local_state;
    {
        // make sure such function pointer will be set at most once.
        static std::once_flag flag;
        std::call_once(flag, []() { MockSetFFI::MockSetRustGcHelper(fn_gc_rust_ptr); });
    }

    return res;
}

raft_serverpb::RegionLocalState MockProxyRegion::getState()
{
    auto _ = genLockGuard();
    return state;
}

raft_serverpb::RaftApplyState MockProxyRegion::getApply()
{
    auto _ = genLockGuard();
    return apply;
}

uint64_t MockProxyRegion::getLatestCommitIndex()
{
    return this->getApply().commit_index();
}

void MockProxyRegion::updateCommitIndex(uint64_t index)
{
    auto _ = genLockGuard();
    this->apply.set_commit_index(index);
}

void MockProxyRegion::setSate(raft_serverpb::RegionLocalState s)
{
    auto _ = genLockGuard();
    this->state = s;
}

MockProxyRegion::MockProxyRegion(uint64_t id_)
    : id(id_)
{
    apply.set_commit_index(5);
    state.mutable_region()->set_id(id);
}

std::optional<kvrpcpb::ReadIndexResponse> RawMockReadIndexTask::poll(std::shared_ptr<MockAsyncNotifier> waker)
{
    auto _ = genLockGuard();

    if (!finished)
    {
        if (waker != this->waker)
        {
            this->waker = waker;
        }
        return {};
    }
    if (has_lock)
    {
        resp.mutable_locked();
        return resp;
    }
    if (has_region_error)
    {
        resp.mutable_region_error()->mutable_data_is_not_ready();
        return resp;
    }
    resp.set_read_index(region->getLatestCommitIndex());
    return resp;
}

void RawMockReadIndexTask::update(bool lock, bool region_error)
{
    {
        auto _ = genLockGuard();
        if (finished)
            return;
        finished = true;
        has_lock = lock;
        has_region_error = region_error;
    }
    if (waker)
        waker->wake();
}

MockProxyRegionPtr MockRaftStoreProxy::getRegion(uint64_t id)
{
    auto _ = genLockGuard();
    return doGetRegion(id);
}

MockProxyRegionPtr MockRaftStoreProxy::doGetRegion(uint64_t id)
{
    if (auto it = regions.find(id); it != regions.end())
    {
        return it->second;
    }
    return nullptr;
}

MockReadIndexTask * MockRaftStoreProxy::makeReadIndexTask(kvrpcpb::ReadIndexRequest req)
{
    auto _ = genLockGuard();

    wake();

    auto region = doGetRegion(req.context().region_id());
    if (region)
    {
        auto * r = new MockReadIndexTask{};
        r->data = std::make_shared<RawMockReadIndexTask>();
        r->data->req = std::move(req);
        r->data->region = region;
        tasks.push_back(r->data);
        return r;
    }
    return nullptr;
}

void MockRaftStoreProxy::init(size_t region_num)
{
    auto _ = genLockGuard();
    for (size_t i = 0; i < region_num; ++i)
    {
        regions.emplace(i, std::make_shared<MockProxyRegion>(i));
    }
}

size_t MockRaftStoreProxy::size() const
{
    auto _ = genLockGuard();
    return regions.size();
}

void MockRaftStoreProxy::wake()
{
    notifier.wake();
}

void MockRaftStoreProxy::testRunNormal(const std::atomic_bool & over)
{
    while (!over)
    {
        runOneRound();
        notifier.blockedWaitFor(std::chrono::seconds(1));
    }
}

void MockRaftStoreProxy::runOneRound()
{
    auto _ = genLockGuard();
    while (!tasks.empty())
    {
        auto & t = *tasks.front();
        if (!region_id_to_drop.count(t.req.context().region_id()))
        {
            if (region_id_to_error.count(t.req.context().region_id()))
                t.update(false, true);
            else
                t.update(false, false);
        }
        tasks.pop_front();
    }
}

void MockRaftStoreProxy::unsafeInvokeForTest(std::function<void(MockRaftStoreProxy &)> && cb)
{
    auto _ = genLockGuard();
    cb(*this);
}

void GCMonitor::add(RawObjType type, int64_t diff)
{
    auto _ = genLockGuard();
    data[type] += diff;
}

bool GCMonitor::checkClean()
{
    auto _ = genLockGuard();
    for (auto && d : data)
    {
        if (d.second)
            return false;
    }
    return true;
}

bool GCMonitor::empty()
{
    auto _ = genLockGuard();
    return data.empty();
}

} // namespace DB
