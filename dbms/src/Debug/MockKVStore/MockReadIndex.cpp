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
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>
#include <Debug/MockKVStore/MockFFIImpls.h>
#include <Debug/MockKVStore/MockRaftStoreProxy.h>
#include <Debug/MockKVStore/MockSSTGenerator.h>
#include <Debug/MockKVStore/MockSSTReader.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgTools.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeInterfaces.h>
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
#include <google/protobuf/text_format.h>

namespace DB
{
RawRustPtr MockFFIImpls::fn_make_read_index_task(RaftStoreProxyPtr ptr, BaseBuffView view)
{
    auto & x = as_ref(ptr);
    kvrpcpb::ReadIndexRequest req;
    req.ParseFromArray(view.data, view.len);
    auto * task = x.makeReadIndexTask(req);
    if (task)
        GCMonitor::instance().add(RawObjType::MockReadIndexTask, 1);
    return RawRustPtr{task, static_cast<uint32_t>(RawObjType::MockReadIndexTask)};
}

RawRustPtr MockFFIImpls::fn_make_async_waker(void (*wake_fn)(RawVoidPtr), RawCppPtr data)
{
    auto * p = new MockAsyncWaker{std::make_shared<MockAsyncNotifier>()};
    p->data->data = data;
    p->data->wake_fn = wake_fn;
    GCMonitor::instance().add(RawObjType::MockAsyncWaker, 1);
    return RawRustPtr{p, static_cast<uint32_t>(RawObjType::MockAsyncWaker)};
}

uint8_t MockFFIImpls::fn_poll_read_index_task(RaftStoreProxyPtr, RawVoidPtr task, RawVoidPtr resp, RawVoidPtr waker)
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

void MockFFIImpls::fn_handle_batch_read_index(
    RaftStoreProxyPtr,
    CppStrVecView,
    RawVoidPtr,
    uint64_t,
    void (*)(RawVoidPtr, BaseBuffView, uint64_t))
{
    throw Exception("`fn_handle_batch_read_index` is deprecated");
}

kvrpcpb::ReadIndexRequest make_read_index_reqs(uint64_t region_id, uint64_t start_ts)
{
    kvrpcpb::ReadIndexRequest req;
    req.set_start_ts(start_ts);
    req.mutable_context()->set_region_id(region_id);
    return req;
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

void MockReadIndex::wakeNotifier()
{
    notifier.wake();
}

void MockReadIndex::runOneRound()
{
    while (!read_index_tasks.empty())
    {
        auto & t = *read_index_tasks.front();
        auto region_id = t.req.context().region_id();
        if (!region_id_to_drop.contains(region_id))
        {
            if (region_id_to_error.contains(region_id))
                t.update(false, true);
            else
                t.update(false, false);
        }
        read_index_tasks.pop_front();
    }
}

} // namespace DB
