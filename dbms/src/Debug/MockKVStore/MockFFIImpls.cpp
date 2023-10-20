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

#include <Debug/MockKVStore/MockFFIImpls.h>
#include <Debug/MockKVStore/MockRaftStoreProxy.h>

namespace DB
{
void MockFFIImpls::fn_gc_rust_ptr(RawVoidPtr ptr, RawRustPtrType type_)
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
    case RawObjType::MockString:
        delete reinterpret_cast<std::string *>(ptr);
        break;
    case RawObjType::MockVecOfString:
        delete reinterpret_cast<RustStrWithViewVecInner *>(ptr);
        break;
    }
}

KVGetStatus MockFFIImpls::fn_get_region_local_state(
    RaftStoreProxyPtr ptr,
    uint64_t region_id,
    RawVoidPtr data,
    RawCppStringPtr * error_msg)
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

void MockFFIImpls::fn_notify_compact_log(
    RaftStoreProxyPtr ptr,
    uint64_t region_id,
    uint64_t compact_index,
    uint64_t compact_term,
    uint64_t applied_index)
{
    UNUSED(applied_index);
    // Update flushed applied_index and truncated state.
    auto & x = as_ref(ptr);
    auto region = x.getRegion(region_id);
    ASSERT(region);
    // `applied_index` in proxy's disk can still be less than the `applied_index` here when fg flush.
    if (region && region->getApply().truncated_state().index() < compact_index)
    {
        region->tryUpdateTruncatedState(compact_index, compact_term);
    }
}

RaftstoreVer MockFFIImpls::fn_get_cluster_raftstore_version(RaftStoreProxyPtr ptr, uint8_t, int64_t)
{
    auto & x = as_ref(ptr);
    return x.cluster_ver;
}

// Must call `RustGcHelper` to gc the returned pointer in the end.
RustStrWithView MockFFIImpls::fn_get_config_json(RaftStoreProxyPtr ptr, ConfigJsonType)
{
    auto & x = as_ref(ptr);
    auto * s = new std::string(x.proxy_config_string);
    GCMonitor::instance().add(RawObjType::MockString, 1);
    return RustStrWithView{
        .buff = cppStringAsBuff(*s),
        .inner = RawRustPtr{.ptr = s, .type = static_cast<RawRustPtrType>(RawObjType::MockString)}};
}

} // namespace DB
