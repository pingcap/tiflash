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

#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>
#include <Storages/Transaction/FastAddPeer.h>

#include <chrono>
#include <optional>
#include <thread>

#include "Debug/MockRaftStoreProxy.h"
#include "Storages/Transaction/ProxyFFI.h"
#include "kvstore_helper.h"

namespace DB
{
namespace FailPoints
{
extern const char fast_add_peer_sleep[];
} // namespace FailPoints
namespace tests
{

using PS::V3::CheckpointManifestFileReader;
using PS::V3::PageDirectory;
using PS::V3::RemoteDataLocation;
using PS::V3::Remote::WriterInfo;
using PS::V3::universal::BlobStoreTrait;
using PS::V3::universal::PageDirectoryTrait;

std::string readData(const std::string & output_directory, const RemoteDataLocation & location)
{
    RUNTIME_CHECK(location.offset_in_file > 0);
    RUNTIME_CHECK(location.data_file_id != nullptr && !location.data_file_id->empty());

    std::string ret;
    ret.resize(location.size_in_file);

    auto buf = ReadBufferFromFile(output_directory + *location.data_file_id);
    buf.seek(location.offset_in_file);
    auto n = buf.readBig(ret.data(), location.size_in_file);
    RUNTIME_CHECK(n == location.size_in_file);

    return ret;
}

TEST_F(RegionKVStoreTest, FAPPSCache)
{
    {
        LocalPageStorageCache<int> cache(1);
        cache.insert(1, 10, 1);
        cache.guardedMaybeEvict(1);
        cache.guardedMaybeEvict(2);
        ASSERT_TRUE(cache.maybeGet(1, 10) != std::nullopt);
    }
    {
        LocalPageStorageCache<int> cache(1);
        cache.insert(2, 0, 1);
        cache.guardedMaybeEvict(1);
        cache.guardedMaybeEvict(2);
        cache.guardedMaybeEvict(3);
        ASSERT_TRUE(cache.maybeGet(2, 0) != std::nullopt);
    }
    {
        LocalPageStorageCache<int> cache(1);
        cache.insert(1, 10, 1);
        cache.insert(1, 11, 1);
        cache.guardedMaybeEvict(1);
        ASSERT_TRUE(cache.maybeGet(1, 10) == std::nullopt);
        ASSERT_TRUE(cache.maybeGet(1, 11) != std::nullopt);
    }
    {
        LocalPageStorageCache<int> cache(1);
        cache.insert(1, 10, 1);
        cache.insert(1, 11, 1);
        cache.insert(2, 0, 1);
        cache.guardedMaybeEvict(1);
        ASSERT_TRUE(cache.maybeGet(1, 10) == std::nullopt);
        ASSERT_TRUE(cache.maybeGet(1, 11) != std::nullopt);
    }
    {
        LocalPageStorageCache<int> cache(1);
        cache.insert(1, 0, 1);
        cache.insert(1, 10, 1);
        cache.insert(1, 11, 1);
        cache.guardedMaybeEvict(1);
        ASSERT_TRUE(cache.maybeGet(1, 0) == std::nullopt);
        ASSERT_TRUE(cache.maybeGet(1, 10) == std::nullopt);
        ASSERT_TRUE(cache.maybeGet(1, 11) != std::nullopt);
    }
    {
        LocalPageStorageCache<int> cache(1);
        cache.insert(2, 0, 1);
        cache.insert(2, 10, 1);
        cache.insert(2, 11, 1);
        cache.guardedMaybeEvict(1);
        cache.guardedMaybeEvict(3);
        ASSERT_TRUE(cache.maybeGet(2, 0) != std::nullopt);
        ASSERT_TRUE(cache.maybeGet(2, 10) != std::nullopt);
        ASSERT_TRUE(cache.maybeGet(2, 11) != std::nullopt);
    }
    {
        LocalPageStorageCache<int> cache(1);
        cache.insert(1, 0, 1);
        cache.insert(1, 10, 1);
        cache.insert(2, 0, 1);
        cache.insert(2, 10, 1);
        cache.insert(2, 11, 1);
        cache.insert(3, 0, 1);
        cache.insert(3, 10, 1);
        cache.guardedMaybeEvict(1);
        cache.guardedMaybeEvict(3);
        ASSERT_TRUE(cache.maybeGet(2, 10) != std::nullopt);
        ASSERT_TRUE(cache.maybeGet(2, 11) != std::nullopt);
        ASSERT_TRUE(cache.maybeGet(1, 10) != std::nullopt);
        ASSERT_TRUE(cache.maybeGet(3, 10) != std::nullopt);
        ASSERT_TRUE(cache.maybeGet(1, 0) == std::nullopt);
        ASSERT_TRUE(cache.maybeGet(3, 0) == std::nullopt);
    }
}

void dumpPageStorage(UniversalPageStoragePtr page_storage, uint64_t store_id)
{
    auto * log = &Poco::Logger::get("fast add");
    auto remote_directory = TiFlashTestEnv::getTemporaryPath("FastAddPeer");
    page_storage->config.ps_remote_directory.set(remote_directory + "/");
    const auto & storage_name = page_storage->storage_name;
    std::string output_directory = page_storage->config.ps_remote_directory;
    LOG_DEBUG(log, "output_directory {}", output_directory);
    {
        auto writer_info = std::make_shared<WriterInfo>();
        // write to a remote store
        writer_info->set_store_id(store_id + 1);
        page_storage->page_directory->dumpRemoteCheckpoint(PageDirectory<PageDirectoryTrait>::DumpRemoteCheckpointOptions<BlobStoreTrait>{
            .temp_directory = output_directory,
            .remote_directory = output_directory,
            .data_file_name_pattern = fmt::format(
                "store_{}/ps_{}_data/{{sequence}}_{{sub_file_index}}.data",
                writer_info->store_id(),
                storage_name),
            .manifest_file_name_pattern = fmt::format(
                "store_{}/ps_{}_manifest/{{sequence}}.manifest",
                writer_info->store_id(),
                storage_name),
            .writer_info = writer_info,
            .blob_store = *page_storage->blob_store,
        });
    }
}

void persistAfterWrite(Context & ctx, KVStore & kvs, std::unique_ptr<MockRaftStoreProxy> & proxy_instance, UniversalPageStoragePtr page_storage, uint64_t region_id, uint64_t index)
{
    MockRaftStoreProxy::FailCond cond;
    proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
    auto region = proxy_instance->getRegion(region_id);
    auto wb = region->persistMeta();
    page_storage->write(std::move(wb), nullptr);
    // There shall be data to flush.
    ASSERT_EQ(kvs.needFlushRegionData(region_id, ctx.getTMTContext()), true);
    ASSERT_EQ(kvs.tryFlushRegionData(region_id, false, false, ctx.getTMTContext(), 0, 0), true);
}

TEST_F(RegionKVStoreTest, FAPE2E)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    {
        uint64_t region_id = 1;
        auto peer_id = 1;
        {
            KVStore & kvs = getKVS();
            auto & page_storage = kvs.region_persister->global_uni_page_storage;

            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);

            auto region = proxy_instance->getRegion(region_id);
            auto store_id = kvs.getStore().store_id.load();
            region->addPeer(store_id, peer_id, metapb::PeerRole::Learner);

            // Write some data, and persist meta.
            auto [index, term] = proxy_instance->normalWrite(region_id, {34}, {"v2"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            persistAfterWrite(ctx, kvs, proxy_instance, page_storage, region_id, index);
            dumpPageStorage(page_storage, store_id);

            auto proxy_helper = std::make_unique<TiFlashRaftProxyHelper>(MockRaftStoreProxy::SetRaftStoreProxyFFIHelper(
                RaftStoreProxyPtr{proxy_instance.get()}));
            ;
            EngineStoreServerWrap server_wrap = {
                .tmt = &ctx.getTMTContext(),
                .proxy_helper = proxy_helper.get(),
                .status = std::atomic<EngineStoreServerStatus>{EngineStoreServerStatus::Running},
            };

            FailPointHelper::enableFailPoint(FailPoints::fast_add_peer_sleep);
            auto res = FastAddPeer(&server_wrap, region_id, peer_id);
            FailPointHelper::disableFailPoint(FailPoints::fast_add_peer_sleep);
            ASSERT_EQ(res.status, FastAddPeerStatus::WaitForData);
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1000ms);
            auto res2 = FastAddPeer(&server_wrap, region_id, peer_id);
            ASSERT_EQ(res2.status, FastAddPeerStatus::Ok);
        }
    }
}

TEST_F(RegionKVStoreTest, FAPRestorePS)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    auto * log = &Poco::Logger::get("fast add");
    {
        uint64_t region_id = 1;
        auto peer_id = 1;
        {
            KVStore & kvs = getKVS();
            auto & page_storage = kvs.region_persister->global_uni_page_storage;

            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);
            auto region = proxy_instance->getRegion(region_id);
            auto store_id = kvs.getStore().store_id.load();
            region->addPeer(store_id, peer_id, metapb::PeerRole::Learner);

            // Write some data, and persist meta.
            auto [index, term] = proxy_instance->normalWrite(region_id, {34}, {"v2"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            persistAfterWrite(ctx, kvs, proxy_instance, page_storage, region_id, index);
            dumpPageStorage(page_storage, store_id);

            RemoteMeta remote_meta;
            auto current_store_id = store_id;
            {
                // Must found the newest manifest
                auto [_, maybe_remote_meta] = selectRemotePeer(ctx, page_storage, current_store_id, region_id, peer_id);
                ASSERT_TRUE(maybe_remote_meta.has_value());
                remote_meta = std::move(maybe_remote_meta.value());
                auto & local_ps = ctx.getLocalPageStorageCache();
                LOG_DEBUG(log, "try get {}.{}", remote_meta.remote_store_id, remote_meta.version);
                ASSERT_TRUE(local_ps.maybeGet(remote_meta.remote_store_id, remote_meta.version) != std::nullopt);
            }
            {
                auto [_, maybe_remote_meta2] = selectRemotePeer(ctx, page_storage, current_store_id, region_id, 42);
                ASSERT_TRUE(!maybe_remote_meta2.has_value());
            }

            auto reader = CheckpointManifestFileReader<PageDirectoryTrait>::create(CheckpointManifestFileReader<PageDirectoryTrait>::Options{
                .file_path = remote_meta.checkpoint_path});
            auto edit = reader->read();
            auto records = edit.getRecords();
            ASSERT_EQ(3, records.size());

            LOG_DEBUG(log, "APPLY restored {} origin {}", remote_meta.apply_state.ShortDebugString(), region->getApply().ShortDebugString());
            LOG_DEBUG(log, "REGION restored {} origin {}", remote_meta.region_state.ShortDebugString(), region->getState().ShortDebugString());
            ASSERT_TRUE(remote_meta.apply_state == region->getApply());
            ASSERT_TRUE(remote_meta.region_state == region->getState());

            auto [index2, ter2] = proxy_instance->normalWrite(region_id, {35}, {"v3"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            kvs.setRegionCompactLogConfig(0, 0, 0);
            persistAfterWrite(ctx, kvs, proxy_instance, page_storage, region_id, index2);

            dumpPageStorage(page_storage, store_id);
            {
                auto [_, maybe_remote_meta] = selectRemotePeer(ctx, page_storage, current_store_id, region_id, peer_id);
                ASSERT_TRUE(maybe_remote_meta.has_value());
                RemoteMeta remote_meta2;
                remote_meta2 = maybe_remote_meta.value();
                auto & local_ps = ctx.getLocalPageStorageCache();
                ASSERT_TRUE(local_ps.maybeGet(remote_meta.remote_store_id, remote_meta.version) == std::nullopt);
                ASSERT_TRUE(local_ps.maybeGet(remote_meta2.remote_store_id, remote_meta2.version) != std::nullopt);
            }
        }
    }
}

} // namespace tests
} // namespace DB
