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

#include "Debug/MockRaftStoreProxy.h"
#include "kvstore_helper.h"

namespace DB
{
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

TEST_F(RegionKVStoreTest, FAPRestorePS)
{
    auto ctx = TiFlashTestEnv::getGlobalContext();
    auto * log = &Poco::Logger::get("fast add");
    {
        auto region_id = 1;
        auto peer_id = 1;
        {
            KVStore & kvs = getKVS();
            auto & page_storage = kvs.region_persister->global_uni_page_storage;

            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);
            auto region = proxy_instance->getRegion(region_id);
            auto store_id = kvs.getStore().store_id.load();
            region->addPeer(store_id, peer_id, metapb::PeerRole::Learner);
            auto [index, term] = proxy_instance->normalWrite(region_id, {34}, {"v2"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            MockRaftStoreProxy::FailCond cond;
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            auto wb = region->persistMeta();
            page_storage->write(std::move(wb), nullptr);

            // There shall be data to flush.
            ASSERT_EQ(kvs.needFlushRegionData(region_id, ctx.getTMTContext()), true);
            ASSERT_EQ(kvs.tryFlushRegionData(region_id, false, false, ctx.getTMTContext(), 0, 0), true);

            // Dump PS
            auto writer_info = std::make_shared<WriterInfo>();
            writer_info->set_store_id(store_id);
            auto remote_directory = TiFlashTestEnv::getTemporaryPath("FastAddPeer");
            page_storage->config.ps_remote_directory.set(remote_directory);
            std::string output_directory = composeOutputDirectory(remote_directory, store_id, page_storage->storage_name);
            LOG_DEBUG(log, "output_directory {}", output_directory);
            page_storage->page_directory->dumpRemoteCheckpoint(PageDirectory<PageDirectoryTrait>::DumpRemoteCheckpointOptions<BlobStoreTrait>{
                .temp_directory = output_directory,
                .remote_directory = output_directory,
                .data_file_name_pattern = "{sequence}_{sub_file_index}.data",
                .manifest_file_name_pattern = "{sequence}.manifest",
                .writer_info = writer_info,
                .blob_store = *page_storage->blob_store,
            });

            // Must Found the newest manifest
            auto maybe_remote_meta = selectRemotePeer(page_storage, region_id, peer_id);
            ASSERT(maybe_remote_meta.has_value());

            auto maybe_remote_meta2 = selectRemotePeer(page_storage, region_id, 42);
            ASSERT(!maybe_remote_meta2.has_value());

            auto remote_meta = std::move(maybe_remote_meta.value());
            const auto & [s_id, restored_region_state, restored_apply_state, optimal] = remote_meta;

            auto reader = CheckpointManifestFileReader<PageDirectoryTrait>::create(CheckpointManifestFileReader<PageDirectoryTrait>::Options{.file_path = output_directory + optimal});
            auto edit = reader->read();
            auto records = edit.getRecords();
            ASSERT_EQ(3, records.size());

            for (auto & record : records)
            {
                std::string page_id = record.page_id.toStr();
                if (keys::validateApplyStateKey(page_id.data(), page_id.size(), region_id) || keys::validateRegionStateKey(page_id.data(), page_id.size(), region_id))
                {
                }
                else
                {
                    auto & location = record.entry.remote_info->data_location;
                    auto buf = ReadBufferFromFile(output_directory + *location.data_file_id);
                    buf.seek(location.offset_in_file);

                    const auto proxy_helper = std::make_unique<TiFlashRaftProxyHelper>(MockRaftStoreProxy::SetRaftStoreProxyFFIHelper(
                        RaftStoreProxyPtr{proxy_instance.get()}));
                    auto decoded_region = Region::deserialize(buf, proxy_helper.get());
                    ASSERT_EQ(decoded_region->appliedIndex(), index);
                    auto key = RecordKVFormat::genKey(proxy_instance->table_id, 34, 1);
                    TiKVValue value = std::string("v2");
                    EXPECT_THROW(decoded_region->insert(ColumnFamilyType::Default, std::move(key), std::move(value)), Exception);
                }
            }
            LOG_DEBUG(log, "APPLY restored {} origin {}", restored_apply_state.ShortDebugString(), region->getApply().ShortDebugString());
            LOG_DEBUG(log, "REGION restored {} origin {}", restored_region_state.ShortDebugString(), region->getState().ShortDebugString());
            ASSERT(restored_apply_state == region->getApply());
            ASSERT(restored_region_state == region->getState());
        }
    }
}

} // namespace tests
} // namespace DB
