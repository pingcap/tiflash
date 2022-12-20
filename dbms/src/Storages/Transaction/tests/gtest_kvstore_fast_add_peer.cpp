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

#include "Debug/MockRaftStoreProxy.h"
#include "kvstore_helper.h"
#include <Storages/Page/V3/Remote/CheckpointManifestFileReader.h>

namespace DB
{
namespace tests
{

using PS::V3::universal::PageDirectoryTrait;
using PS::V3::universal::BlobStoreTrait;
using PS::V3::PageDirectory;
using PS::V3::CheckpointManifestFileReader;
using PS::V3::RemoteDataLocation;
using PS::V3::Remote::WriterInfo;

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
    auto log = &Poco::Logger::get("fast add");
    {
        auto region_id = 1;
        {
            KVStore & kvs = getKVS();
            auto & page_storage = kvs.region_persister->global_uni_page_storage;

            proxy_instance->bootstrap(kvs, ctx.getTMTContext(), region_id);
            auto [index, term] = proxy_instance->normalWrite(region_id, {34}, {"v2"}, {WriteCmdType::Put}, {ColumnFamilyType::Default});
            MockRaftStoreProxy::FailCond cond;
            proxy_instance->doApply(kvs, ctx.getTMTContext(), cond, region_id, index);
            auto region = proxy_instance->getRegion(region_id);
            auto wb = region->persistMeta();
            page_storage->write(std::move(wb), nullptr);

            // There shall be data to flush.
            ASSERT_EQ(kvs.needFlushRegionData(region_id, ctx.getTMTContext()), true);
            ASSERT_EQ(kvs.tryFlushRegionData(region_id, false, false, ctx.getTMTContext(), 0, 0), true);

            auto writer_info = std::make_shared<WriterInfo>();
            writer_info->set_store_id(kvs.getStore().store_id.load());
            std::string output_directory = TiFlashTestEnv::getTemporaryPath("FastAddPeer/");
            
            page_storage->page_directory->dumpRemoteCheckpoint(PageDirectory<PageDirectoryTrait>::DumpRemoteCheckpointOptions<BlobStoreTrait>{
                .temp_directory = output_directory,
                .remote_directory = output_directory,
                .data_file_name_pattern = "{sequence}_{sub_file_index}.data",
                .manifest_file_name_pattern = "{sequence}.manifest",
                .writer_info = writer_info,
                .blob_store = *page_storage->blob_store,
            });

            // Find the newest manifest
            Poco::DirectoryIterator it(output_directory);
            Poco::DirectoryIterator end;
            std::string optimal;
            auto optimal_index = 0;
            while (it != end) {
                if (it->isFile()) {
                    Poco::Path p(it.path());
                    auto index = 0;
                    std::istringstream(p.getBaseName()) >> index;
                    if (p.getExtension() == "manifest" && index > optimal_index) {
                        optimal_index = index;
                        optimal = p.getFileName();
                    }
                    ++ it;
                }
            }
            LOG_DEBUG(log, "use optimal manifest {}", optimal);
            ASSERT_NE(optimal, "");

            auto reader = CheckpointManifestFileReader<PageDirectoryTrait>::create(CheckpointManifestFileReader<PageDirectoryTrait>::Options{.file_path = output_directory + optimal});
            auto edit = reader->read();
            LOG_DEBUG(log, "content is {}", edit.toDebugString());
            auto records = edit.getRecords();
            ASSERT_EQ(3, records.size());

            raft_serverpb::RegionLocalState restored_region_state;
            raft_serverpb::RaftApplyState restored_apply_state;
            for(auto iter = records.begin(); iter != records.end(); iter++) {
                std::string page_id = iter->page_id;
                if (keys::validateApplyStateKey(page_id.data(), page_id.size(), region_id)) {
                    std::string decoded_data;
                    auto & location = iter->entry.remote_info->data_location;
                    auto buf = ReadBufferFromFile(output_directory + *location.data_file_id);
                    std::string ret;
                    ret.resize(location.size_in_file);
                    buf.seek(location.offset_in_file);
                    auto n = buf.readBig(ret.data(), location.size_in_file);
                    RUNTIME_CHECK(n == location.size_in_file);
                    restored_apply_state.ParseFromArray(ret.data(), ret.size());
                } else if (keys::validateRegionStateKey(page_id.data(), page_id.size(), region_id)) {
                    std::string decoded_data;
                    auto & location = iter->entry.remote_info->data_location;
                    auto buf = ReadBufferFromFile(output_directory + *location.data_file_id);
                    std::string ret;
                    ret.resize(location.size_in_file);
                    buf.seek(location.offset_in_file);
                    auto n = buf.readBig(ret.data(), location.size_in_file);
                    RUNTIME_CHECK(n == location.size_in_file);
                    restored_region_state.ParseFromArray(ret.data(), ret.size());
                } else {
                    std::string decoded_data;
                    auto & location = iter->entry.remote_info->data_location;
                    auto buf = ReadBufferFromFile(output_directory + *location.data_file_id);
                    buf.seek(location.offset_in_file);
                    const auto proxy_helper = std::make_unique<TiFlashRaftProxyHelper>(MockRaftStoreProxy::SetRaftStoreProxyFFIHelper(
                        RaftStoreProxyPtr{proxy_instance.get()}));
                    auto decoded_region = Region::deserialize(buf, proxy_helper.get());
                    ASSERT_EQ(decoded_region->appliedIndex(), index);
                    auto key = RecordKVFormat::genKey(proxy_instance->table_id, 34, 1);
                    TiKVValue value = std::string("v2");
                    EXPECT_THROW(decoded_region->insert(ColumnFamilyType::Default, std::move(key), std::move(value)), Exception);
                    LOG_DEBUG(log, "loaded region {}", decoded_region->getDebugString());
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