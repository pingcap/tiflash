// Copyright 2024 PingCAP, Inc.
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

#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/RestoreDMFile.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/Page/PageStorage.h>
#include <Storages/Page/V3/Universal/UniversalPageIdFormatImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>

namespace DB::DM
{

DMFilePtr restoreDMFileFromRemoteDataSource(
    const DMContext & dm_context,
    Remote::IDataStorePtr remote_data_store,
    UInt64 file_page_id)
{
    auto path_delegate = dm_context.path_pool->getStableDiskDelegator();
    auto wn_ps = dm_context.global_context.getWriteNodePageStorage();
    auto full_page_id = UniversalPageIdFormat::toFullPageId(
        UniversalPageIdFormat::toFullPrefix(dm_context.keyspace_id, StorageType::Data, dm_context.physical_table_id),
        file_page_id);
    auto full_external_id = wn_ps->getNormalPageId(full_page_id);
    auto local_external_id = UniversalPageIdFormat::getU64ID(full_external_id);
    auto remote_data_location = wn_ps->getCheckpointLocation(full_page_id);
    const auto & lock_key_view = S3::S3FilenameView::fromKey(*(remote_data_location->data_file_id));
    auto file_oid = lock_key_view.asDataFile().getDMFileOID();
    auto prepared = remote_data_store->prepareDMFile(file_oid, file_page_id);
    auto dmfile = prepared->restore(DMFileMeta::ReadMode::all());
    // gc only begin to run after restore so we can safely call addRemoteDTFileIfNotExists here
    path_delegate.addRemoteDTFileIfNotExists(local_external_id, dmfile->getBytesOnDisk());
    return dmfile;
}

DMFilePtr restoreDMFileFromLocal(const DMContext & dm_context, UInt64 file_page_id)
{
    auto path_delegate = dm_context.path_pool->getStableDiskDelegator();
    auto file_id = dm_context.storage_pool->dataReader()->getNormalPageId(file_page_id);
    auto file_parent_path = dm_context.path_pool->getStableDiskDelegator().getDTFilePath(file_id);
    auto dmfile = DMFile::restore(
        dm_context.global_context.getFileProvider(),
        file_id,
        file_page_id,
        file_parent_path,
        DMFileMeta::ReadMode::all(),
        dm_context.keyspace_id);
    auto res = path_delegate.updateDTFileSize(file_id, dmfile->getBytesOnDisk());
    RUNTIME_CHECK_MSG(res, "update dt file size failed, path={}", dmfile->path());
    return dmfile;
}

DMFilePtr restoreDMFileFromCheckpoint(
    const DMContext & dm_context,
    Remote::IDataStorePtr remote_data_store,
    UniversalPageStoragePtr temp_ps,
    WriteBatches & wbs,
    UInt64 file_page_id)
{
    auto full_page_id = UniversalPageIdFormat::toFullPageId(
        UniversalPageIdFormat::toFullPrefix(dm_context.keyspace_id, StorageType::Data, dm_context.physical_table_id),
        file_page_id);
    auto remote_data_location = temp_ps->getCheckpointLocation(full_page_id);
    auto data_key_view = S3::S3FilenameView::fromKey(*(remote_data_location->data_file_id)).asDataFile();
    auto file_oid = data_key_view.getDMFileOID();
    auto data_key = data_key_view.toFullKey();
    auto delegator = dm_context.path_pool->getStableDiskDelegator();
    auto new_local_page_id = dm_context.storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
    PS::V3::CheckpointLocation loc{
        .data_file_id = std::make_shared<String>(data_key),
        .offset_in_file = 0,
        .size_in_file = 0,
    };
    wbs.data.putRemoteExternal(new_local_page_id, loc);
    auto prepared = remote_data_store->prepareDMFile(file_oid, new_local_page_id);
    auto dmfile = prepared->restore(DMFileMeta::ReadMode::all());
    wbs.writeLogAndData();
    // new_local_page_id is already applied to PageDirectory so we can safely call addRemoteDTFileIfNotExists here
    delegator.addRemoteDTFileIfNotExists(new_local_page_id, dmfile->getBytesOnDisk());
    return dmfile;
}

} // namespace DB::DM
