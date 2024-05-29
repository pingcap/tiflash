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
#include <Common/Stopwatch.h>
#include <IO/IOThreadPools.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStoreS3.h>
#include <Storages/KVStore/Types.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <aws/core/utils/DateTime.h>
#include <common/logger_useful.h>

#include <future>
#include <unordered_map>

namespace DB::DM::Remote
{
void DataStoreS3::putDMFile(DMFilePtr local_dmfile, const S3::DMFileOID & oid, bool remove_local)
{
    Stopwatch sw;
    RUNTIME_CHECK(local_dmfile->fileId() == oid.file_id);
    RUNTIME_CHECK_MSG(
        oid.store_id != InvalidStoreID,
        "try to upload a DMFile with invalid StoreID, oid={} path={}",
        oid,
        local_dmfile->path());

    const auto local_dir = local_dmfile->path();
    const auto local_files = local_dmfile->listFilesForUpload();
    auto itr_meta = std::find_if(local_files.cbegin(), local_files.cend(), [](const auto & file_name) {
        // We always ensure meta v0 exists.
        return file_name == DMFileMetaV2::metaFileName(0);
    });
    RUNTIME_CHECK(itr_meta != local_files.cend());

    putDMFileLocalFiles(local_dir, local_files, oid);

    if (remove_local)
        local_dmfile->switchToRemote(oid);
}

void DataStoreS3::putDMFileLocalFiles(
    const String & local_dir,
    const std::vector<String> & local_files,
    const S3::DMFileOID & oid)
{
    Stopwatch sw;

    const auto remote_dir = S3::S3Filename::fromDMFileOID(oid).toFullKey();
    LOG_DEBUG(
        log,
        "Start upload DMFile local files, local_dir={} remote_dir={} local_files={}",
        local_dir,
        remote_dir,
        local_files);

    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();

    // First, upload non-meta files.
    std::vector<std::future<void>> upload_results;
    upload_results.reserve(local_files.size() - 1);
    for (const auto & fname : local_files)
    {
        if (DMFileMetaV2::isMetaFileName(fname))
            continue;

        auto local_fname = fmt::format("{}/{}", local_dir, fname);
        auto remote_fname = fmt::format("{}/{}", remote_dir, fname);
        auto task = std::make_shared<std::packaged_task<void()>>(
            [&, local_fname = std::move(local_fname), remote_fname = std::move(remote_fname)]() -> void {
                S3::uploadFile(
                    *s3_client,
                    local_fname,
                    remote_fname,
                    EncryptionPath(local_dir, fname, oid.keyspace_id),
                    file_provider);
            });
        upload_results.push_back(task->get_future());
        DataStoreS3Pool::get().scheduleOrThrowOnError([task]() { (*task)(); });
    }
    for (auto & f : upload_results)
        f.get();

    // Then, upload meta files.
    // Only when the meta upload is successful, the dmfile upload can be considered successful.
    upload_results.clear();
    for (const auto & fname : local_files)
    {
        if (!DMFileMetaV2::isMetaFileName(fname))
            continue;

        auto local_fname = fmt::format("{}/{}", local_dir, fname);
        auto remote_fname = fmt::format("{}/{}", remote_dir, fname);
        auto task = std::make_shared<std::packaged_task<void()>>(
            [&, local_fname = std::move(local_fname), remote_fname = std::move(remote_fname)]() {
                S3::uploadFile(
                    *s3_client,
                    local_fname,
                    remote_fname,
                    EncryptionPath(local_dir, fname, oid.keyspace_id),
                    file_provider);
            });
        upload_results.push_back(task->get_future());
        DataStoreS3Pool::get().scheduleOrThrowOnError([task]() { (*task)(); });
    }
    for (auto & f : upload_results)
        f.get();

    LOG_INFO(log, "Upload DMFile finished, key={}, cost={}ms", remote_dir, sw.elapsedMilliseconds());
}

bool DataStoreS3::putCheckpointFiles(
    const PS::V3::LocalCheckpointFiles & local_files,
    StoreID store_id,
    UInt64 upload_seq)
{
    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();

    /// First upload all CheckpointData files and their locks,
    /// then upload the CheckpointManifest to make the files within
    /// `upload_seq` public to S3GCManager.

    std::vector<std::future<void>> upload_results;
    // upload in parallel
    // Note: Local checkpoint files are always not encrypted.
    for (size_t file_idx = 0; file_idx < local_files.data_files.size(); ++file_idx)
    {
        auto task = std::make_shared<std::packaged_task<void()>>([&, idx = file_idx] {
            const auto & local_datafile = local_files.data_files[idx];
            auto s3key = S3::S3Filename::newCheckpointData(store_id, upload_seq, idx);
            auto lock_key = s3key.toView().getLockKey(store_id, upload_seq);
            S3::uploadFile(
                *s3_client,
                local_datafile,
                s3key.toFullKey(),
                EncryptionPath(local_datafile, "", NullspaceID),
                file_provider);
            S3::uploadEmptyFile(*s3_client, lock_key);
        });
        upload_results.push_back(task->get_future());
        DataStoreS3Pool::get().scheduleOrThrowOnError([task] { (*task)(); });
    }
    for (auto & f : upload_results)
    {
        f.get();
    }

    // upload manifest after all CheckpointData uploaded
    auto s3key = S3::S3Filename::newCheckpointManifest(store_id, upload_seq);
    S3::uploadFile(
        *s3_client,
        local_files.manifest_file,
        s3key.toFullKey(),
        EncryptionPath(local_files.manifest_file, "", NullspaceID),
        file_provider);

    return true; // upload success
}

std::unordered_map<String, IDataStore::DataFileInfo> DataStoreS3::getDataFilesInfo(
    const std::unordered_set<String> & lock_keys)
{
    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();

    std::vector<std::future<std::tuple<String, DataFileInfo>>> actual_sizes;
    for (const auto & lock_key : lock_keys)
    {
        auto task = std::make_shared<std::packaged_task<std::tuple<String, DataFileInfo>()>>(
            [&s3_client, lock_key = lock_key, log = this->log]() noexcept {
                auto key_view = S3::S3FilenameView::fromKey(lock_key);
                auto datafile_key = key_view.asDataFile().toFullKey();
                try
                {
                    auto object_info = S3::tryGetObjectInfo(*s3_client, datafile_key);
                    if (object_info.exist && object_info.size >= 0)
                    {
                        return std::make_tuple(
                            lock_key,
                            DataFileInfo{
                                .size = object_info.size,
                                .mtime = object_info.last_modification_time.UnderlyingTimestamp(),
                            });
                    }
                    // else fallback
                    LOG_WARNING(
                        log,
                        "failed to get S3 object size, key={} datafile={} exist={} size={}",
                        lock_key,
                        datafile_key,
                        object_info.exist,
                        object_info.size);
                }
                catch (...)
                {
                    tryLogCurrentException(
                        log,
                        fmt::format("failed to get S3 object size, key={} datafile={}", lock_key, datafile_key));
                }
                return std::make_tuple(lock_key, DataFileInfo{.size = -1, .mtime = {}});
            });
        actual_sizes.emplace_back(task->get_future());
        DataStoreS3Pool::get().scheduleOrThrowOnError([task] { (*task)(); });
    }

    std::unordered_map<String, DataFileInfo> res;
    for (auto & f : actual_sizes)
    {
        const auto & [file_id, actual_size] = f.get();
        res[file_id] = actual_size;
    }
    return res;
}

void DataStoreS3::copyToLocal(
    const S3::DMFileOID & remote_oid,
    const std::vector<String> & target_short_fnames,
    const String & local_dir)
{
    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
    const auto remote_dir = S3::S3Filename::fromDMFileOID(remote_oid).toFullKey();
    std::vector<std::future<void>> results;
    results.reserve(target_short_fnames.size());
    for (const auto & fname : target_short_fnames)
    {
        auto remote_fname = fmt::format("{}/{}", remote_dir, fname);
        auto local_fname = fmt::format("{}/{}", local_dir, fname);
        auto task = std::make_shared<std::packaged_task<void()>>(
            [&, local_fname = std::move(local_fname), remote_fname = std::move(remote_fname)]() {
                auto tmp_fname = fmt::format("{}.tmp", local_fname);
                S3::downloadFile(*s3_client, tmp_fname, remote_fname);
                Poco::File(tmp_fname).renameTo(local_fname);
            });
        results.push_back(task->get_future());
        DataStoreS3Pool::get().scheduleOrThrowOnError([task]() { (*task)(); });
    }
    for (auto & f : results)
    {
        f.get();
    }
}

void DataStoreS3::setTaggingsForKeys(const std::vector<String> & keys, std::string_view tagging)
{
    auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
    std::vector<std::future<void>> results;
    results.reserve(keys.size());
    for (const auto & k : keys)
    {
        auto task = std::make_shared<std::packaged_task<void()>>(
            [&s3_client, &tagging, key = k] { rewriteObjectWithTagging(*s3_client, key, String(tagging)); });
        results.emplace_back(task->get_future());
        DataStoreS3Pool::get().scheduleOrThrowOnError([task] { (*task)(); });
    }
    for (auto & f : results)
    {
        f.get();
    }
}

IPreparedDMFileTokenPtr DataStoreS3::prepareDMFile(const S3::DMFileOID & oid, UInt64 page_id)
{
    return std::make_shared<S3PreparedDMFileToken>(file_provider, oid, page_id);
}

IPreparedDMFileTokenPtr DataStoreS3::prepareDMFileByKey(const String & remote_key)
{
    const auto view = S3::S3FilenameView::fromKeyWithPrefix(remote_key);
    RUNTIME_CHECK(view.isDMFile(), magic_enum::enum_name(view.type), remote_key);
    auto oid = view.getDMFileOID();
    return prepareDMFile(oid, 0);
}

DMFilePtr S3PreparedDMFileToken::restore(DMFileMeta::ReadMode read_mode, UInt32 meta_version)
{
    return DMFile::restore(
        file_provider,
        oid.file_id,
        page_id,
        S3::S3Filename::fromTableID(oid.store_id, oid.keyspace_id, oid.table_id).toFullKeyWithPrefix(),
        read_mode,
        meta_version,
        oid.keyspace_id);
}
} // namespace DB::DM::Remote
