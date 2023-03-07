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

#include <Common/Stopwatch.h>
#include <IO/IOThreadPool.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStoreS3.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>

namespace DB::DM::Remote
{
void DataStoreS3::putDMFile(DMFilePtr local_dmfile, const S3::DMFileOID & oid)
{
    Stopwatch sw;
    RUNTIME_CHECK(local_dmfile->fileId() == oid.file_id);

    const auto local_dir = local_dmfile->path();
    const auto local_files = local_dmfile->listInternalFiles();
    auto itr_meta = std::find(local_files.cbegin(), local_files.cend(), DMFile::metav2FileName());
    RUNTIME_CHECK(itr_meta != local_files.cend());

    const auto remote_dir = S3::S3Filename::fromDMFileOID(oid).toFullKey();
    LOG_DEBUG(log, "Start upload DMFile, local_dir={} remote_dir={} local_files={}", local_dir, remote_dir, local_files);

    auto s3_client = S3::ClientFactory::instance().sharedClient();
    const auto & bucket = S3::ClientFactory::instance().bucket();

    std::vector<std::future<void>> upload_results;
    for (const auto & fname : local_files)
    {
        if (fname == DMFile::metav2FileName())
        {
            // meta file will be upload at last.
            continue;
        }
        auto local_fname = fmt::format("{}/{}", local_dir, fname);
        auto remote_fname = fmt::format("{}/{}", remote_dir, fname);
        auto task = std::make_shared<std::packaged_task<void()>>(
            [&, local_fname = std::move(local_fname), remote_fname = std::move(remote_fname)]() {
                S3::uploadFile(*s3_client, bucket, local_fname, remote_fname);
            });
        upload_results.push_back(task->get_future());
        IOThreadPool::get().scheduleOrThrowOnError([task]() { (*task)(); });
    }
    for (auto & f : upload_results)
    {
        f.get();
    }

    // Only when the meta upload is successful, the dmfile upload can be considered successful.
    auto local_meta_fname = fmt::format("{}/{}", local_dir, DMFile::metav2FileName());
    auto remote_meta_fname = fmt::format("{}/{}", remote_dir, DMFile::metav2FileName());
    S3::uploadFile(*s3_client, bucket, local_meta_fname, remote_meta_fname);

    LOG_INFO(log, "Upload DMFile finished, key={}, cost={}ms", remote_dir, sw.elapsedMilliseconds());
}

void DataStoreS3::copyDMFileMetaToLocalPath(const S3::DMFileOID & remote_oid, const String & local_dir)
{
    Stopwatch sw;
    std::vector<String> target_fnames = {DMFile::metav2FileName(), IDataType::getFileNameForStream(DB::toString(-1), {}) + ".idx"};
    copyToLocal(remote_oid, target_fnames, local_dir);
    LOG_DEBUG(log, "copyDMFileMetaToLocalPath finished. Local_dir={} cost={}ms", local_dir, sw.elapsedMilliseconds());
}

void DataStoreS3::putCheckpointFiles(const LocalCheckpointFiles & local_files, StoreID store_id, UInt64 upload_seq)
{
    auto s3_client = S3::ClientFactory::instance().sharedClient();
    const auto & bucket = S3::ClientFactory::instance().bucket();

    for (size_t file_idx = 0; file_idx < local_files.data_files.size(); ++file_idx)
    {
        const auto & local_datafile = local_files.data_files[file_idx];
        auto s3key = S3::S3Filename::newCheckpointData(store_id, upload_seq, file_idx);
        S3::uploadFile(*s3_client, bucket, local_datafile, s3key.toFullKey());
    }
    auto s3key = S3::S3Filename::newCheckpointManifest(store_id, upload_seq);
    S3::uploadFile(*s3_client, bucket, local_files.manifest_file, s3key.toFullKey());
}

void DataStoreS3::copyToLocal(const S3::DMFileOID & remote_oid, const std::vector<String> & target_short_fnames, const String & local_dir)
{
    auto s3_client = S3::ClientFactory::instance().sharedClient();
    const auto & bucket = S3::ClientFactory::instance().bucket();
    const auto remote_dir = S3::S3Filename::fromDMFileOID(remote_oid).toFullKey();
    std::vector<std::future<void>> results;
    for (const auto & fname : target_short_fnames)
    {
        auto remote_fname = fmt::format("{}/{}", remote_dir, fname);
        auto local_fname = fmt::format("{}/{}", local_dir, fname);
        auto task = std::make_shared<std::packaged_task<void()>>(
            [&, local_fname = std::move(local_fname), remote_fname = std::move(remote_fname)]() {
                auto tmp_fname = fmt::format("{}.tmp", local_fname);
                S3::downloadFile(*s3_client, bucket, tmp_fname, remote_fname);
                Poco::File(tmp_fname).renameTo(local_fname);
            });
        results.push_back(task->get_future());
        IOThreadPool::get().scheduleOrThrowOnError([task]() { (*task)(); });
    }
    for (auto & f : results)
    {
        f.get();
    }
}

IPreparedDMFileTokenPtr DataStoreS3::prepareDMFile(const S3::DMFileOID & oid)
{
    return std::make_shared<S3PreparedDMFileToken>(file_provider, oid);
}

DMFilePtr S3PreparedDMFileToken::restore(DMFile::ReadMetaMode read_mode)
{
    return DMFile::restore(
        file_provider,
        oid.file_id,
        oid.file_id,
        S3::S3Filename::fromTableID(oid.store_id, oid.table_id).toFullKeyWithPrefix(),
        read_mode);
}
} // namespace DB::DM::Remote
