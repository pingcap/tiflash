
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/Page/V3/PageDirectory.h>
#include <Storages/Page/V3/Remote/CheckpointDataFileWriter.h>
#include <Storages/Page/V3/Remote/CheckpointFilesWriter.h>
#include <Storages/Page/V3/Remote/CheckpointManifestFileWriter.h>
#include <Storages/Page/V3/Remote/CheckpointUploadManager.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>
#include <TestUtils/MockS3Client.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/config/ConfigAndCredentialsCacheManager.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3EndpointProvider.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <mutex>

namespace DB::PS::V3
{
CheckpointUploadManagerPtr CheckpointUploadManager::createForDebug(UInt64 store_id, PageDirectoryPtr & directory, BlobStorePtr & blob_store)
{
    auto mgr = std::unique_ptr<CheckpointUploadManager>(new CheckpointUploadManager(directory, blob_store));
    mgr->store_id = store_id;

    // TODO: remove hardcode params
    const auto * access_key_id = "minioadmin";
    const auto * secret_access_key = "minioadmin";
    const auto * bucket_name = "jayson";
    const auto * endpoint = "172.16.5.85:9000";

    mgr->s3_bucket = bucket_name;
    mgr->s3_client = S3::ClientFactory::instance().create(
        endpoint,
        Aws::Http::Scheme::HTTP,
        false,
        access_key_id,
        secret_access_key);

    return mgr;
}

CheckpointUploadManager::CheckpointUploadManager(PageDirectoryPtr & directory_, BlobStorePtr & blob_store_)
    : store_id(0)
    , page_directory(directory_)
    , blob_store(blob_store_)
    , log(Logger::get())
{
}


struct ManifestListResult
{
    Strings all_manifest;
    const String latest_manifest;
    const UInt64 latest_upload_seq;
};

ManifestListResult listManifest(
    const Aws::S3::S3Client & client,
    const String & bucket,
    UInt64 store_id,
    const LoggerPtr & log)
{
    Strings all_manifest;
    String latest_manifest;
    UInt64 latest_upload_seq = 0;

    const auto store_prefix = S3::S3Filename::fromStoreId(store_id).toManifestPrefix();

    S3::listPrefix(client, bucket, store_prefix, "", [&](const Aws::S3::Model::ListObjectsV2Result & result) {
        const auto & objects = result.GetContents();
        all_manifest.reserve(all_manifest.size() + objects.size());
        for (const auto & object : objects)
        {
            const auto & mf_key = object.GetKey();
            LOG_TRACE(log, "mf_key={}", mf_key);
            const auto filename_view = S3::S3FilenameView::fromKey(mf_key);
            RUNTIME_CHECK(filename_view.type == S3::S3FilenameType::CheckpointManifest, mf_key);
            // TODO: also store the object.GetLastModified() for removing
            // outdated manifest objects
            all_manifest.emplace_back(mf_key);
            auto upload_seq = filename_view.getUploadSequence();
            if (upload_seq > latest_upload_seq)
            {
                latest_upload_seq = upload_seq;
                latest_manifest = mf_key;
            }
        }
        return objects.size();
    });
    return ManifestListResult{
        .all_manifest = std::move(all_manifest),
        .latest_manifest = std::move(latest_manifest),
        .latest_upload_seq = latest_upload_seq,
    };
}

void CheckpointUploadManager::initStoreInfo(UInt64 actual_store_id)
{
    if (inited_from_s3)
        return;

    do
    {
        std::unique_lock lock_init(mtx_store_init);
        // Another thread has already init before this thread acquire lock
        if (inited_from_s3)
            break;

        // we need to restore the last_upload_sequence from S3
        const auto manifests = listManifest(*s3_client, s3_bucket, actual_store_id, log);
        last_upload_sequence = manifests.latest_upload_seq;
        store_id = actual_store_id;
        LOG_INFO(log, "restore the last upload sequence from S3, sequence={}", last_upload_sequence);
        inited_from_s3 = true;
    } while (false);
    cv_init.notify_all();
}

bool CheckpointUploadManager::createS3LockForWriteBatch(UniversalWriteBatch & write_batch)
{
    {
        std::unique_lock lock_init(mtx_store_init);
        cv_init.wait(lock_init, [this]() { return this->store_id != 0; });
    }

    for (auto & w : write_batch.getMutWrites())
    {
        switch (w.type)
        {
        case WriteBatchWriteType::PUT_EXTERNAL:
        case WriteBatchWriteType::PUT:
        {
            // apply a put/put external that is actually stored in S3 instead of local
            // Note that origin `w.remote->data_file_id` store the S3 key name of
            // CheckpointDataFile or StableFile.
            // Here we will replace the name to be the S3LockFile key name for later
            // manifest upload.
            if (!w.remote)
                continue;
            auto res = S3::S3FilenameView::fromKey(*w.remote->data_file_id);
            if (!res.isDataFile())
                continue;
            auto create_res = createS3Lock(res, store_id);
            if (!create_res.ok())
                return false;
            // TODO: shared the data_file_id
            w.remote = w.remote->copyWithNewFilename(std::move(create_res.lock_key));
            break;
        }
        default:
            break;
        }
    }
    return true;
}

CheckpointUploadManager::S3LockCreateResult
CheckpointUploadManager::createS3Lock(const S3::S3FilenameView & s3_file, UInt64 lock_store_id)
{
    RUNTIME_CHECK(s3_file.isDataFile());
    bool s3_lock_created = false;

    // To ensuring the lock files with `upload_seq` have been uploaded before the
    // manifest with same `upload_seq` upload, acquire a lock to block manifest
    // upload.
    std::shared_lock manifest_lock(mtx_checkpoint_manifest);
    const UInt64 upload_seq = last_upload_sequence + 1;
    const String lockkey = s3_file.getLockKey(lock_store_id, upload_seq);
    LOG_INFO(log, "S3 lock creating: {}", lockkey);
    String err_msg;

    if (s3_file.store_id == lock_store_id)
    {
        // Try to create a lock file for the data file uploaded by this store
        // TODO: handle s3 network error. retry?
        S3::uploadEmptyFile(*s3_client, s3_bucket, lockkey);
        LOG_DEBUG(log, "S3 lock created: {}", lockkey);
        s3_lock_created = true;
    }
    else
    {
        // TODO: Send rpc to S3LockService
        RUNTIME_CHECK(s3_file.store_id == lock_store_id, s3_file.store_id, lock_store_id);
        S3::uploadEmptyFile(*s3_client, s3_bucket, lockkey);
        s3_lock_created = true;
    }

    if (!s3_lock_created)
        return S3LockCreateResult{"", std::move(err_msg)};

    // The related S3 data files in write batch is not applied into PageDirectory,
    // but we need to ensure they exist in the next manifest file so that these
    // S3 data files will not be deleted by the S3GCManager.
    // Add the lock file key to `pre_locks_files` for manifest uploading.
    pre_locks_files.emplace(lockkey);
    return {lockkey, std::move(err_msg)};
}

void CheckpointUploadManager::cleanAppliedS3ExternalFiles(std::unordered_set<String> && applied_s3files)
{
    // After the entries applied into PageDirectory, manifest can get the S3 lock key
    // from `VersionedPageEntries`, cleanup the pre lock files.
    std::shared_lock manifest_lock(mtx_checkpoint_manifest);
    for (const auto & file : applied_s3files)
    {
        pre_locks_files.erase(file);
    }
}

CheckpointUploadManager::DumpRemoteCheckpointResult
CheckpointUploadManager::dumpRemoteCheckpoint(DumpRemoteCheckpointOptions options)
{
    using Trait = PS::V3::universal::PageDirectoryTrait;
    std::scoped_lock lock(mtx_checkpoint);

    RUNTIME_CHECK(endsWith(options.temp_directory, "/"));

    // FIXME: We need to dump snapshot from files, in order to get a correct `being_ref_count`.
    //  Note that, snapshots from files does not have a correct remote info, so we cannot simply
    //  copy logic from `tryDumpSnapshot`.
    //  Currently this is fine, because we will not reclaim data from the PageStorage.

    // Let's keep this snapshot until all finished, so that blob data will not be GCed.
    auto snap = page_directory->createSnapshot(/*tracing_id*/ "");
    if (snap->sequence == last_checkpoint_sequence)
    {
        LOG_TRACE(log, "Skipped dump checkpoint because sequence is unchanged, last_seq={} this_seq={}", last_checkpoint_sequence, snap->sequence);
        return {};
    }

    LOG_INFO(log, "Start dumpRemoteCheckpoint");

    auto edit_from_mem = page_directory->dumpSnapshotToEdit(snap);
    LOG_DEBUG(log, "Dumped edit from PageDirectory, snap_seq={} n_edits={}", snap->sequence, edit_from_mem.size());

    // As a checkpoint, we write both entries (in manifest) and its data.
    // Some entries' data may be already written by a previous checkpoint. These data will not be written again.

    // TODO: Check temp file exists.
    UInt64 upload_sequence;
    Strings data_file_keys;
    String manifest_file_key;
    String local_manifest_file_path_temp;
    {
        // Acquire as read lock, so that it won't block other thread from
        // creating new lock on S3 for a long time.
        // Note that the S3 keyname must be decided by upload_sequence but not snapshot_sequence
        // for correctness.
        std::shared_lock manifest_lock(mtx_checkpoint_manifest);
        upload_sequence = last_upload_sequence + 1;

        data_file_keys.push_back(S3::S3Filename::newCheckpointData(options.writer_info->store_id(), upload_sequence, 0).toFullKey());
        auto local_data_file_path_temp = options.temp_directory + data_file_keys[0] + ".tmp";

        manifest_file_key = S3::S3Filename::newCheckpointManifest(options.writer_info->store_id(), upload_sequence).toFullKey();
        local_manifest_file_path_temp = options.temp_directory + manifest_file_key + ".tmp";

        Poco::File(Poco::Path(local_data_file_path_temp).parent()).createDirectories();
        Poco::File(Poco::Path(local_manifest_file_path_temp).parent()).createDirectories();

        LOG_DEBUG(log, "data_file_path_temp={} manifest_file_path_temp={}", local_data_file_path_temp, local_manifest_file_path_temp);


        auto data_writer = CheckpointDataFileWriter<Trait>::create(
            typename CheckpointDataFileWriter<Trait>::Options{
                .file_path = local_data_file_path_temp,
                .file_id = data_file_keys[0],
            });
        auto manifest_writer = CheckpointManifestFileWriter<Trait>::create(
            typename CheckpointManifestFileWriter<Trait>::Options{
                .file_path = local_manifest_file_path_temp,
                .file_id = manifest_file_key,
            });
        auto writer = CheckpointFilesWriter<Trait>::create(
            typename CheckpointFilesWriter<Trait>::Options{
                .info = typename CheckpointFilesWriter<Trait>::Info{
                    .writer = options.writer_info,
                    .sequence = snap->sequence,
                    .last_sequence = 0,
                },
                .data_writer = std::move(data_writer),
                .manifest_writer = std::move(manifest_writer),
                .blob_store = blob_store,
                .log = log,
            });

        writer->writePrefix();
        bool has_new_data = writer->writeEditsAndApplyRemoteInfo(edit_from_mem, pre_locks_files);
        writer->writeSuffix();

        writer.reset();

        if (has_new_data)
        {
            // Copy back the remote info to the current PageStorage. New remote infos are attached in `writeEditsAndApplyRemoteInfo`.
            // Snapshot cannot prevent obsolete entries from being deleted.
            // For example, if there is a `Put 1` with sequence 10, `Del 1` with sequence 11,
            // and the snapshot sequence is 12, Page with id 1 may be deleted by the gc process.
            page_directory->copyRemoteInfoFromEdit(edit_from_mem, /* allow_missing */ true);
        }

        if (!has_new_data)
        {
            data_file_keys.clear();
        }
        else
        {
            for (const auto & k : data_file_keys)
            {
                S3::uploadFile(*s3_client, s3_bucket, local_data_file_path_temp, k);
                auto view = S3::S3FilenameView::fromKey(k);
                auto lock_created = createS3Lock(view, options.writer_info->store_id());
                RUNTIME_CHECK(lock_created.ok(), lock_created.lock_key, lock_created.err_msg);
            }
        }
    }

    {
        // Acquire write lock to ensure all locks with the same upload_sequence are uploaded
        std::unique_lock manifest_lock(mtx_checkpoint_manifest);
        S3::uploadFile(*s3_client, s3_bucket, local_manifest_file_path_temp, manifest_file_key);

        // Move forward
        last_upload_sequence = upload_sequence;
        last_checkpoint_sequence = snap->sequence;
    }
    LOG_DEBUG(log, "Upload checkpoint done, last_upload_sequence={}, last_checkpoint_sequence={}", last_upload_sequence, last_checkpoint_sequence);

    // TODO: Remove the local temp files after uploaded

    return DumpRemoteCheckpointResult{
        .data_file = std::move(data_file_keys),
        .manifest_file = std::move(manifest_file_key),
    };
}


} // namespace DB::PS::V3
