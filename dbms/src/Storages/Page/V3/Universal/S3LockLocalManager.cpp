// Copyright 2023 PingCAP, Ltd.
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
#include <Flash/Disaggregated/S3LockClient.h>
#include <Storages/Page/V3/Universal/S3LockLocalManager.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>


namespace DB::ErrorCodes
{
extern const int S3_LOCK_CONFLICT;
}
namespace DB::PS::V3
{

S3LockLocalManager::S3LockLocalManager()
    : store_id(InvalidStoreID)
    , log(Logger::get())
{
    // store_id, s3lock_client are inited later because they may not
    // accessable when UniversalPageStorage is created.
    auto & ins = S3::ClientFactory::instance();
    s3_client = ins.sharedTiFlashClient();
}

void S3LockLocalManager::initStoreInfo(StoreID actual_store_id, DB::S3::S3LockClientPtr s3lock_client_)
{
    if (inited_from_s3)
        return;

    RUNTIME_CHECK(actual_store_id != InvalidStoreID, actual_store_id);
    do
    {
        std::unique_lock latch_init(mtx_store_init);
        // Another thread has already init before this thread acquire lock
        if (inited_from_s3)
            break;

        // we need to restore the last_upload_sequence from S3
        const auto manifests = S3::CheckpointManifestS3Set::getFromS3(*s3_client, actual_store_id);
        if (!manifests.empty())
        {
            last_upload_sequence = manifests.latestUploadSequence();
        }
        else
        {
            last_upload_sequence = 0;
        }

        s3lock_client = std::move(s3lock_client_);
        store_id = actual_store_id;

        LOG_INFO(log, "restore the last upload sequence from S3, sequence={}", last_upload_sequence);

        inited_from_s3 = true;
    } while (false); // release lock_init
    cv_init.notify_all();
}

S3LockLocalManager::ExtraLockInfo
S3LockLocalManager::getUploadLocksInfo()
{
    std::unique_lock wlatch_seq(mtx_sequence);
    std::unique_lock latch_keys(mtx_lock_keys);
    last_upload_sequence += 1;
    UInt64 upload_seq = last_upload_sequence;
    return ExtraLockInfo{
        .upload_sequence = upload_seq,
        .pre_lock_keys = pre_lock_keys,
    };
}

void S3LockLocalManager::createS3LockForWriteBatch(UniversalWriteBatch & write_batch)
{
    {
        std::unique_lock lock_init(mtx_store_init);
        cv_init.wait(lock_init, [this]() { return inited_from_s3.load(); });
    }

    std::map<String, std::shared_ptr<String>> s3_datafiles_to_lock;
    for (const auto & w : write_batch.getWrites())
    {
        switch (w.type)
        {
        case WriteBatchWriteType::PUT_EXTERNAL:
        case WriteBatchWriteType::PUT_REMOTE:
        {
            // apply a put/put external that is actually stored in S3 instead of local
            // Note that origin `w.data_location->data_file_id` store the S3 key name of
            // CheckpointDataFile or StableFile.
            if (!w.data_location)
                continue;
            s3_datafiles_to_lock.emplace(*w.data_location->data_file_id, nullptr);
            break;
        }
        default:
            break;
        }
    }

    for (auto & [datafile_key, lock_key] : s3_datafiles_to_lock)
    {
        auto view = S3::S3FilenameView::fromKey(datafile_key);
        if (!view.isDataFile())
            continue;
        auto lock_result = createS3Lock(datafile_key, view, store_id);
        lock_key = std::make_shared<String>(lock_result);
    }

    for (auto & w : write_batch.getMutWrites())
    {
        // Here we will replace the name to be the S3LockFile key name for later
        // manifest upload.
        switch (w.type)
        {
        case WriteBatchWriteType::PUT_EXTERNAL:
        case WriteBatchWriteType::PUT_REMOTE:
        {
            // TODO: shared the data_file_id between different write batches?
            if (!w.data_location)
                continue;
            auto mapping_iter = s3_datafiles_to_lock.find(*w.data_location->data_file_id);
            if (mapping_iter == s3_datafiles_to_lock.end())
                continue;
            w.data_location = w.data_location->copyWithNewDataFileId(mapping_iter->second);
        }
        default:
            break;
        }
    }
}

String S3LockLocalManager::createS3Lock(const String & datafile_key, const S3::S3FilenameView & s3_file, UInt64 lock_store_id)
{
    RUNTIME_CHECK(s3_file.isDataFile());

    // To ensuring the lock files with `upload_seq` have been uploaded before the
    // manifest with same `upload_seq` upload, acquire a lock to block manifest
    // upload.
    std::shared_lock rlatch_seq(mtx_sequence); // multiple s3 lock can be upload with same `upload_seq` concurrently
    const UInt64 upload_seq = last_upload_sequence + 1;
    String lockkey = s3_file.getLockKey(lock_store_id, upload_seq);

    if (s3_file.store_id == lock_store_id)
    {
        // Create a lock file for the data file created by this store.
        // e.g. the CheckpointDataFile or DTFile generated by this store.
        // directly create lock through S3 client
        // TODO: handle s3 network error and retry?
        S3::uploadEmptyFile(*s3_client, s3_client->bucket(), lockkey);
        LOG_DEBUG(log, "S3 lock created for local datafile, lockkey={}", lockkey);
    }
    else
    {
        // Try to create a lock file for the data file created by another store.
        // e.g. Ingest some pages from CheckpointDataFile or DTFile when doing FAP,
        // send rpc to S3LockService
        RUNTIME_CHECK(s3_file.store_id == lock_store_id, s3_file.store_id, lock_store_id);
        auto [ok, err_msg] = s3lock_client->sendTryAddLockRequest(datafile_key, store_id, upload_seq, 10);
        if (!ok)
        {
            throw Exception(ErrorCodes::S3_LOCK_CONFLICT, err_msg);
        }
        LOG_DEBUG(log, "S3 lock created for ingest datafile, lockkey={}", lockkey);
    }

    // The related S3 data files in write batch is not applied into PageDirectory,
    // but we need to ensure they exist in the next manifest file so that these
    // S3 data files will not be deleted by the S3GCManager.
    // Add the lock file key to `pre_locks_files` for manifest uploading.
    {
        std::unique_lock wlatch_keys(mtx_lock_keys);
        pre_lock_keys.emplace(lockkey);
    }
    return lockkey;
}

void S3LockLocalManager::cleanAppliedS3ExternalFiles(std::unordered_set<String> && applied_s3files)
{
    // After the entries applied into PageDirectory, manifest can get the S3 lock key
    // from `VersionedPageEntries`, cleanup the pre lock files.
    std::unique_lock wlatch_keys(mtx_lock_keys);
    for (const auto & file : applied_s3files)
    {
        pre_lock_keys.erase(file);
    }
}

} // namespace DB::PS::V3
