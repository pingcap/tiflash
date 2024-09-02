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
#include <Flash/Disaggregated/S3LockClient.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/CheckpointFile/Proto/manifest_file.pb.h>
#include <Storages/Page/V3/Universal/S3LockLocalManager.h>
#include <Storages/Page/V3/Universal/UniversalWriteBatchImpl.h>
#include <Storages/S3/CheckpointManifestS3Set.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3RandomAccessFile.h>

#include <magic_enum.hpp>


namespace DB::ErrorCodes
{
extern const int S3_LOCK_CONFLICT;
}
namespace DB::PS::V3
{

S3LockLocalManager::S3LockLocalManager()
    : store_id(InvalidStoreID)
    , log(Logger::get())
{}

// `store_id` is inited later because they may not
// accessable when S3LockLocalManager is created.
std::optional<CheckpointProto::ManifestFilePrefix> S3LockLocalManager::initStoreInfo(
    StoreID actual_store_id,
    DB::S3::S3LockClientPtr s3lock_client_,
    const universal::PageDirectoryPtr & directory)
{
    if (inited_from_s3)
        return std::nullopt;

    RUNTIME_CHECK(actual_store_id != InvalidStoreID, actual_store_id);
    std::optional<CheckpointProto::ManifestFilePrefix> prefix_opt;
    do
    {
        std::unique_lock latch_init(mtx_store_init);
        // Another thread has already init before this thread acquire lock
        if (inited_from_s3)
            break;

        Stopwatch watch;
        size_t num_s3_entries = 0;
        size_t num_copied_entries = 0;

        // we need to restore the last_upload_sequence from S3
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        const auto manifests = S3::CheckpointManifestS3Set::getFromS3(*s3_client, actual_store_id);
        if (!manifests.empty())
        {
            last_upload_sequence = manifests.latestUploadSequence();
            auto manifest_file = S3::S3RandomAccessFile::create(manifests.latestManifestKey());
            auto reader = CPManifestFileReader::create(CPManifestFileReader::Options{.plain_file = manifest_file});
            prefix_opt = reader->readPrefix();

            CheckpointProto::StringsInternMap strings_map;
            while (true)
            {
                auto edit = reader->readEdits(strings_map);
                if (!edit)
                    break;
                size_t cur_s3_entries = edit->size();
                size_t cur_copied_entries = directory->copyCheckpointInfoFromEdit(*edit);
                num_s3_entries += cur_s3_entries;
                num_copied_entries += cur_copied_entries;
                LOG_INFO(
                    log,
                    "restore from S3, key={} cur_entries={} cur_copied_entries={}",
                    manifests.latestManifestKey(),
                    cur_s3_entries,
                    cur_copied_entries);
            }
        }
        else
        {
            last_upload_sequence = 0;
        }

        s3lock_client = std::move(s3lock_client_);
        RUNTIME_CHECK(s3lock_client != nullptr);

        store_id = actual_store_id;

        LOG_INFO(
            log,
            "restore from S3 finish, elapsed={:.3f}s last_upload_sequence={} num_s3_entries={} num_copied_entries={} "
            "last_prefix={}",
            watch.elapsedSeconds(),
            last_upload_sequence,
            num_s3_entries,
            num_copied_entries,
            prefix_opt ? prefix_opt.value().ShortDebugString() : "{None}");

        inited_from_s3 = true;
    } while (false); // release lock_init
    cv_init.notify_all();
    return prefix_opt;
}

void S3LockLocalManager::waitUntilInited()
{
    std::unique_lock lock_init(mtx_store_init);
    cv_init.wait(lock_init, [this]() { return inited_from_s3.load(); });
}

S3LockLocalManager::ExtraLockInfo S3LockLocalManager::allocateNewUploadLocksInfo()
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
    waitUntilInited();

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

    for (auto & [input_key, lock_key] : s3_datafiles_to_lock)
    {
        auto view = S3::S3FilenameView::fromKey(input_key);
        RUNTIME_CHECK_MSG(
            view.isDataFile() || view.isLockFile(),
            "invalid data_file_id, input_key={} type={}",
            input_key,
            magic_enum::enum_name(view.type));
        if (view.isLockFile())
        {
            lock_key = std::make_shared<String>(input_key);
            continue;
        }
        auto lock_result = createS3Lock(input_key, view, store_id);
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

String S3LockLocalManager::createS3Lock(
    const String & datafile_key,
    const S3::S3FilenameView & s3_file,
    UInt64 lock_store_id)
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
        auto s3_client = S3::ClientFactory::instance().sharedTiFlashClient();
        S3::uploadEmptyFile(*s3_client, lockkey);
        LOG_DEBUG(log, "S3 lock created for local datafile, lockkey={}", lockkey);
    }
    else
    {
        RUNTIME_CHECK_MSG(s3lock_client, "S3 Lock Client is not initialized");
        // Try to create a lock file for the data file created by another store.
        // e.g. Ingest some pages from CheckpointDataFile or DTFile when doing FAP,
        // send rpc to S3LockService
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
