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

#pragma once

#include <Flash/Disaggregated/S3LockClient.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>

namespace DB::S3
{

// A simple mock lock client for testing.
// Notice: It does NOT guarantee atomicity between
// "try add lock" and "try mark delete" operations
// on the same `data_file_key`.
class MockS3LockClient : public IS3LockClient
{
public:
    explicit MockS3LockClient(std::shared_ptr<TiFlashS3Client> c)
        : s3_client(std::move(c))
    {
    }

    std::pair<bool, String>
    sendTryAddLockRequest(const String & data_file_key, UInt32 lock_store_id, UInt32 lock_seq, Int64) override
    {
        // If the data file exist and no delmark exist, then create a lock file on `data_file_key`
        auto view = S3FilenameView::fromKey(data_file_key);
        if (!objectExists(*s3_client, s3_client->bucket(), data_file_key))
        {
            return {false, ""};
        }
        auto delmark_key = view.getDelMarkKey();
        if (objectExists(*s3_client, s3_client->bucket(), delmark_key))
        {
            return {false, ""};
        }
        uploadEmptyFile(*s3_client, s3_client->bucket(), view.getLockKey(lock_store_id, lock_seq));
        return {true, ""};
    }

    std::pair<bool, String>
    sendTryMarkDeleteRequest(const String & data_file_key, Int64) override
    {
        // If there is no lock on the given `data_file_key`, then mark as deleted
        auto view = S3FilenameView::fromKey(data_file_key);
        auto lock_prefix = view.getLockPrefix();
        bool any_lock_exist = false;
        listPrefix(*s3_client, s3_client->bucket(), lock_prefix, [&any_lock_exist](const Aws::S3::Model::ListObjectsV2Result & result) -> S3::PageResult {
            if (!result.GetContents().empty())
                any_lock_exist = true;
            return S3::PageResult{.num_keys = result.GetContents().size(), .more = false};
        });
        if (any_lock_exist)
        {
            return {false, ""};
        }
        uploadEmptyFile(*s3_client, s3_client->bucket(), view.getDelMarkKey());
        return {true, ""};
    }

private:
    std::shared_ptr<TiFlashS3Client> s3_client;
};

} // namespace DB::S3
