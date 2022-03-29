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

#pragma once

#include <Core/Types.h>
#include <Encryption/FileProvider.h>
#include <Encryption/WriteReadableFile.h>
#include <Poco/Logger.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/WriteBatch.h>
#include <Storages/PathPool.h>

namespace DB::PS::V3
{
class BlobFile
{
public:
    BlobFile(String path_,
             BlobFileId blob_id_,
             FileProviderPtr file_provider_,
             PSDiskDelegatorPtr delegator_);

    ~BlobFile();

    String getPath()
    {
        return path;
    }

    EncryptionPath getEncryptionPath()
    {
        return EncryptionPath(getPath(), "");
    }

    BlobFileId getBlobFileId();

    void read(char * buffer, size_t offset, size_t size, const ReadLimiterPtr & read_limiter);

    void write(char * buffer, size_t offset, size_t size, const WriteLimiterPtr & write_limiter);

    void truncate(size_t size);

    void remove();

private:
    BlobFileId blob_id;

    FileProviderPtr file_provider;
    PSDiskDelegatorPtr delegator;
    String path;

    WriteReadableFilePtr wrfile;

    std::mutex file_size_lock;
    BlobFileOffset file_size;
};
using BlobFilePtr = std::shared_ptr<BlobFile>;

} // namespace DB::PS::V3