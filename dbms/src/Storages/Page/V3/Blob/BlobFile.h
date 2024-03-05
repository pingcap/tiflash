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

#pragma once

#include <Core/Types.h>
#include <IO/FileProvider/FileProvider.h>
#include <Poco/Logger.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/V3/PageDefines.h>
#include <Storages/PathPool_fwd.h>

namespace DB::PS::V3
{

/**
 * BlobFile is a file that stores the data of multiple pages.
 */
class BlobFile
{
public:
    constexpr static const char * BLOB_PREFIX_NAME = "blobfile_";

public:
    BlobFile(String parent_path_, BlobFileId blob_id_, FileProviderPtr file_provider_, PSDiskDelegatorPtr delegator_);

    ~BlobFile();

    String getPath() const { return fmt::format("{}/{}{}", parent_path, BLOB_PREFIX_NAME, blob_id); }

    EncryptionPath getEncryptionPath() const { return EncryptionPath(getPath(), ""); }

    void read(char * buffer, size_t offset, size_t size, const ReadLimiterPtr & read_limiter, bool background = false);

    void write(
        char * buffer,
        size_t offset,
        size_t size,
        const WriteLimiterPtr & write_limiter,
        bool background = false);

    void truncate(size_t size);

    void remove();

private:
    const BlobFileId blob_id;

    FileProviderPtr file_provider;
    PSDiskDelegatorPtr delegator;
    const String parent_path;

    WriteReadableFilePtr wrfile;

    std::mutex file_size_lock;
    BlobFileOffset file_size;
};
using BlobFilePtr = std::shared_ptr<BlobFile>;

} // namespace DB::PS::V3
