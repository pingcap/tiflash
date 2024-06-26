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

#pragma once

#include <Common/Logger.h>
#include <IO/FileProvider/FileProvider_fwd.h>
#include <Interpreters/SharedContexts/Disagg_fwd.h>
#include <Storages/DeltaMerge/File/DMFileV3IncrementWriter_fwd.h>
#include <Storages/DeltaMerge/Remote/ObjectId.h>
#include <common/types.h>

#include <unordered_set>

namespace DB
{
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;

class StoragePathPool;
using StoragePathPoolPtr = std::shared_ptr<StoragePathPool>;
} // namespace DB

namespace DB::DM
{
class DMFile;
using DMFilePtr = std::shared_ptr<DMFile>;
} // namespace DB::DM

namespace DB::DM
{

class DMFileV3IncrementWriter
{
public:
    struct Options
    {
        const DMFilePtr dm_file;

        const FileProviderPtr file_provider;
        const WriteLimiterPtr write_limiter;
        const StoragePathPoolPtr path_pool;
        const SharedContextDisaggPtr disagg_ctx;
    };

    /**
     * @brief Create a new DMFileV3IncrementWriter for writing new parts for a DMFile.
     *
     * @param options.dm_file Support both remote or local DMFile. When DMFile is remote,
     * a local directory will be re-prepared for holding these new incremental files.
     *
     * Throws if DMFile is not FormatV3, since other Format Versions cannot update incrementally.
     * Throws if DMFile is not readable. Otherwise (e.g. status=WRITING) DMFile metadata
     *   may be changed by others at any time.
     */
    explicit DMFileV3IncrementWriter(const Options & options);

    static DMFileV3IncrementWriterPtr create(const Options & options)
    {
        return std::make_unique<DMFileV3IncrementWriter>(options);
    }

    ~DMFileV3IncrementWriter();

    /**
     * @brief Include a file. The file must be placed in `localPath()`.
     * The file will be uploaded to S3 with the meta file all at once
     * when `finalize()` is called.
     *
     * In non-disaggregated mode, this function does not take effect.
     */
    void include(const String & file_name);

    /**
     * @brief The path of the local directory of the DMFile.
     * If DMFile is local, it equals to the dmfile->path().
     * If DMFile is on S3, the local path is a temporary directory for holding new incremental files.
     */
    String localPath() const { return local_path; }

    /**
    * @brief Persists the current dmfile in-memory meta using the in-memory meta version.
    * If this meta version is already persisted before, exception **may** be thrown.
    * It is caller's duty to ensure there is no concurrent IncrementWriters for the same dmfile
    * to avoid meta version contention.
    *
    * For a remote DMFile, new meta version file and other files specified via `include()`
    * will be uploaded to S3. Local files will be removed after that.
    */
    void finalize();

    void abandonEverything();

private:
    void writeAndIncludeMetaFile();

    void uploadIncludedFiles();

    void removeIncludedFiles();

private:
    const LoggerPtr logger;
    const Options options;
    const UInt32 dmfile_initial_meta_ver;
    bool is_s3_dmfile = false;
    Remote::DMFileOID dmfile_oid; // Valid when is_s3_dmfile == true
    String local_path;

    std::unordered_set<String> included_file_names;

    bool is_finalized = false;
};

} // namespace DB::DM
