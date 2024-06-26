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

#include <Encryption/WriteBufferFromFileProvider.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileV3IncrementWriter.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/PathPool.h>


namespace DB::DM
{

DMFileV3IncrementWriter::DMFileV3IncrementWriter(const Options & options_)
    : logger(Logger::get())
    , options(options_)
    , dmfile_initial_meta_ver(options.dm_file->metaVersion())
{
    RUNTIME_CHECK(options.dm_file != nullptr);
    RUNTIME_CHECK(options.file_provider != nullptr);
    RUNTIME_CHECK(options.path_pool != nullptr);

    // Should never be called from a Compute Node.

    RUNTIME_CHECK(options.dm_file->meta->format_version == DMFileFormat::V3, options.dm_file->meta->format_version);
    RUNTIME_CHECK(options.dm_file->meta->status == DMFileStatus::READABLE);

    auto dmfile_path = options.dm_file->path();
    auto dmfile_path_s3_view = S3::S3FilenameView::fromKeyWithPrefix(dmfile_path);
    is_s3_dmfile = dmfile_path_s3_view.isDataFile();
    if (is_s3_dmfile)
    {
        // When giving a remote DMFile, we expect to have a remoteDataStore
        // so that our modifications can be uploaded to remote as well.
        RUNTIME_CHECK(options.disagg_ctx && options.disagg_ctx->remote_data_store);
        dmfile_oid = dmfile_path_s3_view.getDMFileOID();
    }

    if (is_s3_dmfile)
    {
        auto delegator = options.path_pool->getStableDiskDelegator();
        auto store_path = delegator.choosePath();
        local_path = getPathByStatus(store_path, options.dm_file->fileId(), DMFileStatus::READABLE);

        auto dmfile_directory = Poco::File(local_path);
        dmfile_directory.createDirectories();
    }
    else
    {
        local_path = options.dm_file->path();
    }
}

void DMFileV3IncrementWriter::include(const String & file_name)
{
    RUNTIME_CHECK(!is_finalized);

    auto file_path = local_path + "/" + file_name;
    auto file = Poco::File(file_path);
    RUNTIME_CHECK(file.exists(), file_path);
    RUNTIME_CHECK(file.isFile(), file_path);

    included_file_names.emplace(file_name);
}

void DMFileV3IncrementWriter::finalize()
{
    // DMFileV3IncrementWriter must be created before making change to DMFile, otherwise
    // a directory may not be correctly prepared. Thus, we could safely assert that
    // DMFile meta version is bumped.
    RUNTIME_CHECK_MSG(
        options.dm_file->metaVersion() != dmfile_initial_meta_ver,
        "Attempt to write with the same meta version when DMFileV3IncrementWriter is created, meta_version={}",
        dmfile_initial_meta_ver);
    RUNTIME_CHECK_MSG(
        options.dm_file->metaVersion() > dmfile_initial_meta_ver,
        "Discovered meta version rollback, old_meta_version={} new_meta_version={}",
        dmfile_initial_meta_ver,
        options.dm_file->metaVersion());

    RUNTIME_CHECK(!is_finalized);

    writeAndIncludeMetaFile();

    LOG_DEBUG(
        logger,
        "Write incremental update for DMFile, local_path={} dmfile_path={} old_meta_version={} new_meta_version={}",
        local_path,
        options.dm_file->path(),
        dmfile_initial_meta_ver,
        options.dm_file->metaVersion());

    if (is_s3_dmfile)
    {
        uploadIncludedFiles();
        removeIncludedFiles();
    }
    else
    {
        // If this is a local DMFile, so be it.
        // The new meta and files are visible from now.
    }

    is_finalized = true;
}

void DMFileV3IncrementWriter::abandonEverything()
{
    if (is_finalized)
        return;

    LOG_DEBUG(logger, "Abandon increment write, local_path={} file_names={}", local_path, included_file_names);

    // TODO: Clean up included files?

    is_finalized = true;
}

DMFileV3IncrementWriter::~DMFileV3IncrementWriter()
{
    if (!is_finalized)
        abandonEverything();
}

void DMFileV3IncrementWriter::writeAndIncludeMetaFile()
{
    // We don't check whether new_meta_version file exists.
    // Because it may be a broken file left behind by previous failed writes.

    auto meta_file_name = DMFileMetaV2::metaFileName(options.dm_file->metaVersion());
    auto meta_file_path = local_path + "/" + meta_file_name;
    // We first write to a temporary file, then rename it to the final name
    // to ensure file's integrity.
    auto meta_file_path_for_write = meta_file_path + ".tmp";

    auto meta_file = std::make_unique<WriteBufferFromFileProvider>(
        options.file_provider,
        meta_file_path_for_write, // Must not use meta->metaPath(), because DMFile may be a S3 DMFile
        EncryptionPath(local_path, meta_file_name),
        /*create_new_encryption_info*/ true,
        options.write_limiter,
        DMFileMetaV2::meta_buffer_size);

    options.dm_file->meta->finalize(*meta_file, options.file_provider, options.write_limiter);
    meta_file->sync();
    meta_file.reset();

    Poco::File(meta_file_path_for_write).renameTo(meta_file_path);

    include(meta_file_name);
}

void DMFileV3IncrementWriter::uploadIncludedFiles()
{
    if (included_file_names.empty())
        return;

    auto data_store = options.disagg_ctx->remote_data_store;
    RUNTIME_CHECK(data_store != nullptr);

    std::vector<String> file_names(included_file_names.begin(), included_file_names.end());
    data_store->putDMFileLocalFiles(local_path, file_names, dmfile_oid);
}

void DMFileV3IncrementWriter::removeIncludedFiles()
{
    if (included_file_names.empty())
        return;

    for (const auto & file_name : included_file_names)
    {
        auto file_path = local_path + "/" + file_name;
        auto file = Poco::File(file_path);
        RUNTIME_CHECK(file.exists(), file_path);
        file.remove();
    }

    included_file_names.clear();

    // TODO: No need to remove from file_provider?
    // TODO: Don't remove encryption info?
}

} // namespace DB::DM
