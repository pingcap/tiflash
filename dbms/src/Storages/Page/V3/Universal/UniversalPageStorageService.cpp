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

#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromWritableFile.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Poco/TemporaryFile.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/S3/S3Common.h>
#include <Storages/S3/S3Filename.h>
#include <Storages/S3/S3WritableFile.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
UniversalPageStorageServicePtr UniversalPageStorageService::create(
    Context & context,
    const String & name,
    PSDiskDelegatorPtr delegator,
    String && temp_path,
    const PageStorageConfig & config)
{
    auto service = UniversalPageStorageServicePtr(new UniversalPageStorageService(context, std::move(temp_path)));
    service->uni_page_storage = UniversalPageStorage::create(name, delegator, config, context.getFileProvider());
    service->uni_page_storage->restore();
    service->gc_handle = context.getBackgroundPool().addTask(
        [service] {
            return service->uni_page_storage->gc();
        },
        false,
        /*interval_ms*/ 60 * 1000);
    service->checkpoint_handle = context.getBackgroundPool().addTask(
        [service] {
            service->doCheckpoint();
            return false;
        },
        false,
        /*interval_ms*/ 10 * 1000);
    return service;
}

bool UniversalPageStorageService::gc()
{
    Timepoint now = Clock::now();
    const std::chrono::seconds try_gc_period(30);
    if (now < (last_try_gc_time.load() + try_gc_period))
        return false;

    last_try_gc_time = now;
    // TODO: reload config
    return this->uni_page_storage->gc();
}

void UniversalPageStorageService::doCheckpoint(bool force)
{
    Timepoint now = Clock::now();
    if (!force && now < (last_checkpoint_time.load() + Seconds(10)))
        return;

    last_checkpoint_time = now;

    auto store_info = global_context.getTMTContext().getKVStore()->getStoreMeta();
    if (store_info.id() == 0)
    {
        LOG_INFO(Logger::get(), "Skip checkpoint because store meta is not initialized");
        return;
    }

    try
    {
        Stopwatch watch;
        PS::V3::CheckpointProto::WriterInfo writer_info;
        writer_info.set_store_id(store_info.id());
        writer_info.set_version(store_info.version());
        writer_info.set_version_git(store_info.git_hash());
        writer_info.set_start_at_ms(store_info.start_timestamp() * 1000);
        auto remote_info = writer_info.mutable_remote_info();
        remote_info->set_type_name("S3");

        Poco::TemporaryFile temp_checkpoint_dir{temp_path};
        Poco::File(temp_checkpoint_dir).createDirectories();

        auto data_file_id_pattern = S3::S3Filename::newCheckpointDataNameTemplate(store_info.id());
        auto manifest_file_id_pattern = S3::S3Filename::newCheckpointManifestNameTemplate(store_info.id());
        UniversalPageStorage::DumpCheckpointOptions options{
            .data_file_id_pattern = data_file_id_pattern,
            .data_file_path_pattern = temp_checkpoint_dir.path() + "/" + data_file_id_pattern,
            .manifest_file_id_pattern = manifest_file_id_pattern,
            .manifest_file_path_pattern = temp_checkpoint_dir.path() + "/" + manifest_file_id_pattern,
            .writer_info = writer_info,
        };
        auto result = uni_page_storage->dumpIncrementalCheckpoint(options);

        auto file_provider = global_context.getFileProvider();
        // TODO: create related S3 lock
        // Note we must upload data file before manifest file
        for (const auto & data_file_info : result.new_data_files)
        {
            ReadBufferFromFile buf(data_file_info.path);
            WritableFilePtr s3_file = std::make_shared<S3::S3WritableFile>(S3::ClientFactory::instance().sharedClient(),
                                                                           S3::ClientFactory::instance().bucket(),
                                                                           data_file_info.id,
                                                                           S3::WriteSettings{});
            WriteBufferFromWritableFile s3_write_buf(s3_file);
            copyData(buf, s3_write_buf);
            s3_write_buf.sync();
        }
        for (const auto & manifest_file_info : result.new_manifest_files)
        {
            ReadBufferFromFile buf(manifest_file_info.path);
            WritableFilePtr s3_file = std::make_shared<S3::S3WritableFile>(S3::ClientFactory::instance().sharedClient(),
                                                                           S3::ClientFactory::instance().bucket(),
                                                                           manifest_file_info.id,
                                                                           S3::WriteSettings{});
            WriteBufferFromWritableFile s3_write_buf(s3_file);
            copyData(buf, s3_write_buf);
            s3_write_buf.sync();
        }
        LOG_INFO(Logger::get(), "Dump checkpoint finished in {} milliseconds.", watch.elapsedMilliseconds());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

UniversalPageStorageService::~UniversalPageStorageService()
{
    shutdown();
}

void UniversalPageStorageService::shutdown()
{
    if (gc_handle)
    {
        global_context.getBackgroundPool().removeTask(gc_handle);
        gc_handle = nullptr;
    }
    if (checkpoint_handle)
    {
        global_context.getBackgroundPool().removeTask(gc_handle);
        checkpoint_handle = nullptr;
    }
}
} // namespace DB
