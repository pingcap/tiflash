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

#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <common/types.h>

namespace DB::DM::Remote
{

/**
 * Everything is stored in a local NFS.
 */
class DataStoreNFS : public IDataStore
{
public:
    struct Config
    {
        /**
         * Must trailing with "/".
         */
        String base_directory;
    };

    DataStoreNFS(const Config & config_, const FileProviderPtr & file_provider_)
        : log(Logger::get())
        , file_provider(file_provider_)
        , config(config_)
    {
        RUNTIME_CHECK(file_provider != nullptr);
        RUNTIME_CHECK(endsWith(config.base_directory, "/"), config.base_directory);
        LOG_INFO(log, "DataStoreNFS created, nfs_path={}", config.base_directory);
    }

    class PreparedDMFileToken : public LocalCachePreparedDMFileToken
    {
        friend DataStoreNFS;

    private:
        using LocalCachePreparedDMFileToken::LocalCachePreparedDMFileToken;
    };

    void putDMFile(DMFilePtr local_dm_file, const DMFileOID & oid) override
    {
        RUNTIME_CHECK(local_dm_file->fileId() == oid.file_id);

        Poco::File dtfile_dir(local_dm_file->path());
        const auto remote_path = DMFile::getPathByStatus(buildDMFileParentPathInNFS(oid), local_dm_file->fileId(), DMFile::READABLE);
        LOG_DEBUG(log, "Upload DMFile, oid={} local={} remote={}", oid.info(), local_dm_file->path(), remote_path);
        dtfile_dir.copyTo(remote_path);
    }

    IPreparedDMFileTokenPtr prepareDMFile(const DMFileOID & oid) override
    {
        const auto remote_parent_path = buildDMFileParentPathInNFS(oid);

        LOG_DEBUG(log, "Download DMFile, oid={} remote_parent={} local_cache=(remote_parent)", oid.info(), remote_parent_path);

        // We directly use the NFS directory as the cache directory, relies on OS page cache.
        auto * token = new PreparedDMFileToken(file_provider, oid, remote_parent_path);

        {
            // What we actually want to do is just reading the metadata to ensure the remote file exists.
            // OS will also cache metadata for us if this is the first access, so that
            // later DMFile restore (via token->restore) will be fast.
            auto file = token->restore(DMFile::ReadMetaMode::all());
            RUNTIME_CHECK(file != nullptr);
        }

        return std::shared_ptr<PreparedDMFileToken>(token);
    }

private:
    String buildDMFileParentPathInNFS(const DMFileOID & oid) const
    {
        return fmt::format("{}{}/t_{}", config.base_directory, oid.write_node_id, oid.table_id);
    }

private:
    const LoggerPtr log;
    const FileProviderPtr file_provider;
    const Config config;
};

} // namespace DB::DM::Remote
