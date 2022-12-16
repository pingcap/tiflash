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

#include <Storages/DeltaMerge/Remote/DataStore/DataStoreNFS.h>

namespace DB::DM::Remote
{

void DataStoreNFS::putDMFile(DMFilePtr local_dmf, const DMFileOID & oid)
{
    RUNTIME_CHECK(local_dmf->fileId() == oid.file_id);

    const auto src_dmf_dir_poco = Poco::File(local_dmf->path());
    const auto dist_dmf_dir = DMFile::getPathByStatus(buildDMFileParentPathInNFS(oid), local_dmf->fileId(), DMFile::READABLE);
    LOG_DEBUG(log, "Upload DMFile, oid={} local={} remote={}", oid.info(), local_dmf->path(), dist_dmf_dir);

    // TODO: Make this process atomic? Or may be we should reject this case.
    auto dist_dmf_dir_poco = Poco::File(dist_dmf_dir);
    if (dist_dmf_dir_poco.exists())
    {
        LOG_WARNING(log, "DMFile already exists in the remote directory, overwriting, oid={} local={} remote={}", oid.info(), local_dmf->path(), dist_dmf_dir);
        dist_dmf_dir_poco.remove(true);
    }

    src_dmf_dir_poco.copyTo(dist_dmf_dir);
    LOG_DEBUG(log, "Upload DMFile finished, oid={}", oid.info());
}

IPreparedDMFileTokenPtr DataStoreNFS::prepareDMFile(const DMFileOID & oid)
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
        if (file == nullptr)
        {
            throw DB::Exception(fmt::format(
                "Cannot find DMFile in the NFS directory, oid={} remote_parent={}",
                oid.info(),
                remote_parent_path));
        }

        LOG_DEBUG(log, "DMFile download finished, oid={} rows={}", oid.info(), file->getRows());
    }

    return std::shared_ptr<PreparedDMFileToken>(token);
}

} // namespace DB::DM::Remote
