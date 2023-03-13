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

#include <Encryption/FileProvider.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/Transaction/Types.h>

namespace DB::DM::Remote
{
class DataStoreS3 final : public IDataStore
{
public:
    explicit DataStoreS3(FileProviderPtr file_provider_)
        : file_provider(file_provider_)
        , log(Logger::get("DataStoreS3"))
    {}

    ~DataStoreS3() override = default;

    /**
     * Blocks until a local DMFile is successfully put in the remote data store.
     * Should be used by a write node.
     */
    void putDMFile(DMFilePtr local_dmfile, const S3::DMFileOID & oid, bool remove_local) override;

    void copyDMFileMetaToLocalPath(const S3::DMFileOID & remote_oid, const String & local_dir) override;

    /**
     * Blocks until a DMFile in the remote data store is successfully prepared in a local cache.
     * If the DMFile exists in the local cache, it will not be prepared again.
     *
     * Returns a "token", which can be used to rebuild the `DMFile` object.
     * The DMFile in the local cache may be invalidated if you deconstructs the token.
     *
     * Should be used by a read node.
     */
    IPreparedDMFileTokenPtr prepareDMFile(const S3::DMFileOID & oid) override;

    bool putCheckpointFiles(const PS::V3::LocalCheckpointFiles & local_files, StoreID store_id, UInt64 upload_seq) override;

#ifndef DBMS_PUBLIC_GTEST
private:
#else
public:
#endif

    static void copyToLocal(const S3::DMFileOID & remote_oid, const std::vector<String> & target_short_fnames, const String & local_dir);

    FileProviderPtr file_provider;
    const LoggerPtr log;
};

class S3PreparedDMFileToken : public IPreparedDMFileToken
{
public:
    S3PreparedDMFileToken(const FileProviderPtr & file_provider_, const S3::DMFileOID & oid_)
        : IPreparedDMFileToken::IPreparedDMFileToken(file_provider_, oid_)
    {}

    ~S3PreparedDMFileToken() override = default;

    DMFilePtr restore(DMFile::ReadMetaMode read_mode) override;
};

} // namespace DB::DM::Remote
