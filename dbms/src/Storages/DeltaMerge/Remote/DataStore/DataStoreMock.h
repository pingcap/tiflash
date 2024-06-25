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

#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>

namespace DB::DM::Remote
{
class DataStoreMock final : public IDataStore
{
public:
    explicit DataStoreMock(FileProviderPtr file_provider_)
        : file_provider(file_provider_)
    {}

    ~DataStoreMock() override = default;

    void putDMFile(DMFilePtr, const S3::DMFileOID &, bool) override
    {
        throw Exception("DataStoreMock::putDMFile unsupported");
    }

    IPreparedDMFileTokenPtr prepareDMFile(const S3::DMFileOID &, UInt64) override
    {
        throw Exception("DataStoreMock::prepareDMFile unsupported");
    }

    IPreparedDMFileTokenPtr prepareDMFileByKey(const String & remote_key) override;

    bool putCheckpointFiles(const PS::V3::LocalCheckpointFiles &, StoreID, UInt64) override
    {
        throw Exception("DataStoreMock::putCheckpointFiles unsupported");
    }

    std::unordered_map<String, DataFileInfo> getDataFilesInfo(const std::unordered_set<String> &) override
    {
        throw Exception("DataStoreMock::getDataFilesInfo unsupported");
    }

    void setTaggingsForKeys(const std::vector<String> &, std::string_view) override
    {
        throw Exception("DataStoreMock::setTaggingsForKeys unsupported");
    }

private:
    FileProviderPtr file_provider;
};

class MockPreparedDMFileToken : public IPreparedDMFileToken
{
public:
    MockPreparedDMFileToken(const FileProviderPtr & file_provider_, const String & path_)
        : IPreparedDMFileToken::IPreparedDMFileToken(file_provider_, {}, 0)
        , path(path_)
    {}

    ~MockPreparedDMFileToken() override = default;

    DMFilePtr restore(DMFileMeta::ReadMode read_mode) override;

private:
    String path;
};

} // namespace DB::DM::Remote
