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

#include <Storages/DeltaMerge/Remote/DataStore/DataStoreMock.h>


namespace DB::DM::Remote
{

IPreparedDMFileTokenPtr DataStoreMock::prepareDMFileByKey(const String & remote_key)
{
    return std::make_shared<MockPreparedDMFileToken>(file_provider, remote_key);
}

static std::tuple<String, UInt64> parseDMFilePath(const String & path)
{
    // Path likes /disk1/data/t_100/stable/dmf_2.
    auto pos = path.find_last_of('_');
    RUNTIME_CHECK(pos != std::string::npos, path);
    auto file_id = stoul(path.substr(pos + 1));

    pos = path.rfind("/dmf_");
    RUNTIME_CHECK(pos != std::string::npos, path);
    auto parent_path = path.substr(0, pos);
    return std::tuple<String, UInt64>{parent_path, file_id};
}

DMFilePtr MockPreparedDMFileToken::restore(DMFileMeta::ReadMode read_mode, UInt64 meta_version)
{
    auto [parent_path, file_id] = parseDMFilePath(path);
    return DMFile::restore(
        file_provider,
        file_id,
        /*page_id*/ 0,
        parent_path,
        read_mode,
        meta_version);
}
} // namespace DB::DM::Remote
