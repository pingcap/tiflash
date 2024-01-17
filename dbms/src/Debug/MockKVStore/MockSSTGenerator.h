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

#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/Types.h>
#include <common/types.h>
#include <kvproto/raft_serverpb.pb.h>
#include <raft_cmdpb.pb.h>

namespace DB
{
struct MockSSTGenerator
{
    static std::string encodeSSTView(SSTFormatKind kind, std::string ori)
    {
        if (kind == SSTFormatKind::KIND_TABLET)
        {
            return "!" + ori;
        }
        return ori;
    }

    static SSTFormatKind parseSSTViewKind(std::string_view v)
    {
        if (v[0] == '!')
        {
            return SSTFormatKind::KIND_TABLET;
        }
        return SSTFormatKind::KIND_SST;
    }

    MockSSTGenerator(UInt64 region_id_, TableID table_id_, ColumnFamilyType type_);

    // Actual data will be stored in MockSSTReader.
    void finish_file(SSTFormatKind kind = SSTFormatKind::KIND_SST);
    void freeze() { freezed = true; }

    void insert(HandleID key, std::string val);
    void insert_raw(std::string key, std::string val);

    ColumnFamilyType cf_type() const { return type; }

    // Only use this after all sst_files is generated.
    // vector::push_back can cause destruction of std::string,
    // which is referenced by SSTView.
    std::vector<SSTView> ssts() const;

protected:
    UInt64 region_id;
    TableID table_id;
    ColumnFamilyType type;
    std::vector<std::string> sst_files;
    std::vector<std::pair<std::string, std::string>> kvs;
    int c;
    bool freezed;
};
} // namespace DB
