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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"
#ifdef __clang__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <pingcap/kv/Snapshot.h>
#pragma GCC diagnostic pop

#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Types.h>
#include <pingcap/kv/Scanner.h>

namespace DB
{
struct KeyspaceScanner : public pingcap::kv::Scanner
{
    using Base = pingcap::kv::Scanner;

    KeyspaceScanner(Base scanner_, bool need_cut_)
        : Base(scanner_)
        , need_cut(need_cut_)
    {}

    std::string key();

private:
    bool need_cut;
};

class KeyspaceSnapshot
{
public:
    using Base = pingcap::kv::Snapshot;
    explicit KeyspaceSnapshot(KeyspaceID keyspace_id_, pingcap::kv::Cluster * cluster_, UInt64 version_);

    std::string Get(const std::string & key);
    std::string Get(pingcap::kv::Backoffer & bo, const std::string & key);

    kvrpcpb::MvccInfo mvccGet(const std::string & key);
    KeyspaceScanner Scan(const std::string & begin, const std::string & end);

private:
    Base snap;
    std::string prefix;
    std::string encodeKey(const std::string & key);
};
} // namespace DB
