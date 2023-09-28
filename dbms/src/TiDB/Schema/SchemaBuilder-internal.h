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

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Storages/KVStore/Types.h>

#include <map>
#include <set>
#include <utility>

/// === Some Private struct / method for SchemaBuilder
/// Notice that this file should only included by SchemaBuilder.cpp and unittest for this file.

namespace Poco
{
class Logger;
}
namespace TiDB
{
struct TableInfo;
}
namespace DB
{
constexpr char tmpNamePrefix[] = "_tiflash_tmp_";

struct TmpTableNameGenerator
{
    using TableName = std::pair<String, String>;
    TableName operator()(const TableName & name)
    {
        return std::make_pair(name.first, String(tmpNamePrefix) + name.second);
    }
};

struct TmpColNameGenerator
{
    String operator()(const String & name) { return String(tmpNamePrefix) + name; }
};


struct ColumnNameWithID
{
    String name;
    ColumnID id;

    explicit ColumnNameWithID(String name_ = "", ColumnID id_ = 0)
        : name(std::move(name_))
        , id(id_)
    {}

    bool equals(const ColumnNameWithID & rhs) const { return name == rhs.name && id == rhs.id; }

    // This is for only compare column name in CyclicRenameResolver
    bool operator==(const ColumnNameWithID & rhs) const { return name == rhs.name; }

    bool operator<(const ColumnNameWithID & rhs) const { return name < rhs.name; }

    String toString() const { return name + "(" + std::to_string(id) + ")"; }
};

struct TmpColNameWithIDGenerator
{
    ColumnNameWithID operator()(const ColumnNameWithID & name_with_id)
    {
        return ColumnNameWithID{String(tmpNamePrefix) + name_with_id.name, name_with_id.id};
    }
};


// CyclicRenameResolver resolves cyclic table rename and column rename.
// TmpNameGenerator rename current name to a temp name that will not conflict with other names.
template <typename Name_, typename TmpNameGenerator>
struct CyclicRenameResolver
{
    using Name = Name_;
    using NamePair = std::pair<Name, Name>;
    using NamePairs = std::vector<NamePair>;
    using NameSet = std::set<Name>;
    using NameMap = std::map<Name, Name>;

    // visited records which name has been processed.
    NameSet visited;
    TmpNameGenerator name_gen;

    // We will not ensure correctness if we call it multiple times, so we make it a rvalue call.
    NamePairs resolve(NameMap && rename_map) &&
    {
        NamePairs result;
        for (auto it = rename_map.begin(); it != rename_map.end(); /* */)
        {
            if (!visited.count(it->first))
            {
                resolveImpl(rename_map, it, result);
            }
            // remove dependency of `it` since we have already done rename
            it = rename_map.erase(it);
        }
        return result;
    }

private:
    NamePair resolveImpl(NameMap & rename_map, typename NameMap::iterator & it, NamePairs & result)
    {
        Name origin_name = it->first;
        Name target_name = it->second;
        visited.insert(it->first);
        auto next_it = rename_map.find(target_name);
        if (next_it == rename_map.end())
        {
            // The target name does not exist, so we can rename it directly.
            result.push_back(NamePair(origin_name, target_name));
            return NamePair();
        }
        else if (auto visited_iter = visited.find(target_name); visited_iter != visited.end())
        {
            // The target name is visited, so this is a cyclic rename, generate a tmp name for visited column to break the cyclic.
            const Name & visited_name = *visited_iter;
            auto tmp_name = name_gen(visited_name);
            result.push_back(NamePair(visited_name, tmp_name));
            result.push_back(NamePair(origin_name, target_name));
            return NamePair(target_name, tmp_name);
        }
        else
        {
            // The target name is in rename map, so we continue to resolve it.
            auto pair = resolveImpl(rename_map, next_it, result);
            if (pair.first == origin_name)
            {
                origin_name = pair.second;
            }
            result.push_back(NamePair(origin_name, target_name));
            return pair;
        }
    }
};


} // namespace DB
