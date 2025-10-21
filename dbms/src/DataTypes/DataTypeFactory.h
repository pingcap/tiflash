// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/dbms/src/DataTypes/DataTypeFactory.h
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

#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>

#include <ext/singleton.h>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace DB
{
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

// When there are lots of tables created, the DataTypePtr could consume lots of
// memory (tiflash#9947), or lots of CPU time to parse the string into IAST and
// create the datatype (tiflash#6395). So we make a cache of FullName to
// DataTypePtr in order to reuse the same DataTypePtr.
class DataTypePtrCache
{
public:
    DataTypePtr get(const String & full_name) const;
    void tryCache(const String & full_name, const DataTypePtr & datatype_ptr);
    size_t getFullNameCacheSize() const
    {
        std::shared_lock lock(rw_lock);
        return cached_types.size();
    }

private:
    // full_name -> DataTypePtr
    using FullnameTypes = std::unordered_map<String, DataTypePtr>;
    static constexpr int MAX_FULLNAME_TYPES = 50000;
    static constexpr int FULLNAME_TYPES_HIGH_WATER_MARK = 49000;
    mutable std::shared_mutex rw_lock;
    FullnameTypes cached_types;
};

/** Creates a data type by name of data type family and parameters.
  */
class DataTypeFactory final : public ext::Singleton<DataTypeFactory>
{
private:
    using Creator = std::function<DataTypePtr(const ASTPtr & parameters)>;
    using SimpleCreator = std::function<DataTypePtr()>;
    // family_name -> Creator
    using DataTypesDictionary = std::unordered_map<String, Creator>;

public:
    DataTypePtr get(const String & full_name) const;
    // In order to optimize the speed of generating data type instances, this will cache the full_name -> DataTypePtr.
    DataTypePtr getOrSet(const String & full_name);
    DataTypePtr getOrSet(const ASTPtr & ast);
    DataTypePtr get(const String & family_name, const ASTPtr & parameters) const;
    DataTypePtr get(const ASTPtr & ast) const;
    size_t getFullNameCacheSize() const { return fullname_types.getFullNameCacheSize(); }

    /// For compatibility with SQL, it's possible to specify that certain data type name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    /// Register a type family by its name.
    void registerDataType(
        const String & family_name,
        Creator creator,
        CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a simple data type, that have no parameters.
    void registerSimpleDataType(
        const String & name,
        SimpleCreator creator,
        CaseSensitiveness case_sensitiveness = CaseSensitive);

private:
    DataTypesDictionary data_types;

    /// Case insensitive data types will be additionally added here with lowercased name.
    DataTypesDictionary case_insensitive_data_types;

    DataTypePtrCache fullname_types;

    DataTypeFactory();
    friend class ext::Singleton<DataTypeFactory>;
};

} // namespace DB
