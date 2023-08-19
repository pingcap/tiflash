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

#include <ext/singleton.h>
#include <functional>
#include <memory>
#include <unordered_map>


namespace DB
{
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/** Creates a data type by name of data type family and parameters.
  */
class DataTypeFactory final : public ext::Singleton<DataTypeFactory>
{
private:
    using Creator = std::function<DataTypePtr(const ASTPtr & parameters)>;
    using SimpleCreator = std::function<DataTypePtr()>;
    using DataTypesDictionary = std::unordered_map<String, Creator>;

public:
    DataTypePtr get(const String & full_name) const;
    DataTypePtr get(const String & family_name, const ASTPtr & parameters) const;
    DataTypePtr get(const ASTPtr & ast) const;

    /// For compatibility with SQL, it's possible to specify that certain data type name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    /// Register a type family by its name.
    void registerDataType(const String & family_name, Creator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a simple data type, that have no parameters.
    void registerSimpleDataType(const String & name, SimpleCreator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

private:
    DataTypesDictionary data_types;

    /// Case insensitive data types will be additionally added here with lowercased name.
    DataTypesDictionary case_insensitive_data_types;

    DataTypeFactory();
    friend class ext::Singleton<DataTypeFactory>;
};

} // namespace DB
