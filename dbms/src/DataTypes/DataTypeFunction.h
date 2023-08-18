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

#include <DataTypes/IDataTypeDummy.h>


namespace DB
{
/** Special data type, representing lambda expression.
  */
class DataTypeFunction final : public IDataTypeDummy
{
private:
    DataTypes argument_types;
    DataTypePtr return_type;

public:
    static constexpr bool is_parametric = true;
    bool isParametric() const override { return true; }

    /// Some types could be still unknown.
    DataTypeFunction(const DataTypes & argument_types_ = DataTypes(), const DataTypePtr & return_type_ = nullptr)
        : argument_types(argument_types_)
        , return_type(return_type_)
    {}

    std::string getName() const override;
    const char * getFamilyName() const override { return "Function"; }
    TypeIndex getTypeId() const override { return TypeIndex::Function; }

    const DataTypes & getArgumentTypes() const { return argument_types; }

    const DataTypePtr & getReturnType() const { return return_type; }

    bool equals(const IDataType & rhs) const override;
};

} // namespace DB
