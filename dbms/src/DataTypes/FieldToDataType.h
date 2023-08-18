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

#include <Common/FieldVisitors.h>
#include <DataTypes/DataTypeDecimal.h>

namespace DB
{
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;


/** For a given Field returns the minimum data type that allows this value to be stored.
  * Note that you still have to convert Field to corresponding data type before inserting to columns
  *  (for example, this is necessary to convert elements of Array to common type).
  */
class FieldToDataType : public StaticVisitor<DataTypePtr>
{
public:
    DataTypePtr operator()(const Null & x) const;
    DataTypePtr operator()(const UInt64 & x) const;
    DataTypePtr operator()(const Int64 & x) const;
    DataTypePtr operator()(const Float64 & x) const;
    DataTypePtr operator()(const String & x) const;
    DataTypePtr operator()(const Array & x) const;
    DataTypePtr operator()(const Tuple & x) const;
    template <typename T>
    DataTypePtr operator()(const DecimalField<T> & x) const
    {
        return std::make_shared<DataTypeDecimal<T>>(x.getPrecWithCurrentDecimalType(), x.getScale());
    }
};

} // namespace DB
