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

#include <utility>

#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>


namespace DB
{
class WriteBuffer;


/** Column data along with its data type and name.
  * Column data could be nullptr - to represent just 'header' of column.
  * Name could be either name from a table or some temporary generated name during expression evaluation.
  */

struct ColumnWithTypeAndName
{
    ColumnPtr column;
    DataTypePtr type;
    String name;

    /// TODO Handle column_id properly after we support DDL.
    Int64 column_id;
    Field default_value;

    ColumnWithTypeAndName()
        : ColumnWithTypeAndName(nullptr, nullptr, "")
    {}
    /// Uses type->createColumn() to create column
    ColumnWithTypeAndName(const DataTypePtr & type_, const String & name_)
        : ColumnWithTypeAndName(type_->createColumn(), type_, name_)
    {}

    ColumnWithTypeAndName(ColumnPtr column_, DataTypePtr type_)
        : ColumnWithTypeAndName(column_, type_, "")
    {}
    // If we use `Field default_value_` as the param and init default_value with `std::move(default_value_)`, it brings compile warnings.
    // Check PR#1383 for more details
    ColumnWithTypeAndName(
        ColumnPtr column_,
        DataTypePtr type_,
        String name_,
        Int64 column_id_ = 0,
        const Field & default_value_ = Field())
        : column(std::move(column_))
        , type(std::move(type_))
        , name(std::move(name_))
        , column_id(column_id_)
        , default_value(default_value_)
    {}

    ColumnWithTypeAndName cloneEmpty() const;
    bool operator==(const ColumnWithTypeAndName & other) const;

    void dumpStructure(WriteBuffer & out) const;
    String dumpStructure() const;

    void dumpJsonStructure(WriteBuffer & out) const;
    String dumpJsonStructure() const;
};

} // namespace DB
