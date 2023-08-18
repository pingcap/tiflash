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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Core/ColumnWithTypeAndName.h>

namespace DB
{

/** Adds a materialized const column to the block with a specified value.
  */
template <typename T>
class AddingConstColumnBlockInputStream : public IProfilingBlockInputStream
{
public:
    AddingConstColumnBlockInputStream(
        BlockInputStreamPtr input_,
        DataTypePtr data_type_,
        T value_,
        String column_name_)
        : data_type(data_type_), value(value_), column_name(column_name_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "AddingConstColumn"; }

    Block getHeader() const override
    {
        Block res = children.back()->getHeader();
        res.insert({data_type->createColumn(), data_type, column_name});
        return res;
    }

protected:
    Block readImpl() override
    {
        Block res = children.back()->read();
        if (!res)
            return res;

        res.insert({data_type->createColumnConst(res.rows(), value)->convertToFullColumnIfConst(), data_type, column_name});
        return res;
    }

private:
    DataTypePtr data_type;
    T value;
    String column_name;
};

}
