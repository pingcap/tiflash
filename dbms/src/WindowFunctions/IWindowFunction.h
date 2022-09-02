// Copyright 2022 PingCAP, Ltd.
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

#include <Core/ColumnNumbers.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>

#include <memory>


namespace DB
{
class WindowBlockInputStream;
using WindowBlockInputStreamPtr = std::shared_ptr<WindowBlockInputStream>;

class IWindowFunction
{
public:
    explicit IWindowFunction(const DataTypes & argument_types_)
        : argument_types(argument_types_)
    {}

    virtual String getName() const = 0;

    virtual ~IWindowFunction() = default;

    virtual DataTypePtr getReturnType() const = 0;
    // Must insert the result for current_row.
    virtual void windowInsertResultInto(
        WindowBlockInputStreamPtr streamPtr,
        size_t function_index,
        const ColumnNumbers & arguments)
        = 0;

protected:
    DataTypes argument_types;
};

using WindowFunctionPtr = std::shared_ptr<IWindowFunction>;

} // namespace DB
