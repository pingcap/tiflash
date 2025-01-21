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

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/Decimal.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalComparison.h>
#include <Core/Field.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <Interpreters/WindowDescription.h>
#include <WindowFunctions/WindowUtils.h>
#include <common/UInt128.h>
#include <common/types.h>
#include <tipb/executor.pb.h>

#include <magic_enum.hpp>

namespace DB
{
WindowBlockInputStream::WindowBlockInputStream(
    const BlockInputStreamPtr & input,
    const WindowDescription & window_description_,
    const String & req_id)
    : action(input->getHeader(), window_description_, req_id)
{
    children.push_back(input);
}

bool WindowBlockInputStream::returnIfCancelledOrKilled()
{
    if (isCancelledOrThrowIfKilled())
    {
        action.cleanUp();
        return true;
    }
    return false;
}

Block WindowBlockInputStream::readImpl()
{
    // first try to get result without reading from children
    auto block = action.tryGetOutputBlock();
    if (block)
        return block;

    // then if input is not finished, keep reading input until one result block is generated
    if (!action.input_is_finished)
    {
        const auto & stream = children.back();
        while (!action.input_is_finished)
        {
            if (returnIfCancelledOrKilled())
                return {};

            Block block = stream->read();
            if (!block)
                action.input_is_finished = true;
            else
                action.appendBlock(block);
            if (auto block = action.tryGetOutputBlock())
                return block;
        }
    }
    return {};
}

void WindowBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    action.appendInfo(buffer);
}
} // namespace DB
