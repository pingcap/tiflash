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
#include <iostream>

namespace DB
{

/// Removes columns other than columns_to_save_ from block,
///  and reorders columns as in columns_to_save_.
/// Functionality is similar to ExpressionBlockInputStream with ExpressionActions containing PROJECT action.
class FilterColumnsBlockInputStream : public IProfilingBlockInputStream
{
public:
    FilterColumnsBlockInputStream(
        const BlockInputStreamPtr & input, const Names & columns_to_save_, bool throw_if_column_not_found_)
        : columns_to_save(columns_to_save_), throw_if_column_not_found(throw_if_column_not_found_)
    {
        children.push_back(input);
    }

    String getName() const override
    {
        return "FilterColumns";
    }

    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    Names columns_to_save;
    bool throw_if_column_not_found;
};

}
