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

#include <DataStreams/FilterColumnsBlockInputStream.h>

namespace DB
{

Block FilterColumnsBlockInputStream::getHeader() const
{
    Block block = children.back()->getHeader();
    Block filtered;

    for (const auto & it : columns_to_save)
        if (throw_if_column_not_found || block.has(it))
            filtered.insert(std::move(block.getByName(it)));

    return filtered;
}

Block FilterColumnsBlockInputStream::readImpl()
{
    Block block = children.back()->read();

    if (!block)
        return block;

    Block filtered;

    for (const auto & it : columns_to_save)
        if (throw_if_column_not_found || block.has(it))
            filtered.insert(std::move(block.getByName(it)));

    return filtered;
}

}
