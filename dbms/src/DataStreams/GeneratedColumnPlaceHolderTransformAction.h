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

#include <Core/Block.h>

namespace DB
{
class GeneratedColumnPlaceHolderTransformAction
{
public:
    GeneratedColumnPlaceHolderTransformAction(
        const Block & header_,
        const std::vector<std::tuple<UInt64, String, DataTypePtr>> & generated_column_infos_);

    void transform(Block & block);

    Block getHeader() const;

    void checkColumn() const;

private:
    void insertColumns(Block & block, bool insert_data) const;

private:
    Block header;
    const std::vector<std::tuple<UInt64, String, DataTypePtr>> generated_column_infos;
};
} // namespace DB
