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

#include <Columns/IColumn.h>
#include <common/types.h>

#include <vector>


namespace DB
{

class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;

struct DictionaryStructure;

/// Write keys to block output stream.

/// For simple key
void formatIDs(BlockOutputStreamPtr & out, const std::vector<UInt64> & ids);

/// For composite key
void formatKeys(
    const DictionaryStructure & dict_struct,
    BlockOutputStreamPtr & out,
    const Columns & key_columns,
    const std::vector<size_t> & requested_rows);

} // namespace DB
