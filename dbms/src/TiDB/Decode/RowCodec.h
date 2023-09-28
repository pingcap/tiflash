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
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/Decode/DecodingStorageSchemaSnapshot.h>

namespace DB
{
/// The following two encode functions are used for testing.
void encodeRowV1(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss);
void encodeRowV2(const TiDB::TableInfo & table_info, const std::vector<Field> & fields, WriteBuffer & ss);

bool appendRowToBlock(
    const TiKVValue::Base & raw_value,
    SortedColumnIDWithPosConstIter column_ids_iter,
    SortedColumnIDWithPosConstIter column_ids_iter_end,
    Block & block,
    size_t block_column_pos,
    const DecodingStorageSchemaSnapshotConstPtr & schema_snapshot,
    bool force_decode);


} // namespace DB
