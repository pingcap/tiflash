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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/JoinHashMap.h>

namespace DB
{
namespace
{
bool canAsColumnString(const IColumn * column)
{
    return typeid_cast<const ColumnString *>(column)
        || (column->isColumnConst()
            && typeid_cast<const ColumnString *>(&static_cast<const ColumnConst *>(column)->getDataColumn()));
}
} // namespace

JoinMapMethod chooseJoinMapMethod(
    const ColumnRawPtrs & key_columns,
    Sizes & key_sizes,
    const TiDB::TiDBCollators & collators)
{
    const size_t keys_size = key_columns.size();

    if (keys_size == 0)
        return JoinMapMethod::CROSS;

    bool all_fixed = true;
    size_t keys_bytes = 0;
    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    /// If there is one numeric key that fits in 64 bits
    if (keys_size == 1 && key_columns[0]->isNumeric())
    {
        size_t size_of_field = key_columns[0]->sizeOfValueIfFixed();
        if (size_of_field == 1)
            return JoinMapMethod::key8;
        if (size_of_field == 2)
            return JoinMapMethod::key16;
        if (size_of_field == 4)
            return JoinMapMethod::key32;
        if (size_of_field == 8)
            return JoinMapMethod::key64;
        if (size_of_field == 16)
            return JoinMapMethod::keys128;
        throw Exception(
            "Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16.",
            ErrorCodes::LOGICAL_ERROR);
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return JoinMapMethod::keys128;
    if (all_fixed && keys_bytes <= 32)
        return JoinMapMethod::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1 && canAsColumnString(key_columns[0]))
    {
        if (collators.empty() || !collators[0])
            return JoinMapMethod::key_strbin;
        else
        {
            switch (collators[0]->getCollatorType())
            {
            case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
            case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
            case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
            case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
            {
                return JoinMapMethod::key_strbinpadding;
            }
            case TiDB::ITiDBCollator::CollatorType::BINARY:
            {
                return JoinMapMethod::key_strbin;
            }
            default:
            {
                // for CI COLLATION, use original way
                return JoinMapMethod::key_string;
            }
            }
        }
    }

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return JoinMapMethod::key_fixed_string;

    /// Otherwise, use serialized values as the key.
    return JoinMapMethod::serialized;
}
} // namespace DB
