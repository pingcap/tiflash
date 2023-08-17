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
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/SetVariants.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <typename Variant>
void SetVariantsTemplate<Variant>::init(Type type_)
{
    type = type_;

    switch (type)
    {
    case Type::EMPTY:
        break;

#define M(NAME)                                                             \
    case Type::NAME:                                                        \
        (NAME) = std::make_unique<typename decltype(NAME)::element_type>(); \
        break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown Set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Variant>
size_t SetVariantsTemplate<Variant>::getTotalRowCount() const
{
    switch (type)
    {
    case Type::EMPTY:
        return 0;

#define M(NAME)      \
    case Type::NAME: \
        return (NAME)->data.size();
        APPLY_FOR_SET_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown Set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Variant>
size_t SetVariantsTemplate<Variant>::getTotalByteCount() const
{
    switch (type)
    {
    case Type::EMPTY:
        return 0;

#define M(NAME)      \
    case Type::NAME: \
        return (NAME)->data.getBufferSizeInBytes();
        APPLY_FOR_SET_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown Set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Variant>
typename SetVariantsTemplate<Variant>::Type SetVariantsTemplate<Variant>::chooseMethod(
    const ColumnRawPtrs & key_columns,
    Sizes & key_sizes,
    const TiDB::TiDBCollators & collators)
{
    /// Check if at least one of the specified keys is nullable.
    /// Create a set of nested key columns from the corresponding key columns.
    /// Here "nested" means that, if a key column is nullable, we take its nested
    /// column; otherwise we take the key column as is.
    ColumnRawPtrs nested_key_columns;
    nested_key_columns.reserve(key_columns.size());
    bool has_nullable_key = false;

    for (const auto & col : key_columns)
    {
        if (col->isColumnNullable())
        {
            const auto & nullable_col = static_cast<const ColumnNullable &>(*col);
            nested_key_columns.push_back(&nullable_col.getNestedColumn());
            has_nullable_key = true;
        }
        else
            nested_key_columns.push_back(col);
    }

    size_t keys_size = nested_key_columns.size();

    bool all_fixed = true;
    size_t keys_bytes = 0;
    bool has_dec = false;
    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!nested_key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = nested_key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    if (has_nullable_key)
    {
        /// At least one key is nullable. Therefore we choose a method
        /// that takes into account this fact.
        if ((keys_size == 1) && (nested_key_columns[0]->isNumeric()) && !has_dec)
        {
            /// We have exactly one key and it is nullable. We shall add it a tag
            /// which specifies whether its value is null or not.
            size_t size_of_field = nested_key_columns[0]->sizeOfValueIfFixed();
            if ((size_of_field == 1) || (size_of_field == 2) || (size_of_field == 4) || (size_of_field == 8))
                return Type::nullable_keys128;
            else
                throw Exception{
                    "Logical error: numeric column has sizeOfField not in 1, 2, 4, 8.",
                    ErrorCodes::LOGICAL_ERROR};
        }

        if (all_fixed && !has_dec)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (keys_bytes > (std::numeric_limits<size_t>::max() - std::tuple_size<KeysNullMap<UInt128>>::value))
                throw Exception{"Aggregator: keys sizes overflow", ErrorCodes::LOGICAL_ERROR};
            if ((std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes) <= 16)
                return Type::nullable_keys128;
            if ((std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes) <= 32)
                return Type::nullable_keys256;
        }

        /// Fallback case.
        return Type::hashed;
    }

    /// If there is one numeric key that fits into 64 bits
    if (keys_size == 1 && nested_key_columns[0]->isNumeric())
    {
        size_t size_of_field = nested_key_columns[0]->sizeOfValueIfFixed();
        if (size_of_field == 1)
            return Type::key8;
        if (size_of_field == 2)
            return Type::key16;
        if (size_of_field == 4)
            return Type::key32;
        if (size_of_field == 8)
            return Type::key64;
        if (size_of_field == 16)
            return Type::keys128;
        //        if (size_of_field == sizeof(Decimal))
        //            return Type::key_decimal;
        throw Exception(
            "Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16.",
            ErrorCodes::LOGICAL_ERROR);
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= 32)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1
        && (typeid_cast<const ColumnString *>(nested_key_columns[0])
            || (nested_key_columns[0]->isColumnConst()
                && typeid_cast<const ColumnString *>(
                    &static_cast<const ColumnConst *>(nested_key_columns[0])->getDataColumn()))))
    {
        if (collators.empty() || !collators[0])
            return Type::key_strbin;
        else
        {
            switch (collators[0]->getCollatorType())
            {
            case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
            case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
            case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
            case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
            {
                return Type::key_strbinpadding;
            }
            case TiDB::ITiDBCollator::CollatorType::BINARY:
            {
                return Type::key_strbin;
            }
            default:
            {
                // for CI COLLATION, use original way
                return Type::key_string;
            }
            }
        }
    }

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(nested_key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return Type::hashed;
}

template struct SetVariantsTemplate<NonClearableSet>;
template struct SetVariantsTemplate<ClearableSet>;

} // namespace DB
