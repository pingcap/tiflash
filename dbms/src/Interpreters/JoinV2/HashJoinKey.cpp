// Copyright 2024 PingCAP, Inc.
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

#include <Interpreters/JoinV2/HashJoinKey.h>

namespace DB
{

size_t getHashValueByteSize(HashJoinKeyMethod method)
{
    switch (method)
    {
    case HashJoinKeyMethod::Empty:
    case HashJoinKeyMethod::Cross:
        return 0;

#define M(METHOD)                                                                                   \
    case HashJoinKeyMethod::METHOD:                                                                 \
        return sizeof(typename HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>::HashValueType); \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

HashJoinKeyMethod findFixedSizeJoinKeyMethod(size_t keys_size, size_t all_key_fixed_size)
{
    if (keys_size == 1)
    {
        switch (all_key_fixed_size)
        {
        case 1:
            return HashJoinKeyMethod::OneKey8;
        case 2:
            return HashJoinKeyMethod::OneKey16;
        case 4:
            return HashJoinKeyMethod::OneKey32;
        case 8:
            return HashJoinKeyMethod::OneKey64;
        case 16:
            return HashJoinKeyMethod::OneKey128;
        default:
            break;
        }
    }
    if (all_key_fixed_size <= 4)
        return HashJoinKeyMethod::KeysFixed32;
    if (all_key_fixed_size <= 8)
        return HashJoinKeyMethod::KeysFixed64;
    if (all_key_fixed_size <= 16)
        return HashJoinKeyMethod::KeysFixed128;
    if (all_key_fixed_size <= 32)
        return HashJoinKeyMethod::KeysFixed256;
    return HashJoinKeyMethod::KeysFixedOther;
}

std::unique_ptr<void, std::function<void(void *)>> createHashJoinKeyGetter(
    HashJoinKeyMethod method,
    const TiDB::TiDBCollators & collators)
{
    switch (method)
    {
    case HashJoinKeyMethod::Empty:
    case HashJoinKeyMethod::Cross:
        return nullptr;

#define M(METHOD)                                                                                         \
    case HashJoinKeyMethod::METHOD:                                                                       \
        using KeyGetterType##METHOD = typename HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>::Type; \
        return std::unique_ptr<void, std::function<void(void *)>>(                                        \
            static_cast<void *>(new KeyGetterType##METHOD(collators)),                                    \
            [](void * ptr) { delete reinterpret_cast<KeyGetterType##METHOD *>(ptr); });                   \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

void resetHashJoinKeyGetter(
    HashJoinKeyMethod method,
    std::unique_ptr<void, std::function<void(void *)>> & key_getter,
    const ColumnRawPtrs & key_columns,
    const HashJoinRowLayout & row_layout)
{
    switch (method)
    {
    case HashJoinKeyMethod::Empty:
    case HashJoinKeyMethod::Cross:
        return;

#define M(METHOD)                                                                                     \
    case HashJoinKeyMethod::METHOD:                                                                   \
        using KeyGetter##METHOD = typename HashJoinKeyGetterForType<HashJoinKeyMethod::METHOD>::Type; \
        static_cast<KeyGetter##METHOD *>(key_getter.get())                                            \
            ->reset(key_columns, row_layout.raw_required_key_column_indexes.size());                  \
        break;
        APPLY_FOR_HASH_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

} // namespace DB
