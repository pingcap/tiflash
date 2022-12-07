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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/ColumnsHashing.h>
#include <Common/FmtUtils.h>
#include <Common/typeid_cast.h>
#include <DataStreams/HashOrderBlockInputStream.h>
#include <Interpreters/sortBlock.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
} // namespace ErrorCodes


ColumnRawPtrs getKeyColumns(SortDescription descr, const Block & block)
{
    size_t keys_size = descr.size();
    ColumnRawPtrs key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns[i] = block.getByName(descr[i].column_name).column.get();

        if (key_columns[i]->isColumnNullable())
            key_columns[i] = &static_cast<const ColumnNullable &>(*key_columns[i]).getNestedColumn();
    }

    return key_columns;
}

HashOrderBlockInputStream::HashOrderBlockInputStream(
    const BlockInputStreamPtr & input_,
    SortDescription & description_,
    const String & req_id,
    const Context & context_,
    const Block & sample_block,
    size_t limit_)
    : description(description_)
    , limit(limit_)
    , log(Logger::get(NAME, req_id))
    , context(context_)
{
    children.push_back(input_);

    type = chooseMethod(getKeyColumns(description, sample_block), key_sizes);
}

void HashOrderBlockInputStream::initMap(size_t capacity = 0)
{
    switch (type)
    {
#define M(TYPE)                                                                     \
    case Type::TYPE:                                                                \
        maps.TYPE = std::make_unique<typename decltype(maps.TYPE)::element_type>(); \
        maps.TYPE->reserve(capacity);                                               \
        break;
        APPLY_FOR_HASH_ORDER_VARIANTS(M)
#undef M
    }
}

void HashOrderBlockInputStream::initIter()
{
    switch (type)
    {
#define M(TYPE)                           \
    case Type::TYPE:                      \
        assert(maps.TYPE);                \
        iters.TYPE = maps.TYPE->cbegin(); \
        break;
        APPLY_FOR_HASH_ORDER_VARIANTS(M)
#undef M
    }
}

HashOrderBlockInputStream::Type HashOrderBlockInputStream::chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    size_t keys_size = key_columns.size();

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
            return Type::key8;
        if (size_of_field == 2)
            return Type::key16;
        if (size_of_field == 4)
            return Type::key32;
        if (size_of_field == 8)
            return Type::key64;
        if (size_of_field == 16)
            return Type::keys128;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16.", ErrorCodes::LOGICAL_ERROR);
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= 32)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1
        && (typeid_cast<const ColumnString *>(key_columns[0])
            || (key_columns[0]->isColumnConst() && typeid_cast<const ColumnString *>(&static_cast<const ColumnConst *>(key_columns[0])->getDataColumn()))))
        return Type::key_string;

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, use serialized values as the key.
    return Type::serialized;
}

template <HashOrderBlockInputStream::Type type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashOrderBlockInputStream::Type::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, true>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashOrderBlockInputStream::Type::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, true>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashOrderBlockInputStream::Type::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, true>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashOrderBlockInputStream::Type::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, true>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashOrderBlockInputStream::Type::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashOrderBlockInputStream::Type::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashOrderBlockInputStream::Type::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, true, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashOrderBlockInputStream::Type::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, true, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<HashOrderBlockInputStream::Type::serialized, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodSerialized<Value, Mapped>;
};

template <HashOrderBlockInputStream::Type type, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
};


template <typename Map, typename KeyGetter>
void HashOrderBlockInputStream::insert(Map & map, size_t rows, KeyGetter key_getter, std::vector<std::string> & sort_key_container, Block * block)
{
    for (size_t i = 0; i < rows; ++i)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, *pool, sort_key_container);

        if (emplace_result.isInserted())
            new (&emplace_result.getMapped()) typename Map::mapped_type(block, i);
        else
        {
            /** The first element of the list is stored in the value of the hash table, the rest in the pool.
                 * We will insert each time the element into the second place.
                 * That is, the former second element, if it was, will be the third, and so on.
                 */
            auto * elem = reinterpret_cast<RowRefList *>(pool->alloc(sizeof(RowRefList)));
            insertRowToList(&emplace_result.getMapped(), elem, block, i);
        }
    }
}

template <typename Map, typename MapIterator>
Block HashOrderBlockInputStream::output(Map & map, MapIterator & iter)
{
    if (iter == map->cend())
    {
        return Block();
    }

    auto block_size_limit = context.getSettingsRef().max_block_size.get();

    auto columns = blocks.front().cloneEmptyColumns();
    while (iter != map->cend() && columns[0]->size() < block_size_limit)
    {
        for (const RowRefList * curr = &iter->getMapped(); curr != nullptr; curr = curr->next)
            for (size_t j = 0; j < columns.size(); j++)
                columns[j]->insertFrom(*curr->block->getByPosition(j).column.get(), curr->row_num);
        ++iter;
    }

    return blocks.front().cloneWithColumns(std::move(columns));
}

Block HashOrderBlockInputStream::readImpl()
{
    return readImplInternal();
}

void HashOrderBlockInputStream::insertFromBlock(Block * block)
{
    size_t rows = block->rows();
    switch (type)
    {
#define M(TYPE)                                                                                                       \
    case Type::TYPE:                                                                                                  \
    {                                                                                                                 \
        auto key_columns = getKeyColumns(description, *block);                                                        \
        std::vector<std::string> sort_key_container;                                                                  \
        sort_key_container.resize(key_columns.size());                                                                \
        using KeyGetter = typename KeyGetterForType<Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type; \
        insert(*maps.TYPE, rows, KeyGetter(key_columns, key_sizes, collators), sort_key_container, block);            \
        iters.TYPE = maps.TYPE->cbegin();                                                                             \
        break;                                                                                                        \
    }
        APPLY_FOR_HASH_ORDER_VARIANTS(M)
#undef M
    }
}

size_t getEstimateRows(BlocksList & blocks)
{
    // TODO: use sample or other methods.
    size_t rows = 0;
    for (auto & block : blocks)
    {
        rows += block.rows();
    }
    return rows;
}


Block HashOrderBlockInputStream::readImplInternal()
{
    call_count += 1;
    LOG_FMT_WARNING(log, "call count = {}", call_count);

    if (!executed)
    {
        executed = true;

        auto block_size_limit = context.getSettingsRef().debug_hash_sort_window_reserve_size.get();
        initMap(block_size_limit);

        while (Block block = children.back()->read())
        {
            blocks.push_back(block);
            insertFromBlock(&blocks.back());
        }
        initIter();
    }

    if (blocks.empty())
    {
        return Block();
    }
    switch (type)
    {
#define M(TYPE)      \
    case Type::TYPE: \
        return output(maps.TYPE, iters.TYPE);
        APPLY_FOR_HASH_ORDER_VARIANTS(M)
#undef M
    }
}

void HashOrderBlockInputStream::appendInfo(FmtBuffer & buffer) const
{
    buffer.fmtAppend(": limit = {}", limit);
}
} // namespace DB
