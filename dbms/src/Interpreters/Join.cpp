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
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/ColumnsHashing.h>
#include <Common/FailPoint.h>
#include <Common/typeid_cast.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Join.h>
#include <Interpreters/NullableUtils.h>
#include <common/logger_useful.h>


namespace DB
{
namespace FailPoints
{
extern const char random_join_build_failpoint[];
extern const char random_join_prob_failpoint[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
extern const int LOGICAL_ERROR;
extern const int SET_SIZE_LIMIT_EXCEEDED;
extern const int TYPE_MISMATCH;
extern const int ILLEGAL_COLUMN;
} // namespace ErrorCodes

namespace
{
/// Do I need to use the hash table maps_*_full, in which we remember whether the row was joined.
bool getFullness(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Right || kind == ASTTableJoin::Kind::Cross_Right || kind == ASTTableJoin::Kind::Full;
}
bool isLeftJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Cross_Left;
}
bool isRightJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Right || kind == ASTTableJoin::Kind::Cross_Right;
}
bool isInnerJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Cross;
}
bool isAntiJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Anti || kind == ASTTableJoin::Kind::Cross_Anti;
}
bool isCrossJoin(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Cross || kind == ASTTableJoin::Kind::Cross_Left
        || kind == ASTTableJoin::Kind::Cross_Right || kind == ASTTableJoin::Kind::Cross_Anti
        || kind == ASTTableJoin::Kind::Cross_LeftSemi || kind == ASTTableJoin::Kind::Cross_LeftAnti;
}
/// (cartesian) (anti) left semi join.
bool isLeftSemiFamily(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::LeftSemi || kind == ASTTableJoin::Kind::LeftAnti
        || kind == ASTTableJoin::Kind::Cross_LeftSemi || kind == ASTTableJoin::Kind::Cross_LeftAnti;
}

void convertColumnToNullable(ColumnWithTypeAndName & column)
{
    column.type = makeNullable(column.type);
    if (column.column)
        column.column = makeNullable(column.column);
}

ColumnRawPtrs getKeyColumns(const Names & key_names, const Block & block)
{
    size_t keys_size = key_names.size();
    ColumnRawPtrs key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns[i] = block.getByName(key_names[i]).column.get();

        /// We will join only keys, where all components are not NULL.
        if (key_columns[i]->isColumnNullable())
            key_columns[i] = &static_cast<const ColumnNullable &>(*key_columns[i]).getNestedColumn();
    }

    return key_columns;
}
} // namespace

const std::string Join::match_helper_prefix = "__left-semi-join-match-helper";
const DataTypePtr Join::match_helper_type = makeNullable(std::make_shared<DataTypeInt8>());


Join::Join(
    const Names & key_names_left_,
    const Names & key_names_right_,
    bool use_nulls_,
    ASTTableJoin::Kind kind_,
    ASTTableJoin::Strictness strictness_,
    const String & req_id,
    bool enable_fine_grained_shuffle_,
    size_t fine_grained_shuffle_count_,
    const TiDB::TiDBCollators & collators_,
    const String & left_filter_column_,
    const String & right_filter_column_,
    const String & other_filter_column_,
    const String & other_eq_filter_from_in_column_,
    ExpressionActionsPtr other_condition_ptr_,
    size_t max_block_size_,
    const String & match_helper_name)
    : match_helper_name(match_helper_name)
    , kind(kind_)
    , strictness(strictness_)
    , key_names_left(key_names_left_)
    , key_names_right(key_names_right_)
    , use_nulls(use_nulls_)
    , build_concurrency(0)
    , collators(collators_)
    , left_filter_column(left_filter_column_)
    , right_filter_column(right_filter_column_)
    , other_filter_column(other_filter_column_)
    , other_eq_filter_from_in_column(other_eq_filter_from_in_column_)
    , other_condition_ptr(other_condition_ptr_)
    , original_strictness(strictness)
    , max_block_size_for_cross_join(max_block_size_)
    , build_table_state(BuildTableState::SUCCEED)
    , log(Logger::get(req_id))
    , enable_fine_grained_shuffle(enable_fine_grained_shuffle_)
    , fine_grained_shuffle_count(fine_grained_shuffle_count_)
{
    if (other_condition_ptr != nullptr)
    {
        /// if there is other_condition, then should keep all the valid rows during probe stage
        if (strictness == ASTTableJoin::Strictness::Any)
        {
            strictness = ASTTableJoin::Strictness::All;
        }
    }
    if (unlikely(!left_filter_column.empty() && !isLeftJoin(kind)))
        throw Exception("Not supported: non left join with left conditions");
    if (unlikely(!right_filter_column.empty() && !isRightJoin(kind)))
        throw Exception("Not supported: non right join with right conditions");
    LOG_DEBUG(log, "FineGrainedShuffle flag {}, stream count {}", enable_fine_grained_shuffle, fine_grained_shuffle_count);
}

void Join::setBuildTableState(BuildTableState state_)
{
    std::lock_guard lk(build_table_mutex);
    build_table_state = state_;
    build_table_cv.notify_all();
}

bool CanAsColumnString(const IColumn * column)
{
    return typeid_cast<const ColumnString *>(column)
        || (column->isColumnConst() && typeid_cast<const ColumnString *>(&static_cast<const ColumnConst *>(column)->getDataColumn()));
}

Join::Type Join::chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes) const
{
    const size_t keys_size = key_columns.size();

    if (keys_size == 0)
        return Type::CROSS;

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
    if (keys_size == 1 && CanAsColumnString(key_columns[0]))
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

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, use serialized values as the key.
    return Type::serialized;
}


template <typename Maps>
static void initImpl(Maps & maps, Join::Type type, size_t build_concurrency)
{
    switch (type)
    {
    case Join::Type::EMPTY:
        break;
    case Join::Type::CROSS:
        break;

#define M(TYPE)                                                                                      \
    case Join::Type::TYPE:                                                                           \
        maps.TYPE = std::make_unique<typename decltype(maps.TYPE)::element_type>(build_concurrency); \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Maps>
static size_t getTotalRowCountImpl(const Maps & maps, Join::Type type)
{
    switch (type)
    {
    case Join::Type::EMPTY:
        return 0;
    case Join::Type::CROSS:
        return 0;

#define M(NAME)            \
    case Join::Type::NAME: \
        return maps.NAME ? maps.NAME->rowCount() : 0;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Maps>
static size_t getTotalByteCountImpl(const Maps & maps, Join::Type type)
{
    switch (type)
    {
    case Join::Type::EMPTY:
        return 0;
    case Join::Type::CROSS:
        return 0;

#define M(NAME)            \
    case Join::Type::NAME: \
        return maps.NAME ? maps.NAME->getBufferSizeInBytes() : 0;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}


template <Join::Type type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::key_strbinpadding, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodStringBin<Value, Mapped, true>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::key_strbin, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodStringBin<Value, Mapped, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<Join::Type::serialized, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodSerialized<Value, Mapped>;
};


template <Join::Type type, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
};

void Join::initMapImpl(Type type_)
{
    type = type_;

    if (isCrossJoin(kind))
        return;

    if (!getFullness(kind))
    {
        if (strictness == ASTTableJoin::Strictness::Any)
            initImpl(maps_any, type, getBuildConcurrencyInternal());
        else
            initImpl(maps_all, type, getBuildConcurrencyInternal());
    }
    else
    {
        if (strictness == ASTTableJoin::Strictness::Any)
            initImpl(maps_any_full, type, getBuildConcurrencyInternal());
        else
            initImpl(maps_all_full, type, getBuildConcurrencyInternal());
    }
}

size_t Join::getTotalRowCount() const
{
    size_t res = 0;

    if (type == Type::CROSS)
    {
        for (const auto & block : blocks)
            res += block.rows();
    }
    else
    {
        res += getTotalRowCountImpl(maps_any, type);
        res += getTotalRowCountImpl(maps_all, type);
        res += getTotalRowCountImpl(maps_any_full, type);
        res += getTotalRowCountImpl(maps_all_full, type);
    }

    return res;
}

size_t Join::getTotalByteCount() const
{
    size_t res = 0;

    if (type == Type::CROSS)
    {
        for (const auto & block : blocks)
            res += block.bytes();
    }
    else
    {
        res += getTotalByteCountImpl(maps_any, type);
        res += getTotalByteCountImpl(maps_all, type);
        res += getTotalByteCountImpl(maps_any_full, type);
        res += getTotalByteCountImpl(maps_all_full, type);
        for (const auto & pool : pools)
        {
            /// note the return value might not be accurate since it does not use lock, but should be enough for current usage
            res += pool->size();
        }
    }

    return res;
}

void Join::setBuildConcurrencyAndInitPool(size_t build_concurrency_)
{
    if (unlikely(build_concurrency > 0))
        throw Exception("Logical error: `setBuildConcurrencyAndInitPool` shouldn't be called more than once", ErrorCodes::LOGICAL_ERROR);
    build_concurrency = std::max(1, build_concurrency_);

    for (size_t i = 0; i < getBuildConcurrencyInternal(); ++i)
        pools.emplace_back(std::make_shared<Arena>());
    // init for non-joined-streams.
    if (getFullness(kind))
    {
        for (size_t i = 0; i < getNotJoinedStreamConcurrencyInternal(); ++i)
            rows_not_inserted_to_map.push_back(std::make_unique<RowRefList>());
    }
}

void Join::setSampleBlock(const Block & block)
{
    sample_block_with_columns_to_add = materializeBlock(block);

    /// Move from `sample_block_with_columns_to_add` key columns to `sample_block_with_keys`, keeping the order.
    size_t pos = 0;
    while (pos < sample_block_with_columns_to_add.columns())
    {
        const auto & name = sample_block_with_columns_to_add.getByPosition(pos).name;
        if (key_names_right.end() != std::find(key_names_right.begin(), key_names_right.end(), name))
        {
            sample_block_with_keys.insert(sample_block_with_columns_to_add.getByPosition(pos));
            sample_block_with_columns_to_add.erase(pos);
        }
        else
            ++pos;
    }

    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        auto & column = sample_block_with_columns_to_add.getByPosition(i);
        if (!column.column)
            column.column = column.type->createColumn();
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (use_nulls && (isLeftJoin(kind) || kind == ASTTableJoin::Kind::Full))
        for (size_t i = 0; i < num_columns_to_add; ++i)
            convertColumnToNullable(sample_block_with_columns_to_add.getByPosition(i));

    if (isLeftSemiFamily(kind))
        sample_block_with_columns_to_add.insert(ColumnWithTypeAndName(Join::match_helper_type, match_helper_name));
}

void Join::init(const Block & sample_block, size_t build_concurrency_)
{
    std::unique_lock lock(rwlock);
    if (unlikely(initialized))
        throw Exception("Logical error: Join has been initialized", ErrorCodes::LOGICAL_ERROR);
    initialized = true;
    setBuildConcurrencyAndInitPool(build_concurrency_);
    /// Choose data structure to use for JOIN.
    initMapImpl(chooseMethod(getKeyColumns(key_names_right, sample_block), key_sizes));
    setSampleBlock(sample_block);
}

namespace
{
void insertRowToList(Join::RowRefList * list, Join::RowRefList * elem, Block * stored_block, size_t index)
{
    elem->next = list->next; // NOLINT(clang-analyzer-core.NullDereference)
    list->next = elem;
    elem->block = stored_block;
    elem->row_num = index;
}

/// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
template <ASTTableJoin::Strictness STRICTNESS, typename Map, typename KeyGetter>
struct Inserter
{
    static void insert(Map & map, const typename Map::key_type & key, Block * stored_block, size_t i, Arena & pool, std::vector<String> & sort_key_containers);
};

template <typename Map, typename KeyGetter>
struct Inserter<ASTTableJoin::Strictness::Any, Map, KeyGetter>
{
    static void insert(Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool, std::vector<String> & sort_key_container)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool, sort_key_container);

        if (emplace_result.isInserted())
            new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
    }
};

template <typename Map, typename KeyGetter>
struct Inserter<ASTTableJoin::Strictness::All, Map, KeyGetter>
{
    using MappedType = typename Map::mapped_type;
    static void insert(Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool, std::vector<String> & sort_key_container)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool, sort_key_container);

        if (emplace_result.isInserted())
            new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
        else
        {
            /** The first element of the list is stored in the value of the hash table, the rest in the pool.
                 * We will insert each time the element into the second place.
                 * That is, the former second element, if it was, will be the third, and so on.
                 */
            auto elem = reinterpret_cast<MappedType *>(pool.alloc(sizeof(MappedType)));
            insertRowToList(&emplace_result.getMapped(), elem, stored_block, i);
        }
    }
};


template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
void NO_INLINE insertFromBlockImplTypeCase(
    Map & map,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    Block * stored_block,
    ConstNullMapPtr null_map,
    Join::RowRefList * rows_not_inserted_to_map,
    size_t stream_index,
    Arena & pool)
{
    KeyGetter key_getter(key_columns, key_sizes, collators);
    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(key_columns.size());

    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            if (rows_not_inserted_to_map)
            {
                /// for right/full out join, need to record the rows not inserted to map
                auto * elem = reinterpret_cast<Join::RowRefList *>(pool.alloc(sizeof(Join::RowRefList)));
                insertRowToList(rows_not_inserted_to_map, elem, stored_block, i);
            }
            continue;
        }

        size_t segment_index = stream_index;
        Inserter<STRICTNESS, typename Map::SegmentType::HashTable, KeyGetter>::insert(
            map.getSegmentTable(segment_index),
            key_getter,
            stored_block,
            i,
            pool,
            sort_key_containers);
    }
}

template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
void NO_INLINE insertFromBlockImplTypeCaseWithLock(
    Map & map,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    Block * stored_block,
    ConstNullMapPtr null_map,
    Join::RowRefList * rows_not_inserted_to_map,
    size_t stream_index,
    Arena & pool)
{
    KeyGetter key_getter(key_columns, key_sizes, collators);
    std::vector<std::string> sort_key_containers(key_columns.size());
    size_t segment_size = map.getSegmentSize();
    /// when inserting with lock, first calculate and save the segment index for each row, then
    /// insert the rows segment by segment to avoid too much conflict. This will introduce some overheads:
    /// 1. key_getter.getKey will be called twice, here we do not cache key because it can not be cached
    /// with relatively low cost(if key is stringRef, just cache a stringRef is meaningless, we need to cache the whole `sort_key_containers`)
    /// 2. hash value is calculated twice, maybe we can refine the code to cache the hash value
    /// 3. extra memory to store the segment index info
    std::vector<std::vector<size_t>> segment_index_info;
    if (has_null_map && rows_not_inserted_to_map)
    {
        segment_index_info.resize(segment_size + 1);
    }
    else
    {
        segment_index_info.resize(segment_size);
    }
    size_t rows_per_seg = rows / segment_index_info.size();
    for (auto & segment_index : segment_index_info)
    {
        segment_index.reserve(rows_per_seg);
    }
    for (size_t i = 0; i < rows; i++)
    {
        if (has_null_map && (*null_map)[i])
        {
            if (rows_not_inserted_to_map)
                segment_index_info[segment_index_info.size() - 1].push_back(i);
            continue;
        }
        auto key_holder = key_getter.getKeyHolder(i, &pool, sort_key_containers);
        auto key = keyHolderGetKey(key_holder);
        size_t segment_index = 0;
        size_t hash_value = 0;
        if (!ZeroTraits::check(key))
        {
            hash_value = map.hash(key);
            segment_index = hash_value % segment_size;
        }
        segment_index_info[segment_index].push_back(i);
        keyHolderDiscardKey(key_holder);
    }
    for (size_t insert_index = 0; insert_index < segment_index_info.size(); insert_index++)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_build_failpoint);
        size_t segment_index = (insert_index + stream_index) % segment_index_info.size();
        if (segment_index == segment_size)
        {
            /// null value
            /// here ignore mutex because rows_not_inserted_to_map is privately owned by each stream thread
            for (auto index : segment_index_info[segment_index])
            {
                /// for right/full out join, need to record the rows not inserted to map
                auto * elem = reinterpret_cast<Join::RowRefList *>(pool.alloc(sizeof(Join::RowRefList)));
                insertRowToList(rows_not_inserted_to_map, elem, stored_block, index);
            }
        }
        else
        {
            std::lock_guard lk(map.getSegmentMutex(segment_index));
            for (size_t i = 0; i < segment_index_info[segment_index].size(); i++)
            {
                Inserter<STRICTNESS, typename Map::SegmentType::HashTable, KeyGetter>::insert(map.getSegmentTable(segment_index), key_getter, stored_block, segment_index_info[segment_index][i], pool, sort_key_containers);
            }
        }
    }
}

template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
void insertFromBlockImplType(
    Map & map,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    Block * stored_block,
    ConstNullMapPtr null_map,
    Join::RowRefList * rows_not_inserted_to_map,
    size_t stream_index,
    size_t insert_concurrency,
    Arena & pool,
    bool enable_fine_grained_shuffle)
{
    if (null_map)
    {
        if (insert_concurrency > 1 && !enable_fine_grained_shuffle)
        {
            insertFromBlockImplTypeCaseWithLock<STRICTNESS, KeyGetter, Map, true>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
        }
        else
        {
            if (!enable_fine_grained_shuffle)
                RUNTIME_CHECK(stream_index == 0);
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
        }
    }
    else
    {
        if (insert_concurrency > 1 && !enable_fine_grained_shuffle)
        {
            insertFromBlockImplTypeCaseWithLock<STRICTNESS, KeyGetter, Map, false>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
        }
        else
        {
            if (!enable_fine_grained_shuffle)
                RUNTIME_CHECK(stream_index == 0);
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
        }
    }
}

template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
void insertFromBlockImpl(
    Join::Type type,
    Maps & maps,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    Block * stored_block,
    ConstNullMapPtr null_map,
    Join::RowRefList * rows_not_inserted_to_map,
    size_t stream_index,
    size_t insert_concurrency,
    Arena & pool,
    bool enable_fine_grained_shuffle)
{
    switch (type)
    {
    case Join::Type::EMPTY:
        break;
    case Join::Type::CROSS:
        break; /// Do nothing. We have already saved block, and it is enough.

#define M(TYPE)                                                                                                                                \
    case Join::Type::TYPE:                                                                                                                     \
        insertFromBlockImplType<STRICTNESS, typename KeyGetterForType<Join::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
            *maps.TYPE,                                                                                                                        \
            rows,                                                                                                                              \
            key_columns,                                                                                                                       \
            key_sizes,                                                                                                                         \
            collators,                                                                                                                         \
            stored_block,                                                                                                                      \
            null_map,                                                                                                                          \
            rows_not_inserted_to_map,                                                                                                          \
            stream_index,                                                                                                                      \
            insert_concurrency,                                                                                                                \
            pool,                                                                                                                              \
            enable_fine_grained_shuffle);                                                                                                      \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}
} // namespace

void recordFilteredRows(const Block & block, const String & filter_column, ColumnPtr & null_map_holder, ConstNullMapPtr & null_map)
{
    if (filter_column.empty())
        return;
    auto column = block.getByName(filter_column).column;
    if (column->isColumnConst())
        column = column->convertToFullColumnIfConst();
    if (column->isColumnNullable())
    {
        const auto & column_nullable = static_cast<const ColumnNullable &>(*column);
        if (!null_map_holder)
        {
            null_map_holder = column_nullable.getNullMapColumnPtr();
        }
        else
        {
            MutableColumnPtr mutable_null_map_holder = (*std::move(null_map_holder)).mutate();

            PaddedPODArray<UInt8> & mutable_null_map = static_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();
            const PaddedPODArray<UInt8> & other_null_map = column_nullable.getNullMapData();
            for (size_t i = 0, size = mutable_null_map.size(); i < size; ++i)
                mutable_null_map[i] |= other_null_map[i];

            null_map_holder = std::move(mutable_null_map_holder);
        }
    }

    if (!null_map_holder)
    {
        null_map_holder = ColumnVector<UInt8>::create(column->size(), 0);
    }
    MutableColumnPtr mutable_null_map_holder = (*std::move(null_map_holder)).mutate();
    PaddedPODArray<UInt8> & mutable_null_map = static_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();

    const auto & nested_column = column->isColumnNullable() ? static_cast<const ColumnNullable &>(*column).getNestedColumnPtr() : column;
    for (size_t i = 0, size = nested_column->size(); i < size; ++i)
        mutable_null_map[i] |= (!nested_column->getInt(i));

    null_map_holder = std::move(mutable_null_map_holder);

    null_map = &static_cast<const ColumnUInt8 &>(*null_map_holder).getData();
}

void Join::insertFromBlock(const Block & block)
{
    std::unique_lock lock(rwlock);
    if (unlikely(!initialized))
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);
    total_input_build_rows += block.rows();
    blocks.push_back(block);
    Block * stored_block = &blocks.back();
    insertFromBlockInternal(stored_block, 0);
}

/// the block should be valid.
void Join::insertFromBlock(const Block & block, size_t stream_index)
{
    std::shared_lock lock(rwlock);
    assert(stream_index < getBuildConcurrencyInternal());
    assert(stream_index < getNotJoinedStreamConcurrencyInternal());

    if (unlikely(!initialized))
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);
    Block * stored_block = nullptr;
    {
        std::lock_guard lk(blocks_lock);
        total_input_build_rows += block.rows();
        blocks.push_back(block);
        stored_block = &blocks.back();
        original_blocks.push_back(block);
    }
    insertFromBlockInternal(stored_block, stream_index);
}

void Join::insertFromBlockInternal(Block * stored_block, size_t stream_index)
{
    size_t keys_size = key_names_right.size();
    ColumnRawPtrs key_columns(keys_size);

    const Block & block = *stored_block;

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    Columns materialized_columns;

    /// Memoize key columns to work.
    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns[i] = block.getByName(key_names_right[i]).column.get();

        if (ColumnPtr converted = key_columns[i]->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns[i] = materialized_columns.back().get();
        }
    }

    /// We will insert to the map only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter will not insert to the maps
    recordFilteredRows(block, right_filter_column, null_map_holder, null_map);

    size_t rows = block.rows();

    if (getFullness(kind))
    {
        /** Move the key columns to the beginning of the block.
          * This is where NonJoinedBlockInputStream will expect.
          */
        size_t key_num = 0;
        for (const auto & name : key_names_right)
        {
            size_t pos = stored_block->getPositionByName(name);
            ColumnWithTypeAndName col = stored_block->safeGetByPosition(pos);
            stored_block->erase(pos);
            stored_block->insert(key_num, std::move(col));
            ++key_num;
        }
    }
    else
    {
        /// Remove the key columns from stored_block, as they are not needed.
        for (const auto & name : key_names_right)
            stored_block->erase(stored_block->getPositionByName(name));
    }

    size_t size = stored_block->columns();

    /// Rare case, when joined columns are constant. To avoid code bloat, simply materialize them.
    for (size_t i = 0; i < size; ++i)
    {
        ColumnPtr col = stored_block->safeGetByPosition(i).column;
        if (ColumnPtr converted = col->convertToFullColumnIfConst())
            stored_block->safeGetByPosition(i).column = converted;
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (use_nulls && (isLeftJoin(kind) || kind == ASTTableJoin::Kind::Full))
    {
        for (size_t i = getFullness(kind) ? keys_size : 0; i < size; ++i)
        {
            convertColumnToNullable(stored_block->getByPosition(i));
        }
    }

    if (!isCrossJoin(kind))
    {
        /// Fill the hash table.
        if (!getFullness(kind))
        {
            if (strictness == ASTTableJoin::Strictness::Any)
                insertFromBlockImpl<ASTTableJoin::Strictness::Any>(type, maps_any, rows, key_columns, key_sizes, collators, stored_block, null_map, nullptr, stream_index, getBuildConcurrencyInternal(), *pools[stream_index], enable_fine_grained_shuffle);
            else
                insertFromBlockImpl<ASTTableJoin::Strictness::All>(type, maps_all, rows, key_columns, key_sizes, collators, stored_block, null_map, nullptr, stream_index, getBuildConcurrencyInternal(), *pools[stream_index], enable_fine_grained_shuffle);
        }
        else
        {
            if (strictness == ASTTableJoin::Strictness::Any)
                insertFromBlockImpl<ASTTableJoin::Strictness::Any>(type, maps_any_full, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map[stream_index].get(), stream_index, getBuildConcurrencyInternal(), *pools[stream_index], enable_fine_grained_shuffle);
            else
                insertFromBlockImpl<ASTTableJoin::Strictness::All>(type, maps_all_full, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map[stream_index].get(), stream_index, getBuildConcurrencyInternal(), *pools[stream_index], enable_fine_grained_shuffle);
        }
    }
}


namespace
{
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Map>
struct Adder;

template <typename Map>
struct Adder<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any, Map>
{
    static void addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & it, size_t num_columns_to_add, MutableColumns & added_columns, size_t /*i*/, IColumn::Filter * /*filter*/, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, const std::vector<size_t> & right_indexes)
    {
        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertFrom(*it->getMapped().block->getByPosition(right_indexes[j]).column.get(), it->getMapped().row_num);
    }

    static void addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t /*i*/, IColumn::Filter * /*filter*/, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/)
    {
        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertDefault();
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any, Map>
{
    static void addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & it, size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, const std::vector<size_t> & right_indexes)
    {
        (*filter)[i] = 1;

        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertFrom(*it->getMapped().block->getByPosition(right_indexes[j]).column.get(), it->getMapped().row_num);
    }

    static void addNotFound(size_t /*num_columns_to_add*/, MutableColumns & /*added_columns*/, size_t i, IColumn::Filter * filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/)
    {
        (*filter)[i] = 0;
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::Anti, ASTTableJoin::Strictness::Any, Map>
{
    static void addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & /*it*/, size_t /*num_columns_to_add*/, MutableColumns & /*added_columns*/, size_t i, IColumn::Filter * filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, const std::vector<size_t> & /*right_indexes*/)
    {
        (*filter)[i] = 0;
    }

    static void addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/)
    {
        (*filter)[i] = 1;
        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertDefault();
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::LeftSemi, ASTTableJoin::Strictness::Any, Map>
{
    static void addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & /*it*/, size_t num_columns_to_add, MutableColumns & added_columns, size_t /*i*/, IColumn::Filter * /*filter*/, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, const std::vector<size_t> & /*right_indexes*/)
    {
        for (size_t j = 0; j < num_columns_to_add - 1; ++j)
            added_columns[j]->insertDefault();
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_1);
    }

    static void addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t /*i*/, IColumn::Filter * /*filter*/, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/)
    {
        for (size_t j = 0; j < num_columns_to_add - 1; ++j)
            added_columns[j]->insertDefault();
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_0);
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::LeftSemi, ASTTableJoin::Strictness::All, Map>
{
    static void addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & it, size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * /*filter*/, IColumn::Offset & current_offset, IColumn::Offsets * offsets, const std::vector<size_t> & right_indexes)
    {
        for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped()); current != nullptr; current = current->next)
        {
            for (size_t j = 0; j < num_columns_to_add - 1; ++j)
                added_columns[j]->insertFrom(*current->block->getByPosition(right_indexes[j]).column.get(), current->row_num);
            ++current_offset;
        }
        (*offsets)[i] = current_offset;
        /// we insert only one row to `match-helper` for each row of left block
        /// so before the execution of `HandleOtherConditions`, column sizes of temporary block may be different.
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_1);
    }

    static void addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * /*filter*/, IColumn::Offset & current_offset, IColumn::Offsets * offsets)
    {
        ++current_offset;
        (*offsets)[i] = current_offset;

        for (size_t j = 0; j < num_columns_to_add - 1; ++j)
            added_columns[j]->insertDefault();
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_0);
    }
};

template <ASTTableJoin::Kind KIND, typename Map>
struct Adder<KIND, ASTTableJoin::Strictness::All, Map>
{
    static void addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & it, size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * filter, IColumn::Offset & current_offset, IColumn::Offsets * offsets, const std::vector<size_t> & right_indexes)
    {
        size_t rows_joined = 0;
        for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped()); current != nullptr; current = current->next)
        {
            for (size_t j = 0; j < num_columns_to_add; ++j)
                added_columns[j]->insertFrom(*current->block->getByPosition(right_indexes[j]).column.get(), current->row_num);

            ++rows_joined;
        }

        current_offset += rows_joined;
        (*offsets)[i] = current_offset;
        if (KIND == ASTTableJoin::Kind::Anti)
            /// anti join with other condition is very special: if the row is matched during probe stage, we can not throw it
            /// away because it might failed in other condition, so we add the matched rows to the result, but set (*filter)[i] = 0
            /// to indicate that the row is matched during probe stage, this will be used in handleOtherConditions
            (*filter)[i] = 0;
    }

    static void addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * filter, IColumn::Offset & current_offset, IColumn::Offsets * offsets)
    {
        if (KIND == ASTTableJoin::Kind::Inner)
        {
            (*offsets)[i] = current_offset;
        }
        else
        {
            if (KIND == ASTTableJoin::Kind::Anti)
                (*filter)[i] = 1;
            ++current_offset;
            (*offsets)[i] = current_offset;

            for (size_t j = 0; j < num_columns_to_add; ++j)
                added_columns[j]->insertDefault();
        }
    }
};

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
void NO_INLINE joinBlockImplTypeCase(
    const Map & map,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    MutableColumns & added_columns,
    ConstNullMapPtr null_map,
    std::unique_ptr<IColumn::Filter> & filter,
    IColumn::Offset & current_offset,
    std::unique_ptr<IColumn::Offsets> & offsets_to_replicate,
    const std::vector<size_t> & right_indexes,
    const TiDB::TiDBCollators & collators,
    bool enable_fine_grained_shuffle,
    size_t fine_grained_shuffle_count)
{
    size_t num_columns_to_add = right_indexes.size();

    KeyGetter key_getter(key_columns, key_sizes, collators);
    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(key_columns.size());
    Arena pool;
    WeakHash32 shuffle_hash(0); /// reproduce hash values in FinedGrainedShuffleWriter
    if (enable_fine_grained_shuffle && rows > 0)
    {
        /// TODO: consider adding a virtual column in Sender side to avoid computing cost and potential inconsistency by heterogeneous envs(AMD64, ARM64)
        /// Note: 1. Not sure, if inconsistency will do happen in heterogeneous envs
        ///       2. Virtual column would take up a little more network bandwidth, might lead to poor performance if network was bottleneck
        /// Currently, the computation cost is tolerable, since it's a very simple crc32 hash algorithm, and heterogeneous envs support is not considered
        HashBaseWriterHelper::computeHash(rows,
                                          key_columns,
                                          collators,
                                          sort_key_containers,
                                          shuffle_hash);
    }

    size_t segment_size = map.getSegmentSize();
    const auto & shuffle_hash_data = shuffle_hash.getData();
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            Adder<KIND, STRICTNESS, Map>::addNotFound(
                num_columns_to_add,
                added_columns,
                i,
                filter.get(),
                current_offset,
                offsets_to_replicate.get());
        }
        else
        {
            auto key_holder = key_getter.getKeyHolder(i, &pool, sort_key_containers);
            auto key = keyHolderGetKey(key_holder);
            size_t hash_value = 0;
            bool zero_flag = ZeroTraits::check(key);
            if (segment_size > 0 && !zero_flag)
            {
                hash_value = map.hash(key);
            }

            size_t segment_index = 0;
            if (enable_fine_grained_shuffle)
            {
                RUNTIME_CHECK(segment_size > 0);
                /// Need to calculate the correct segment_index so that rows with same key will map to the same segment_index both in Build and Prob
                /// The "reproduce" of segment_index generated in Build phase relies on the facts that:
                /// Possible pipelines(FineGrainedShuffleWriter => ExchangeReceiver => HashBuild)
                /// 1. In FineGrainedShuffleWriter, selector value finally maps to packet_stream_id by '% fine_grained_shuffle_count'
                /// 2. In ExchangeReceiver, build_stream_id = packet_stream_id % build_stream_count;
                /// 3. In HashBuild, build_concurrency decides map's segment size, and build_steam_id decides the segment index
                auto packet_stream_id = shuffle_hash_data[i] % fine_grained_shuffle_count;
                segment_index = packet_stream_id % segment_size;
            }
            else
            {
                if (segment_size > 0 && !zero_flag)
                {
                    segment_index = hash_value % segment_size;
                }
            }

            auto & internal_map = map.getSegmentTable(segment_index);
            /// do not require segment lock because in join, the hash table can not be changed in probe stage.
            auto it = segment_size > 0 ? internal_map.find(key, hash_value) : internal_map.find(key);
            if (it != internal_map.end())
            {
                it->getMapped().setUsed();
                Adder<KIND, STRICTNESS, Map>::addFound(
                    it,
                    num_columns_to_add,
                    added_columns,
                    i,
                    filter.get(),
                    current_offset,
                    offsets_to_replicate.get(),
                    right_indexes);
            }
            else
                Adder<KIND, STRICTNESS, Map>::addNotFound(
                    num_columns_to_add,
                    added_columns,
                    i,
                    filter.get(),
                    current_offset,
                    offsets_to_replicate.get());
            keyHolderDiscardKey(key_holder);
        }
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
void joinBlockImplType(
    const Map & map,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    MutableColumns & added_columns,
    ConstNullMapPtr null_map,
    std::unique_ptr<IColumn::Filter> & filter,
    IColumn::Offset & current_offset,
    std::unique_ptr<IColumn::Offsets> & offsets_to_replicate,
    const std::vector<size_t> & right_indexes,
    const TiDB::TiDBCollators & collators,
    bool enable_fine_grained_shuffle,
    size_t fine_grained_shuffle_count)
{
    if (null_map)
        joinBlockImplTypeCase<KIND, STRICTNESS, KeyGetter, Map, true>(
            map,
            rows,
            key_columns,
            key_sizes,
            added_columns,
            null_map,
            filter,
            current_offset,
            offsets_to_replicate,
            right_indexes,
            collators,
            enable_fine_grained_shuffle,
            fine_grained_shuffle_count);
    else
        joinBlockImplTypeCase<KIND, STRICTNESS, KeyGetter, Map, false>(
            map,
            rows,
            key_columns,
            key_sizes,
            added_columns,
            null_map,
            filter,
            current_offset,
            offsets_to_replicate,
            right_indexes,
            collators,
            enable_fine_grained_shuffle,
            fine_grained_shuffle_count);
}
} // namespace

void mergeNullAndFilterResult(Block & block, ColumnVector<UInt8>::Container & filter_column, const String & filter_column_name, bool null_as_true)
{
    auto orig_filter_column = block.getByName(filter_column_name).column;
    if (orig_filter_column->isColumnConst())
        orig_filter_column = orig_filter_column->convertToFullColumnIfConst();
    if (orig_filter_column->isColumnNullable())
    {
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(orig_filter_column.get());
        const auto & nested_column_data = static_cast<const ColumnVector<UInt8> *>(nullable_column->getNestedColumnPtr().get())->getData();
        for (size_t i = 0; i < nullable_column->size(); i++)
        {
            if (filter_column[i] == 0)
                continue;
            if (nullable_column->isNullAt(i))
                filter_column[i] = null_as_true;
            else
                filter_column[i] = filter_column[i] && nested_column_data[i];
        }
    }
    else
    {
        const auto * other_filter_column = checkAndGetColumn<ColumnVector<UInt8>>(orig_filter_column.get());
        const auto & other_filter_column_data = static_cast<const ColumnVector<UInt8> *>(other_filter_column)->getData();
        for (size_t i = 0; i < other_filter_column->size(); i++)
            filter_column[i] = filter_column[i] && other_filter_column_data[i];
    }
}

/**
 * handle other join conditions
 * Join Kind/Strictness               ALL               ANY
 *     INNER                    TiDB inner join    TiDB semi join
 *     LEFT                     TiDB left join     should not happen
 *     RIGHT                    should not happen  should not happen
 *     ANTI                     should not happen  TiDB anti semi join
 * @param block
 * @param offsets_to_replicate
 * @param left_table_columns
 * @param right_table_columns
 */
void Join::handleOtherConditions(Block & block, std::unique_ptr<IColumn::Filter> & anti_filter, std::unique_ptr<IColumn::Offsets> & offsets_to_replicate, const std::vector<size_t> & right_table_columns) const
{
    other_condition_ptr->execute(block);

    auto filter_column = ColumnUInt8::create();
    auto & filter = filter_column->getData();
    filter.assign(block.rows(), static_cast<UInt8>(1));
    if (!other_filter_column.empty())
    {
        mergeNullAndFilterResult(block, filter, other_filter_column, false);
    }

    ColumnUInt8::Container row_filter(filter.size(), 0);

    if (isLeftSemiFamily(kind))
    {
        const auto helper_pos = block.getPositionByName(match_helper_name);

        const auto * old_match_nullable = checkAndGetColumn<ColumnNullable>(block.safeGetByPosition(helper_pos).column.get());
        const auto & old_match_vec = static_cast<const ColumnVector<Int8> *>(old_match_nullable->getNestedColumnPtr().get())->getData();

        {
            /// we assume there is no null value in the `match-helper` column after adder<>().
            if (!mem_utils::memoryIsZero(old_match_nullable->getNullMapData().data(), old_match_nullable->getNullMapData().size()))
                throw Exception("T here shouldn't be null before merging other conditions.", ErrorCodes::LOGICAL_ERROR);
        }

        const auto rows = offsets_to_replicate->size();
        if (old_match_vec.size() != rows)
            throw Exception("Size of column match-helper must be equal to column size of left block.", ErrorCodes::LOGICAL_ERROR);

        auto match_col = ColumnInt8::create(rows, 0);
        auto & match_vec = match_col->getData();
        auto match_nullmap = ColumnUInt8::create(rows, 0);
        auto & match_nullmap_vec = match_nullmap->getData();

        /// nullmap and data of `other_eq_filter_from_in_column`.
        const ColumnUInt8::Container *eq_in_vec = nullptr, *eq_in_nullmap = nullptr;
        if (!other_eq_filter_from_in_column.empty())
        {
            auto orig_filter_column = block.getByName(other_eq_filter_from_in_column).column;
            if (orig_filter_column->isColumnConst())
                orig_filter_column = orig_filter_column->convertToFullColumnIfConst();
            if (orig_filter_column->isColumnNullable())
            {
                const auto * nullable_column = checkAndGetColumn<ColumnNullable>(orig_filter_column.get());
                eq_in_vec = &static_cast<const ColumnVector<UInt8> *>(nullable_column->getNestedColumnPtr().get())->getData();
                eq_in_nullmap = &nullable_column->getNullMapData();
            }
            else
                eq_in_vec = &checkAndGetColumn<ColumnUInt8>(orig_filter_column.get())->getData();
        }

        /// for (anti)leftSemi join, we should keep only one row for each original row of left table.
        /// and because it is semi join, we needn't save columns of right table, so we just keep the first replica.
        for (size_t i = 0; i < offsets_to_replicate->size(); i++)
        {
            size_t prev_offset = i > 0 ? (*offsets_to_replicate)[i - 1] : 0;
            size_t current_offset = (*offsets_to_replicate)[i];

            row_filter[prev_offset] = 1;
            if (old_match_vec[i] == 0)
                continue;

            /// fill match_vec and match_nullmap_vec
            /// if there is `1` in filter, match_vec is 1.
            /// if there is `null` in eq_in_nullmap, match_nullmap_vec is 1.
            /// else, match_vec is 0.

            bool has_row_matched = false, has_row_null = false;
            for (size_t index = prev_offset; index < current_offset; index++)
            {
                if (!filter[index])
                    continue;
                if (eq_in_nullmap && (*eq_in_nullmap)[index])
                    has_row_null = true;
                else if (!eq_in_vec || (*eq_in_vec)[index])
                {
                    has_row_matched = true;
                    break;
                }
            }

            if (has_row_matched)
                match_vec[i] = 1;
            else if (has_row_null)
                match_nullmap_vec[i] = 1;
        }

        for (size_t i = 0; i < block.columns(); i++)
            if (i != helper_pos)
                block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        block.safeGetByPosition(helper_pos).column = ColumnNullable::create(std::move(match_col), std::move(match_nullmap));
        return;
    }

    if (!other_eq_filter_from_in_column.empty())
    {
        /// other_eq_filter_from_in_column is used in anti semi join:
        /// if there is a row that return null or false for other_condition, then for anti semi join, this row should be returned.
        /// otherwise, it will check other_eq_filter_from_in_column, if other_eq_filter_from_in_column return false, this row should
        /// be returned, if other_eq_filter_from_in_column return true or null this row should not be returned.
        mergeNullAndFilterResult(block, filter, other_eq_filter_from_in_column, isAntiJoin(kind));
    }

    if (isInnerJoin(kind) && original_strictness == ASTTableJoin::Strictness::All)
    {
        /// inner join, just use other_filter_column to filter result
        for (size_t i = 0; i < block.columns(); i++)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, -1);
        return;
    }

    for (size_t i = 0, prev_offset = 0; i < offsets_to_replicate->size(); i++)
    {
        size_t current_offset = (*offsets_to_replicate)[i];
        bool has_row_kept = false;
        for (size_t index = prev_offset; index < current_offset; index++)
        {
            if (original_strictness == ASTTableJoin::Strictness::Any)
            {
                /// for semi/anti join, at most one row is kept
                row_filter[index] = !has_row_kept && filter[index];
            }
            else
            {
                /// strictness = ALL && kind = Anti should not happens
                row_filter[index] = filter[index];
            }
            if (row_filter[index])
                has_row_kept = true;
        }
        if (prev_offset < current_offset)
        {
            /// for outer join, at least one row must be kept
            if (isLeftJoin(kind) && !has_row_kept)
                row_filter[prev_offset] = 1;
            if (isAntiJoin(kind))
            {
                if (has_row_kept && !(*anti_filter)[i])
                    /// anti_filter=false means equal condition is matched,
                    /// has_row_kept=true means other condition is matched,
                    /// for anti join, we should not return any rows when the both conditions are matched.
                    for (size_t index = prev_offset; index < current_offset; index++)
                        row_filter[index] = 0;
                else
                    /// when there is a condition not matched, we should return this row.
                    row_filter[prev_offset] = 1;
            }
        }
        prev_offset = current_offset;
    }
    if (isLeftJoin(kind))
    {
        /// for left join, convert right column to null if not joined
        for (size_t right_table_column : right_table_columns)
        {
            auto & column = block.getByPosition(right_table_column);
            auto full_column = column.column->isColumnConst() ? column.column->convertToFullColumnIfConst() : column.column;
            if (!full_column->isColumnNullable())
            {
                throw Exception("Should not reach here, the right table column for left join must be nullable");
            }
            auto current_column = full_column;
            auto result_column = (*std::move(current_column)).mutate();
            static_cast<ColumnNullable &>(*result_column).applyNegatedNullMap(*filter_column);
            column.column = std::move(result_column);
        }
        for (size_t i = 0; i < block.columns(); i++)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        return;
    }
    if (isInnerJoin(kind) || isAntiJoin(kind))
    {
        /// for semi/anti join, filter out not matched rows
        for (size_t i = 0; i < block.columns(); i++)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        return;
    }
    throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void Join::joinBlockImpl(Block & block, const Maps & maps) const
{
    size_t keys_size = key_names_left.size();
    ColumnRawPtrs key_columns(keys_size);

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    Columns materialized_columns;

    /// Memoize key columns to work with.
    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns[i] = block.getByName(key_names_left[i]).column.get();

        if (ColumnPtr converted = key_columns[i]->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns[i] = materialized_columns.back().get();
        }
    }

    /// Keys with NULL value in any column won't join to anything.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter won't join to anything
    recordFilteredRows(block, left_filter_column, null_map_holder, null_map);

    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if (getFullness(kind))
    {
        for (size_t i = 0; i < existing_columns; ++i)
        {
            auto & col = block.getByPosition(i).column;

            if (ColumnPtr converted = col->convertToFullColumnIfConst())
                col = converted;

            /// If use_nulls, convert left columns (except keys) to Nullable.
            if (use_nulls)
            {
                if (std::end(key_names_left) == std::find(key_names_left.begin(), key_names_left.end(), block.getByPosition(i).name))
                    convertColumnToNullable(block.getByPosition(i));
            }
        }
    }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      */
    size_t num_columns_to_skip = 0;
    if (getFullness(kind))
        num_columns_to_skip = keys_size;

    /// Add new columns to the block.
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();
    MutableColumns added_columns;
    added_columns.reserve(num_columns_to_add);

    std::vector<size_t> right_table_column_indexes;
    for (size_t i = 0; i < num_columns_to_add; i++)
    {
        right_table_column_indexes.push_back(i + existing_columns);
    }

    std::vector<size_t> right_indexes;
    right_indexes.reserve(num_columns_to_add);

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.safeGetByPosition(i);

        /// Don't insert column if it's in left block.
        if (!block.has(src_column.name))
        {
            added_columns.push_back(src_column.column->cloneEmpty());
            added_columns.back()->reserve(src_column.column->size());
            right_indexes.push_back(num_columns_to_skip + i);
        }
    }

    size_t rows = block.rows();

    /// Used with ANY INNER JOIN
    std::unique_ptr<IColumn::Filter> filter;

    if (((kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Right) && strictness == ASTTableJoin::Strictness::Any)
        || kind == ASTTableJoin::Kind::Anti)
        filter = std::make_unique<IColumn::Filter>(rows);

    /// Used with ALL ... JOIN
    IColumn::Offset current_offset = 0;
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;

    if (strictness == ASTTableJoin::Strictness::All)
        offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    switch (type)
    {
#define M(TYPE)                                                                                                                                \
    case Join::Type::TYPE:                                                                                                                     \
        joinBlockImplType<KIND, STRICTNESS, typename KeyGetterForType<Join::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
            *maps.TYPE,                                                                                                                        \
            rows,                                                                                                                              \
            key_columns,                                                                                                                       \
            key_sizes,                                                                                                                         \
            added_columns,                                                                                                                     \
            null_map,                                                                                                                          \
            filter,                                                                                                                            \
            current_offset,                                                                                                                    \
            offsets_to_replicate,                                                                                                              \
            right_indexes,                                                                                                                     \
            collators,                                                                                                                         \
            enable_fine_grained_shuffle,                                                                                                       \
            fine_grained_shuffle_count);                                                                                                       \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_prob_failpoint);
    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & sample_col = sample_block_with_columns_to_add.getByPosition(i);
        block.insert(ColumnWithTypeAndName(std::move(added_columns[i]), sample_col.type, sample_col.name));
    }

    /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
    if (filter && !(kind == ASTTableJoin::Kind::Anti && strictness == ASTTableJoin::Strictness::All))
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(*filter, -1);

    /// If ALL ... JOIN - we replicate all the columns except the new ones.
    if (offsets_to_replicate)
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);

    /// handle other conditions
    if (!other_filter_column.empty() || !other_eq_filter_from_in_column.empty())
    {
        if (!offsets_to_replicate)
            throw Exception("Should not reach here, the strictness of join with other condition must be ALL");
        handleOtherConditions(block, filter, offsets_to_replicate, right_table_column_indexes);
    }
}

namespace
{
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder;

template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>
{
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        size_t expanded_row_size = 0;
        for (const Block & block_right : blocks)
        {
            size_t rows_right = block_right.rows();
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                rows_right = std::min(rows_right, 1);
            }

            for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
                for (size_t j = 0; j < rows_right; ++j)
                    dst_columns[col_num]->insertFrom(*src_left_columns[col_num], i);

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn * column_right = block_right.getByPosition(col_num).column.get();

                for (size_t j = 0; j < rows_right; ++j)
                    dst_columns[num_existing_columns + col_num]->insertFrom(*column_right, j);
            }
            expanded_row_size += rows_right;
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                if (expanded_row_size >= 1)
                    break;
            }
        }
        (*is_row_matched)[i - start_offset] = 0;
        (*expanded_row_size_after_join)[i - start_offset] = current_offset + expanded_row_size;
        current_offset += expanded_row_size;
    }
    static void addNotFound(MutableColumns & /* dst_columns */, size_t /* num_existing_columns */, ColumnRawPtrs & /* src_left_columns */, size_t /* num_columns_to_add */, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        /// for inner all/any join, just skip this row
        (*is_row_matched)[i - start_offset] = 0;
        (*expanded_row_size_after_join)[i - start_offset] = current_offset;
    }
    static bool allRightRowsMaybeAdded()
    {
        return STRICTNESS == ASTTableJoin::Strictness::All;
    }
};
template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Left, STRICTNESS>
{
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, blocks, is_row_matched, current_offset, expanded_row_size_after_join);
    }
    static void addNotFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        /// for left all/any join, mark this row as matched
        (*is_row_matched)[i - start_offset] = 1;
        (*expanded_row_size_after_join)[i - start_offset] = 1 + current_offset;
        current_offset += 1;
        for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
            dst_columns[col_num]->insertFrom(*src_left_columns[col_num], i);
        for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            dst_columns[num_existing_columns + col_num]->insertDefault();
    }
    static bool allRightRowsMaybeAdded()
    {
        return STRICTNESS == ASTTableJoin::Strictness::All;
    }
};
template <>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Anti, ASTTableJoin::Strictness::Any>
{
    static void addFound(MutableColumns & /* dst_columns */, size_t /* num_existing_columns */, ColumnRawPtrs & /* src_left_columns */, size_t /* num_columns_to_add */, size_t start_offset, size_t i, const BlocksList & /* blocks */, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        (*is_row_matched)[i - start_offset] = 0;
        (*expanded_row_size_after_join)[i - start_offset] = current_offset;
    }
    static void addNotFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross_Left, ASTTableJoin::Strictness::Any>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, is_row_matched, current_offset, expanded_row_size_after_join);
    }
    static bool allRightRowsMaybeAdded()
    {
        return false;
    }
};
template <>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_Anti, ASTTableJoin::Strictness::All>
{
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::All>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, blocks, is_row_matched, current_offset, expanded_row_size_after_join);
    }
    static void addNotFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross_Left, ASTTableJoin::Strictness::Any>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, is_row_matched, current_offset, expanded_row_size_after_join);
    }
    static bool allRightRowsMaybeAdded()
    {
        return true;
    }
};
template <ASTTableJoin::Strictness STRICTNESS>
struct CrossJoinAdder<ASTTableJoin::Kind::Cross_LeftSemi, STRICTNESS>
{
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add - 1, start_offset, i, blocks, is_row_matched, current_offset, expanded_row_size_after_join);
        dst_columns[num_existing_columns + num_columns_to_add - 1]->insert(FIELD_INT8_1);
    }
    static void addNotFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross_Left, STRICTNESS>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add - 1, start_offset, i, is_row_matched, current_offset, expanded_row_size_after_join);
        dst_columns[num_existing_columns + num_columns_to_add - 1]->insert(FIELD_INT8_0);
    }
    static bool allRightRowsMaybeAdded()
    {
        return STRICTNESS == ASTTableJoin::Strictness::All;
    }
};
} // namespace

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, bool has_null_map>
void Join::joinBlockImplCrossInternal(Block & block, ConstNullMapPtr null_map [[maybe_unused]]) const
{
    /// Add new columns to the block.
    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();
    size_t rows_left = block.rows();

    ColumnRawPtrs src_left_columns(num_existing_columns);

    for (size_t i = 0; i < num_existing_columns; ++i)
    {
        src_left_columns[i] = block.getByPosition(i).column.get();
    }

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.getByPosition(i);
        block.insert(src_column);
    }

    /// NOTE It would be better to use `reserve`, as well as `replicate` methods to duplicate the values of the left block.
    size_t right_table_rows = 0;
    for (const Block & block_right : blocks)
        right_table_rows += block_right.rows();

    size_t left_rows_per_iter = std::max(rows_left, 1);
    if (max_block_size_for_cross_join > 0 && right_table_rows > 0 && other_condition_ptr != nullptr
        && CrossJoinAdder<KIND, STRICTNESS>::allRightRowsMaybeAdded())
    {
        /// if other_condition is not null, and all right columns maybe added during join, try to use multiple iter
        /// to make memory usage under control, for anti semi cross join that is converted by not in subquery,
        /// it is likely that other condition may filter out most of the rows
        left_rows_per_iter = std::max(max_block_size_for_cross_join / right_table_rows, 1);
    }

    std::vector<size_t> right_column_index;
    for (size_t i = 0; i < num_columns_to_add; i++)
        right_column_index.push_back(num_existing_columns + i);

    std::vector<Block> result_blocks;
    for (size_t start = 0; start <= rows_left; start += left_rows_per_iter)
    {
        size_t end = std::min(start + left_rows_per_iter, rows_left);
        MutableColumns dst_columns(num_existing_columns + num_columns_to_add);
        for (size_t i = 0; i < block.columns(); i++)
        {
            dst_columns[i] = block.getByPosition(i).column->cloneEmpty();
        }
        IColumn::Offset current_offset = 0;
        std::unique_ptr<IColumn::Filter> is_row_matched = std::make_unique<IColumn::Filter>(end - start);
        std::unique_ptr<IColumn::Offsets> expanded_row_size_after_join = std::make_unique<IColumn::Offsets>(end - start);
        for (size_t i = start; i < end; i++)
        {
            if constexpr (has_null_map)
            {
                if ((*null_map)[i])
                {
                    /// filter out by left_conditions, so just treated as not joined column
                    CrossJoinAdder<KIND, STRICTNESS>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start, i, is_row_matched.get(), current_offset, expanded_row_size_after_join.get());
                    continue;
                }
            }
            if (right_table_rows > 0)
            {
                CrossJoinAdder<KIND, STRICTNESS>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start, i, blocks, is_row_matched.get(), current_offset, expanded_row_size_after_join.get());
            }
            else
            {
                CrossJoinAdder<KIND, STRICTNESS>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start, i, is_row_matched.get(), current_offset, expanded_row_size_after_join.get());
            }
        }
        auto block_per_iter = block.cloneWithColumns(std::move(dst_columns));
        if (other_condition_ptr != nullptr)
            handleOtherConditions(block_per_iter, is_row_matched, expanded_row_size_after_join, right_column_index);
        if (start == 0 || block_per_iter.rows() > 0)
            /// always need to generate at least one block
            result_blocks.push_back(block_per_iter);
    }

    if (result_blocks.size() == 1)
    {
        block = result_blocks[0];
    }
    else
    {
        auto & sample_block = result_blocks[0];
        MutableColumns dst_columns(sample_block.columns());
        for (size_t i = 0; i < sample_block.columns(); i++)
        {
            dst_columns[i] = sample_block.getByPosition(i).column->cloneEmpty();
        }
        for (auto & current_block : result_blocks)
        {
            if (current_block.rows() > 0)
            {
                for (size_t column = 0; column < current_block.columns(); column++)
                {
                    dst_columns[column]->insertRangeFrom(*current_block.getByPosition(column).column, 0, current_block.rows());
                }
            }
        }
        block = sample_block.cloneWithColumns(std::move(dst_columns));
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
void Join::joinBlockImplCross(Block & block) const
{
    size_t rows_left = block.rows();
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    recordFilteredRows(block, left_filter_column, null_map_holder, null_map);

    std::unique_ptr<IColumn::Filter> filter = std::make_unique<IColumn::Filter>(rows_left);
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows_left);

    if (null_map)
        joinBlockImplCrossInternal<KIND, STRICTNESS, true>(block, null_map);
    else
        joinBlockImplCrossInternal<KIND, STRICTNESS, false>(block, nullptr);
}


void Join::checkTypesOfKeys(const Block & block_left, const Block & block_right) const
{
    size_t keys_size = key_names_left.size();

    for (size_t i = 0; i < keys_size; ++i)
    {
        /// Compare up to Nullability.

        DataTypePtr left_type = removeNullable(block_left.getByName(key_names_left[i]).type);
        DataTypePtr right_type = removeNullable(block_right.getByName(key_names_right[i]).type);

        if (!left_type->equals(*right_type))
            throw Exception("Type mismatch of columns to JOIN by: "
                                + key_names_left[i] + " " + left_type->getName() + " at left, "
                                + key_names_right[i] + " " + right_type->getName() + " at right",
                            ErrorCodes::TYPE_MISMATCH);
    }
}

void Join::joinBlock(Block & block) const
{
    //    std::cerr << "joinBlock: " << block.dumpStructure() << "\n";

    // ck will use this function to generate header, that's why here is a check.
    {
        std::unique_lock lk(build_table_mutex);

        build_table_cv.wait(lk, [&]() { return build_table_state != BuildTableState::WAITING; });
        if (build_table_state == BuildTableState::FAILED) /// throw this exception once failed to build the hash table
            throw Exception("Build failed before join probe!");
    }

    std::shared_lock lock(rwlock);

    checkTypesOfKeys(block, sample_block_with_keys);

    /// TODO: after we bumping to C++20, use `using enum` to simplify code here.
    /// using enum ASTTableJoin::Strictness;
    /// using enum ASTTableJoin::Kind;

    if (kind == ASTTableJoin::Kind::Left && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>(block, maps_any);
    else if (kind == ASTTableJoin::Kind::Inner && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImpl<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any>(block, maps_any);
    else if (kind == ASTTableJoin::Kind::Left && strictness == ASTTableJoin::Strictness::All)
        joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::All>(block, maps_all);
    else if (kind == ASTTableJoin::Kind::Inner && strictness == ASTTableJoin::Strictness::All)
        joinBlockImpl<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::All>(block, maps_all);
    else if (kind == ASTTableJoin::Kind::Full && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>(block, maps_any_full);
    else if (kind == ASTTableJoin::Kind::Right && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImpl<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any>(block, maps_any_full);
    else if (kind == ASTTableJoin::Kind::Full && strictness == ASTTableJoin::Strictness::All)
        joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::All>(block, maps_all_full);
    else if (kind == ASTTableJoin::Kind::Right && strictness == ASTTableJoin::Strictness::All)
        joinBlockImpl<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::All>(block, maps_all_full);
    else if (kind == ASTTableJoin::Kind::Anti && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImpl<ASTTableJoin::Kind::Anti, ASTTableJoin::Strictness::Any>(block, maps_any);
    else if (kind == ASTTableJoin::Kind::Anti && strictness == ASTTableJoin::Strictness::All)
        joinBlockImpl<ASTTableJoin::Kind::Anti, ASTTableJoin::Strictness::All>(block, maps_all);
    else if (kind == ASTTableJoin::Kind::LeftSemi && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImpl<ASTTableJoin::Kind::LeftSemi, ASTTableJoin::Strictness::Any>(block, maps_any);
    else if (kind == ASTTableJoin::Kind::LeftSemi && strictness == ASTTableJoin::Strictness::All)
        joinBlockImpl<ASTTableJoin::Kind::LeftSemi, ASTTableJoin::Strictness::All>(block, maps_all);
    else if (kind == ASTTableJoin::Kind::LeftAnti && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImpl<ASTTableJoin::Kind::LeftSemi, ASTTableJoin::Strictness::Any>(block, maps_any);
    else if (kind == ASTTableJoin::Kind::LeftAnti && strictness == ASTTableJoin::Strictness::All)
        joinBlockImpl<ASTTableJoin::Kind::LeftSemi, ASTTableJoin::Strictness::All>(block, maps_all);
    else if (kind == ASTTableJoin::Kind::Cross && strictness == ASTTableJoin::Strictness::All)
        joinBlockImplCross<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::All>(block);
    else if (kind == ASTTableJoin::Kind::Cross && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImplCross<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::Any>(block);
    else if (kind == ASTTableJoin::Kind::Cross_Left && strictness == ASTTableJoin::Strictness::All)
        joinBlockImplCross<ASTTableJoin::Kind::Cross_Left, ASTTableJoin::Strictness::All>(block);
    else if (kind == ASTTableJoin::Kind::Cross_Left && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImplCross<ASTTableJoin::Kind::Cross_Left, ASTTableJoin::Strictness::Any>(block);
    else if (kind == ASTTableJoin::Kind::Cross_Anti && strictness == ASTTableJoin::Strictness::All)
        joinBlockImplCross<ASTTableJoin::Kind::Cross_Anti, ASTTableJoin::Strictness::All>(block);
    else if (kind == ASTTableJoin::Kind::Cross_Anti && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImplCross<ASTTableJoin::Kind::Cross_Anti, ASTTableJoin::Strictness::Any>(block);
    else if (kind == ASTTableJoin::Kind::Cross_LeftSemi && strictness == ASTTableJoin::Strictness::All)
        joinBlockImplCross<ASTTableJoin::Kind::Cross_LeftSemi, ASTTableJoin::Strictness::All>(block);
    else if (kind == ASTTableJoin::Kind::Cross_LeftSemi && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImplCross<ASTTableJoin::Kind::Cross_LeftSemi, ASTTableJoin::Strictness::Any>(block);
    else if (kind == ASTTableJoin::Kind::Cross_LeftAnti && strictness == ASTTableJoin::Strictness::All)
        joinBlockImplCross<ASTTableJoin::Kind::Cross_LeftSemi, ASTTableJoin::Strictness::All>(block);
    else if (kind == ASTTableJoin::Kind::Cross_LeftAnti && strictness == ASTTableJoin::Strictness::Any)
        joinBlockImplCross<ASTTableJoin::Kind::Cross_LeftSemi, ASTTableJoin::Strictness::Any>(block);
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);

    /// for (cartesian)antiLeftSemi join, the meaning of "match-helper" is `non-matched` instead of `matched`.
    if (kind == ASTTableJoin::Kind::LeftAnti || kind == ASTTableJoin::Kind::Cross_LeftAnti)
    {
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(block.getByName(match_helper_name).column.get());
        const auto & vec_matched = static_cast<const ColumnVector<Int8> *>(nullable_column->getNestedColumnPtr().get())->getData();

        auto col_non_matched = ColumnInt8::create(vec_matched.size());
        auto & vec_non_matched = col_non_matched->getData();

        for (size_t i = 0; i < vec_matched.size(); ++i)
            vec_non_matched[i] = !vec_matched[i];

        block.getByName(match_helper_name).column = ColumnNullable::create(std::move(col_non_matched), std::move(nullable_column->getNullMapColumnPtr()));
    }
}

void Join::joinTotals(Block & block) const
{
    Block totals_without_keys = totals;

    if (totals_without_keys)
    {
        for (const auto & name : key_names_right)
            totals_without_keys.erase(totals_without_keys.getPositionByName(name));

        for (size_t i = 0; i < totals_without_keys.columns(); ++i)
            block.insert(totals_without_keys.safeGetByPosition(i));
    }
    else
    {
        /// We will join empty `totals` - from one row with the default values.

        for (size_t i = 0; i < sample_block_with_columns_to_add.columns(); ++i)
        {
            const auto & col = sample_block_with_columns_to_add.getByPosition(i);
            block.insert({col.type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst(),
                          col.type,
                          col.name});
        }
    }
}


template <ASTTableJoin::Strictness STRICTNESS, typename Mapped>
struct AdderNonJoined;

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::Any, Mapped>
{
    static size_t add(const Mapped & mapped, size_t key_num, size_t num_columns_left, MutableColumns & columns_left, size_t num_columns_right, MutableColumns & columns_right)
    {
        for (size_t j = 0; j < num_columns_left; ++j)
            columns_left[j]->insertDefault();

        for (size_t j = 0; j < num_columns_right; ++j)
            columns_right[j]->insertFrom(*mapped.block->getByPosition(key_num + j).column.get(), mapped.row_num);
        return 1;
    }
};

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::All, Mapped>
{
    static size_t add(const Mapped & mapped, size_t key_num, size_t num_columns_left, MutableColumns & columns_left, size_t num_columns_right, MutableColumns & columns_right)
    {
        size_t rows_added = 0;
        for (auto current = &static_cast<const typename Mapped::Base_t &>(mapped); current != nullptr; current = current->next)
        {
            for (size_t j = 0; j < num_columns_left; ++j)
                columns_left[j]->insertDefault();

            for (size_t j = 0; j < num_columns_right; ++j)
                columns_right[j]->insertFrom(*current->block->getByPosition(key_num + j).column.get(), current->row_num);
            rows_added++;
        }
        return rows_added;
    }
};


/// Stream from not joined earlier rows of the right table.
class NonJoinedBlockInputStream : public IProfilingBlockInputStream
{
public:
    NonJoinedBlockInputStream(const JoinPtr & parent_, const Block & left_sample_block, size_t index_, size_t step_, size_t max_block_size_)
        : parent(parent_)
        , index(index_)
        , step(step_)
        , max_block_size(max_block_size_)
        , add_not_mapped_rows(true)
    {
        size_t build_concurrency = parent->getBuildConcurrency();
        if (unlikely(step > build_concurrency || index >= build_concurrency))
            throw Exception("The concurrency of NonJoinedBlockInputStream should not be larger than join build concurrency");

        /** left_sample_block contains keys and "left" columns.
          * result_sample_block - keys, "left" columns, and "right" columns.
          */

        size_t num_columns_left = left_sample_block.columns();
        size_t num_columns_right = parent->sample_block_with_columns_to_add.columns();

        result_sample_block = materializeBlock(left_sample_block);

        /// Add columns from the right-side table to the block.
        for (size_t i = 0; i < num_columns_right; ++i)
        {
            const ColumnWithTypeAndName & src_column = parent->sample_block_with_columns_to_add.getByPosition(i);
            result_sample_block.insert(src_column.cloneEmpty());
        }

        column_indices_left.reserve(num_columns_left);
        column_indices_right.reserve(num_columns_right);
        BoolVec is_key_column_in_left_block(num_columns_left, false);

        for (size_t i = 0; i < num_columns_left; ++i)
        {
            column_indices_left.push_back(i);
        }

        for (size_t i = 0; i < num_columns_right; ++i)
            column_indices_right.push_back(num_columns_left + i);

        /// If use_nulls, convert left columns to Nullable.
        if (parent->use_nulls)
        {
            for (size_t i = 0; i < num_columns_left; ++i)
            {
                convertColumnToNullable(result_sample_block.getByPosition(column_indices_left[i]));
            }
        }

        columns_left.resize(num_columns_left);
        columns_right.resize(num_columns_right);
        next_index = index;
    }

    String getName() const override { return "NonJoined"; }

    Block getHeader() const override { return result_sample_block; };


protected:
    Block readImpl() override
    {
        if (parent->blocks.empty())
            return Block();

        if (add_not_mapped_rows)
        {
            setNextCurrentNotMappedRow();
            add_not_mapped_rows = false;
        }

        if (parent->strictness == ASTTableJoin::Strictness::Any)
            return createBlock<ASTTableJoin::Strictness::Any>(parent->maps_any_full);
        else if (parent->strictness == ASTTableJoin::Strictness::All)
            return createBlock<ASTTableJoin::Strictness::All>(parent->maps_all_full);
        else
            throw Exception("Logical error: unknown JOIN strictness (must be ANY or ALL)", ErrorCodes::LOGICAL_ERROR);
    }

private:
    const JoinPtr parent;
    size_t index;
    size_t step;
    size_t max_block_size;
    bool add_not_mapped_rows;
    size_t next_index;

    Block result_sample_block;
    /// Indices of columns in result_sample_block that come from the left-side table (except key columns).
    ColumnNumbers column_indices_left;
    /// Indices of columns that come from the right-side table.
    /// Order is significant: it is the same as the order of columns in the blocks of the right-side table that are saved in parent.blocks.
    ColumnNumbers column_indices_right;
    /// Columns of the current output block corresponding to column_indices_left.
    MutableColumns columns_left;
    /// Columns of the current output block corresponding to column_indices_right.
    MutableColumns columns_right;

    std::unique_ptr<void, std::function<void(void *)>> position; /// type erasure
    size_t current_segment = 0;
    Join::RowRefList * current_not_mapped_row = nullptr;

    void setNextCurrentNotMappedRow()
    {
        while (current_not_mapped_row == nullptr && next_index < parent->rows_not_inserted_to_map.size())
        {
            current_not_mapped_row = parent->rows_not_inserted_to_map[next_index]->next;
            next_index += step;
        }
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    Block createBlock(const Maps & maps)
    {
        size_t num_columns_left = column_indices_left.size();
        size_t num_columns_right = column_indices_right.size();

        for (size_t i = 0; i < num_columns_left; ++i)
        {
            const auto & src_col = result_sample_block.safeGetByPosition(column_indices_left[i]);
            columns_left[i] = src_col.type->createColumn();
        }

        for (size_t i = 0; i < num_columns_right; ++i)
        {
            const auto & src_col = result_sample_block.safeGetByPosition(column_indices_right[i]);
            columns_right[i] = src_col.type->createColumn();
        }

        size_t rows_added = 0;

        switch (parent->type)
        {
#define M(TYPE)                                                                                                             \
    case Join::Type::TYPE:                                                                                                  \
        rows_added = fillColumns<STRICTNESS>(*maps.TYPE, num_columns_left, columns_left, num_columns_right, columns_right); \
        break;
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M

        default:
            throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
        }

        if (!rows_added)
            return {};

        Block res = result_sample_block.cloneEmpty();
        for (size_t i = 0; i < num_columns_left; ++i)
            res.getByPosition(column_indices_left[i]).column = std::move(columns_left[i]);
        for (size_t i = 0; i < num_columns_right; ++i)
            res.getByPosition(column_indices_right[i]).column = std::move(columns_right[i]);

        return res;
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map,
                       size_t num_columns_left,
                       MutableColumns & mutable_columns_left,
                       size_t num_columns_right,
                       MutableColumns & mutable_columns_right)
    {
        size_t rows_added = 0;
        size_t key_num = parent->key_names_right.size();
        while (current_not_mapped_row != nullptr)
        {
            rows_added++;
            for (size_t j = 0; j < num_columns_left; ++j)
                mutable_columns_left[j]->insertDefault();

            for (size_t j = 0; j < num_columns_right; ++j)
                mutable_columns_right[j]->insertFrom(*current_not_mapped_row->block->getByPosition(key_num + j).column.get(),
                                                     current_not_mapped_row->row_num);

            current_not_mapped_row = current_not_mapped_row->next;
            setNextCurrentNotMappedRow();
            if (rows_added == max_block_size)
            {
                return rows_added;
            }
        }

        if (!position)
        {
            current_segment = index;
            position = decltype(position)(
                static_cast<void *>(new typename Map::SegmentType::HashTable::const_iterator(map.getSegmentTable(current_segment).begin())),
                [](void * ptr) { delete reinterpret_cast<typename Map::SegmentType::HashTable::const_iterator *>(ptr); });
        }

        /// use pointer instead of reference because `it` need to be re-assigned latter
        auto it = reinterpret_cast<typename Map::SegmentType::HashTable::const_iterator *>(position.get());
        auto end = map.getSegmentTable(current_segment).end();

        for (; *it != end || current_segment < map.getSegmentSize() - step; ++(*it))
        {
            if (*it == end)
            {
                // move to next internal hash table
                do
                {
                    current_segment += step;
                    position = decltype(position)(
                        static_cast<void *>(new typename Map::SegmentType::HashTable::const_iterator(
                            map.getSegmentTable(current_segment).begin())),
                        [](void * ptr) { delete reinterpret_cast<typename Map::SegmentType::HashTable::const_iterator *>(ptr); });
                    it = reinterpret_cast<typename Map::SegmentType::HashTable::const_iterator *>(position.get());
                    end = map.getSegmentTable(current_segment).end();
                } while (*it == end && current_segment < map.getSegmentSize() - step);
                if (*it == end)
                    break;
            }
            if ((*it)->getMapped().getUsed())
                continue;

            rows_added += AdderNonJoined<STRICTNESS, typename Map::mapped_type>::add((*it)->getMapped(), key_num, num_columns_left, mutable_columns_left, num_columns_right, mutable_columns_right);

            if (rows_added >= max_block_size)
            {
                ++(*it);
                break;
            }
        }
        return rows_added;
    }
};


BlockInputStreamPtr createStreamWithNonJoinedRows(const JoinPtr & parent, const Block & left_sample_block, size_t index, size_t step, size_t max_block_size)
{
    return std::make_shared<NonJoinedBlockInputStream>(parent, left_sample_block, index, step, max_block_size);
}

} // namespace DB
