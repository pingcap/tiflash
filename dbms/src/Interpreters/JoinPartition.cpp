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

#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/FailPoint.h>
#include <Interpreters/JoinPartition.h>
#include <Interpreters/NullAwareSemiJoinHelper.h>
#include <Interpreters/ProbeProcessInfo.h>

#include <ext/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SET_DATA_VARIANT;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char random_fail_in_resize_callback[];
} // namespace FailPoints

namespace
{
template <typename List, typename Elem>
void insertRowToList(List * list, Elem * elem)
{
    elem->next = list->next; // NOLINT(clang-analyzer-core.NullDereference)
    list->next = elem;
}
} // namespace

namespace FailPoints
{
extern const char random_join_build_failpoint[];
} // namespace FailPoints

using PointerHelper = PointerTypeColumnHelper<sizeof(void *)>;

void RowsNotInsertToMap::insertRow(Block * stored_block, size_t index, bool need_materialize, Arena & pool)
{
    if (need_materialize)
    {
        if (materialized_columns_vec.empty() || total_size % max_block_size == 0)
            materialized_columns_vec.emplace_back(stored_block->cloneEmptyColumns());

        auto & last_one = materialized_columns_vec.back();
        size_t columns = stored_block->columns();
        RUNTIME_ASSERT(last_one.size() == columns);
        for (size_t i = 0; i < columns; ++i)
            last_one[i]->insertFrom(*stored_block->getByPosition(i).column.get(), index);
    }
    else
    {
        auto * elem = reinterpret_cast<RowRefList *>(pool.alloc(sizeof(RowRefList)));
        new (elem) RowRefList(stored_block, index);
        insertRowToList(&head, elem);
    }
    ++total_size;
}

template <typename Maps>
static void initImpl(Maps & maps, JoinMapMethod method)
{
    switch (method)
    {
    case JoinMapMethod::EMPTY:
        break;
    case JoinMapMethod::CROSS:
        break;

#define M(METHOD)                                                                       \
    case JoinMapMethod::METHOD:                                                         \
        maps.METHOD = std::make_unique<typename decltype(maps.METHOD)::element_type>(); \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Map, typename Maps>
static Map & getMapImpl(Maps & maps, JoinMapMethod method)
{
    void * ret = nullptr;
    switch (method)
    {
    case JoinMapMethod::EMPTY:
    case JoinMapMethod::CROSS:
        throw Exception("Should not reach here");

#define M(METHOD)                \
    case JoinMapMethod::METHOD:  \
        ret = maps.METHOD.get(); \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
    return *reinterpret_cast<Map *>(ret);
}

template <typename Maps>
static size_t getRowCountImpl(const Maps & maps, JoinMapMethod method)
{
    switch (method)
    {
    case JoinMapMethod::EMPTY:
        return 0;
    case JoinMapMethod::CROSS:
        return 0;

#define M(NAME)               \
    case JoinMapMethod::NAME: \
        return maps.NAME ? maps.NAME->size() : 0;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Maps>
static size_t getByteCountImpl(const Maps & maps, JoinMapMethod method)
{
    switch (method)
    {
    case JoinMapMethod::EMPTY:
        return 0;
    case JoinMapMethod::CROSS:
        return 0;

#define M(NAME)               \
    case JoinMapMethod::NAME: \
        return maps.NAME ? maps.NAME->getBufferSizeInBytes() : 0;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Maps>
static void setResizeCallbackImpl(const Maps & maps, JoinMapMethod method, const ResizeCallback & resize_callback)
{
    switch (method)
    {
    case JoinMapMethod::EMPTY:
    case JoinMapMethod::CROSS:
        return;
#define M(NAME)                                            \
    case JoinMapMethod::NAME:                              \
        if (maps.NAME)                                     \
            maps.NAME->setResizeCallback(resize_callback); \
        return;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Maps>
static size_t clearMaps(Maps & maps, JoinMapMethod method)
{
    size_t ret = 0;
    switch (method)
    {
    case JoinMapMethod::EMPTY:
    case JoinMapMethod::CROSS:
        ret = 0;
        break;
#define M(NAME)                                      \
    case JoinMapMethod::NAME:                        \
        if (maps.NAME)                               \
        {                                            \
            ret = maps.NAME->getBufferSizeInBytes(); \
            maps.NAME.reset();                       \
        }                                            \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
    return ret;
}

size_t JoinPartition::getRowCount()
{
    size_t ret = 0;
    ret += getRowCountImpl(maps_any, join_map_method);
    ret += getRowCountImpl(maps_all, join_map_method);
    ret += getRowCountImpl(maps_any_full, join_map_method);
    ret += getRowCountImpl(maps_all_full, join_map_method);
    ret += getRowCountImpl(maps_all_full_with_row_flag, join_map_method);
    return ret;
}

void JoinPartition::updateHashMapAndPoolMemoryUsage()
{
    hash_table_pool_memory_usage = getHashMapAndPoolByteCount();
}

size_t JoinPartition::getHashMapAndPoolByteCount()
{
    size_t ret = 0;
    ret += getByteCountImpl(maps_any, join_map_method);
    ret += getByteCountImpl(maps_all, join_map_method);
    ret += getByteCountImpl(maps_any_full, join_map_method);
    ret += getByteCountImpl(maps_all_full, join_map_method);
    ret += getByteCountImpl(maps_all_full_with_row_flag, join_map_method);
    ret += pool->size();
    return ret;
}

void JoinPartition::setResizeCallbackIfNeeded()
{
    if (hash_join_spill_context->isSpillEnabled() && hash_join_spill_context->isInAutoSpillMode())
    {
        auto resize_callback = [this]() {
            if (hash_join_spill_context->supportFurtherSpill()
                && hash_join_spill_context->isPartitionMarkedForAutoSpill(partition_index))
                return false;
            bool ret = true;
            fiu_do_on(FailPoints::random_fail_in_resize_callback, {
                if (hash_join_spill_context->supportFurtherSpill())
                {
                    ret = !hash_join_spill_context->markPartitionForAutoSpill(partition_index);
                }
            });
            return ret;
        };
        assert(pool != nullptr);
        pool->setResizeCallback(resize_callback);
        setResizeCallbackImpl(maps_any, join_map_method, resize_callback);
        setResizeCallbackImpl(maps_all, join_map_method, resize_callback);
        setResizeCallbackImpl(maps_any_full, join_map_method, resize_callback);
        setResizeCallbackImpl(maps_all_full, join_map_method, resize_callback);
        setResizeCallbackImpl(maps_all_full_with_row_flag, join_map_method, resize_callback);
    }
}

void JoinPartition::initMap()
{
    if (isCrossJoin(kind))
        return;

    if (isNecessaryKindToUseRowFlaggedHashMap(kind))
    {
        if (has_other_condition)
            initImpl(maps_all_full_with_row_flag, join_map_method);
        else
            initImpl(maps_all_full, join_map_method);
    }
    else if (!getFullness(kind))
    {
        if (strictness == ASTTableJoin::Strictness::Any)
            initImpl(maps_any, join_map_method);
        else
            initImpl(maps_all, join_map_method);
    }
    else
    {
        if (strictness == ASTTableJoin::Strictness::Any)
            initImpl(maps_any_full, join_map_method);
        else
            initImpl(maps_all_full, join_map_method);
    }
}

void JoinPartition::insertBlockForBuild(Block && block)
{
    size_t rows = block.rows();
    size_t bytes = block.estimateBytesForSpill();
    build_partition.rows += rows;
    build_partition.bytes += bytes;
    build_partition.blocks.push_back(block);
    build_partition.original_blocks.push_back(std::move(block));
    addBlockDataMemoryUsage(bytes);
}

void JoinPartition::insertBlockForProbe(Block && block)
{
    size_t rows = block.rows();
    size_t bytes = block.estimateBytesForSpill();
    probe_partition.rows += rows;
    probe_partition.bytes += bytes;
    probe_partition.blocks.push_back(std::move(block));
    addBlockDataMemoryUsage(bytes);
}
std::unique_lock<std::mutex> JoinPartition::lockPartition()
{
    return std::unique_lock(partition_mutex);
}
std::unique_lock<std::mutex> JoinPartition::tryLockPartition()
{
    return std::unique_lock(partition_mutex, std::try_to_lock);
}
void JoinPartition::releaseBuildPartitionBlocks(std::unique_lock<std::mutex> &)
{
    build_partition.bytes = 0;
    build_partition.rows = 0;
    build_partition.blocks.clear();
    build_partition.original_blocks.clear();
    block_data_memory_usage = 0;
}
void JoinPartition::releaseProbePartitionBlocks(std::unique_lock<std::mutex> &)
{
    probe_partition.bytes = 0;
    probe_partition.rows = 0;
    probe_partition.blocks.clear();
    block_data_memory_usage = 0;
}

void JoinPartition::releasePartitionPoolAndHashMap(std::unique_lock<std::mutex> &)
{
    pool.reset();
    clearMaps(maps_any, join_map_method);
    clearMaps(maps_all, join_map_method);
    clearMaps(maps_any_full, join_map_method);
    clearMaps(maps_all_full, join_map_method);
    clearMaps(maps_all_full_with_row_flag, join_map_method);
    LOG_DEBUG(log, "release {} memories from partition {}", hash_table_pool_memory_usage, partition_index);
    hash_table_pool_memory_usage = 0;
}

Blocks JoinPartition::trySpillBuildPartition(std::unique_lock<std::mutex> & partition_lock)
{
    if (isSpill() && build_partition.rows > 0)
    {
        auto ret = build_partition.original_blocks;
        releaseBuildPartitionBlocks(partition_lock);
        return ret;
    }
    else
    {
        return {};
    }
}
Blocks JoinPartition::trySpillProbePartition(std::unique_lock<std::mutex> & partition_lock)
{
    if (isSpill() && probe_partition.rows > 0)
    {
        auto ret = probe_partition.blocks;
        releaseProbePartitionBlocks(partition_lock);
        return ret;
    }
    else
        return {};
}
namespace
{
/// code for hash map insertion
template <JoinMapMethod method, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::key_strbinpadding, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodStringBin<Value, Mapped, true>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::key_strbin, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodStringBin<Value, Mapped, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false>;
};
template <typename Value, typename Mapped>
struct KeyGetterForTypeImpl<JoinMapMethod::serialized, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodSerialized<Value, Mapped>;
};


template <JoinMapMethod method, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<method, Value, Mapped>::Type;
};

/// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
template <ASTTableJoin::Strictness STRICTNESS, typename Map, typename KeyGetter>
struct Inserter
{
    static void insert(
        Map & map,
        const typename Map::key_type & key,
        Block * stored_block,
        size_t i,
        Arena & pool,
        std::vector<String> & sort_key_containers);
};

template <typename Map, typename KeyGetter>
struct Inserter<ASTTableJoin::Strictness::Any, Map, KeyGetter>
{
    static void insert(
        Map & map,
        KeyGetter & key_getter,
        Block * stored_block,
        size_t i,
        Arena & pool,
        std::vector<String> & sort_key_container)
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
    static void insert(
        Map & map,
        KeyGetter & key_getter,
        Block * stored_block,
        size_t i,
        Arena & pool,
        std::vector<String> & sort_key_container)
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
            new (elem) typename Map::mapped_type(stored_block, i);
            insertRowToList(&emplace_result.getMapped(), elem);
        }
    }
};

/// insert Block into one map, don't need acquire lock inside this function
template <
    ASTTableJoin::Strictness STRICTNESS,
    typename KeyGetter,
    typename Map,
    bool has_null_map,
    bool need_record_not_insert_rows>
void NO_INLINE insertBlockIntoMapTypeCase(
    JoinPartition & join_partition,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    Block * stored_block,
    ConstNullMapPtr null_map,
    RowsNotInsertToMap * rows_not_inserted_to_map)
{
    auto & pool = *join_partition.getPartitionPool();

    Map & map = join_partition.template getHashMap<Map>();
    KeyGetter key_getter(key_columns, key_sizes, collators);
    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(key_columns.size());

    bool null_need_materialize = isNullAwareSemiFamily(join_partition.getJoinKind());
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_null_map)
        {
            if ((*null_map)[i])
            {
                if constexpr (need_record_not_insert_rows)
                {
                    /// for right/full out join or null-aware semi join, need to insert into rows_not_inserted_to_map
                    rows_not_inserted_to_map->insertRow(stored_block, i, null_need_materialize, pool);
                }
                continue;
            }
        }

        Inserter<STRICTNESS, Map, KeyGetter>::insert(map, key_getter, stored_block, i, pool, sort_key_containers);
    }
}

/// insert Block into maps, for each map, need to acquire lock before insert
template <
    ASTTableJoin::Strictness STRICTNESS,
    typename KeyGetter,
    typename Map,
    bool has_null_map,
    bool need_record_not_insert_rows>
void NO_INLINE insertBlockIntoMapsTypeCase(
    JoinPartitions & join_partitions,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    Block * stored_block,
    ConstNullMapPtr null_map,
    size_t stream_index,
    RowsNotInsertToMap * rows_not_inserted_to_map)
{
    auto & current_join_partition = join_partitions[stream_index];
    auto & pool = *current_join_partition->getPartitionPool();

    /// use this map to calculate key hash
    auto & map = current_join_partition->getHashMap<Map>();
    KeyGetter key_getter(key_columns, key_sizes, collators);
    std::vector<std::string> sort_key_containers(key_columns.size());
    /// note this implicitly assume that each join partition has only one hash table
    size_t segment_size = join_partitions.size();
    /// when inserting with lock, first calculate and save the segment index for each row, then
    /// insert the rows segment by segment to avoid too much conflict. This will introduce some overheads:
    /// 1. key_getter.getKey will be called twice, here we do not cache key because it can not be cached
    /// with relatively low cost(if key is stringRef, just cache a stringRef is meaningless, we need to cache the whole `sort_key_containers`)
    /// 2. hash value is calculated twice, maybe we can refine the code to cache the hash value
    /// 3. extra memory to store the segment index info
    std::vector<std::vector<size_t>> segment_index_info;
    if constexpr (has_null_map && need_record_not_insert_rows)
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
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_null_map)
        {
            if ((*null_map)[i])
            {
                if constexpr (need_record_not_insert_rows)
                    segment_index_info.back().push_back(i);
                continue;
            }
        }
        auto key_holder = key_getter.getKeyHolder(i, &pool, sort_key_containers);
        SCOPE_EXIT(keyHolderDiscardKey(key_holder));
        auto key = keyHolderGetKey(key_holder);

        size_t segment_index = 0;
        if (!ZeroTraits::check(key))
        {
            size_t hash_value = map.hash(key);
            segment_index = hash_value % segment_size;
        }
        segment_index_info[segment_index].push_back(i);
    }

    std::list<size_t> insert_indexes;
    for (size_t i = 0; i < segment_index_info.size(); ++i)
    {
        size_t insert_index = (i + stream_index) % segment_index_info.size();
        insert_indexes.emplace_back(insert_index);
    }

#define INSERT_TO_MAP(join_partition, segment_index)          \
    auto & current_map = (join_partition)->getHashMap<Map>(); \
    for (auto & s_i : (segment_index))                        \
    {                                                         \
        Inserter<STRICTNESS, Map, KeyGetter>::insert(         \
            current_map,                                      \
            key_getter,                                       \
            stored_block,                                     \
            s_i,                                              \
            pool,                                             \
            sort_key_containers);                             \
    }

#define INSERT_TO_NOT_INSERTED_MAP                                                                      \
    /* null value */                                                                                    \
    /* here ignore mutex because rows_not_inserted_to_map is privately owned by each stream thread */   \
    /* for right/full out join or null-aware semi join, need to insert into rows_not_inserted_to_map */ \
    assert(rows_not_inserted_to_map != nullptr);                                                        \
    assert(segment_index_info.size() == (1 + segment_size));                                            \
    bool null_need_materialize = isNullAwareSemiFamily(current_join_partition->getJoinKind());          \
    for (auto index : segment_index_info[segment_size])                                                 \
    {                                                                                                   \
        rows_not_inserted_to_map->insertRow(stored_block, index, null_need_materialize, pool);          \
    }

    // First use tryLock to traverse twice to find all segments that can acquire locks immediately and execute insert.
    //
    // If there is only one segment left, there is no need to use try locks
    // since it only causes unnecessary CPU consumption, and a blocking lock can be used directly.
    for (size_t i = 0; i <= 1 && insert_indexes.size() > 1; ++i)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_build_failpoint);
        for (auto it = insert_indexes.begin(); it != insert_indexes.end();)
        {
            auto segment_index = *it;
            if (segment_index == segment_size)
            {
                INSERT_TO_NOT_INSERTED_MAP
                it = insert_indexes.erase(it);
            }
            else
            {
                auto & join_partition = join_partitions[segment_index];
                if (auto try_lock = join_partition->tryLockPartition(); try_lock)
                {
                    INSERT_TO_MAP(join_partition, segment_index_info[segment_index]);
                    it = insert_indexes.erase(it);
                }
                else
                {
                    ++it;
                }
            }
        }
    }

    // Next use blocking locks to insert the remaining segments to avoid unnecessary cpu consumption.
    for (auto segment_index : insert_indexes)
    {
        // When segment_index is segment_size, it must be processed in first step.
        RUNTIME_CHECK_MSG(
            segment_index < segment_size,
            "Internal Error: When segment_index is segment_size, it must be processed in first step.");
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_build_failpoint);
        auto & join_partition = join_partitions[segment_index];
        auto lock = join_partition->lockPartition();
        INSERT_TO_MAP(join_partition, segment_index_info[segment_index]);
    }

#undef INSERT_TO_MAP
#undef INSERT_TO_NOT_INSERTED_MAP
}

template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
void insertBlockIntoMapsImplType(
    JoinPartitions & join_partitions,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    Block * stored_block,
    ConstNullMapPtr null_map,
    size_t stream_index,
    size_t insert_concurrency,
    bool enable_fine_grained_shuffle,
    bool enable_join_spill)
{
    auto & current_join_partition = join_partitions[stream_index];
    auto * rows_not_inserted_to_map = current_join_partition->getRowsNotInsertedToMap();
    if (enable_join_spill)
    {
        /// case 1, join with spill support, the partition level lock is acquired in `Join::insertFromBlock`
        if (null_map)
        {
            if (rows_not_inserted_to_map)
                insertBlockIntoMapTypeCase<STRICTNESS, KeyGetter, Map, true, true>(
                    *current_join_partition,
                    rows,
                    key_columns,
                    key_sizes,
                    collators,
                    stored_block,
                    null_map,
                    rows_not_inserted_to_map);
            else
                insertBlockIntoMapTypeCase<STRICTNESS, KeyGetter, Map, true, false>(
                    *current_join_partition,
                    rows,
                    key_columns,
                    key_sizes,
                    collators,
                    stored_block,
                    null_map,
                    nullptr);
        }
        else
        {
            insertBlockIntoMapTypeCase<STRICTNESS, KeyGetter, Map, false, false>(
                *current_join_partition,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                nullptr);
        }
        return;
    }
    else if (enable_fine_grained_shuffle)
    {
        /// case 2, join with fine_grained_shuffle, no need to acquire any lock
        if (null_map)
        {
            if (rows_not_inserted_to_map)
                insertBlockIntoMapTypeCase<STRICTNESS, KeyGetter, Map, true, true>(
                    *current_join_partition,
                    rows,
                    key_columns,
                    key_sizes,
                    collators,
                    stored_block,
                    null_map,
                    rows_not_inserted_to_map);
            else
                insertBlockIntoMapTypeCase<STRICTNESS, KeyGetter, Map, true, false>(
                    *current_join_partition,
                    rows,
                    key_columns,
                    key_sizes,
                    collators,
                    stored_block,
                    null_map,
                    nullptr);
        }
        else
        {
            insertBlockIntoMapTypeCase<STRICTNESS, KeyGetter, Map, false, false>(
                *current_join_partition,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                nullptr);
        }
    }
    else if (insert_concurrency > 1)
    {
        /// case 3, normal join with concurrency > 1, will acquire lock in `insertBlockIntoMapsTypeCase`
        if (null_map)
        {
            if (rows_not_inserted_to_map)
                insertBlockIntoMapsTypeCase<STRICTNESS, KeyGetter, Map, true, true>(
                    join_partitions,
                    rows,
                    key_columns,
                    key_sizes,
                    collators,
                    stored_block,
                    null_map,
                    stream_index,
                    rows_not_inserted_to_map);
            else
                insertBlockIntoMapsTypeCase<STRICTNESS, KeyGetter, Map, true, false>(
                    join_partitions,
                    rows,
                    key_columns,
                    key_sizes,
                    collators,
                    stored_block,
                    null_map,
                    stream_index,
                    nullptr);
        }
        else
        {
            insertBlockIntoMapsTypeCase<STRICTNESS, KeyGetter, Map, false, false>(
                join_partitions,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                stream_index,
                nullptr);
        }
    }
    else
    {
        /// case 4, normal join with concurrency == 1, no need to acquire any lock
        RUNTIME_CHECK(stream_index == 0);
        if (null_map)
        {
            if (rows_not_inserted_to_map)
                insertBlockIntoMapTypeCase<STRICTNESS, KeyGetter, Map, true, true>(
                    *current_join_partition,
                    rows,
                    key_columns,
                    key_sizes,
                    collators,
                    stored_block,
                    null_map,
                    rows_not_inserted_to_map);
            else
                insertBlockIntoMapTypeCase<STRICTNESS, KeyGetter, Map, true, false>(
                    *current_join_partition,
                    rows,
                    key_columns,
                    key_sizes,
                    collators,
                    stored_block,
                    null_map,
                    nullptr);
        }
        else
        {
            insertBlockIntoMapTypeCase<STRICTNESS, KeyGetter, Map, false, false>(
                *current_join_partition,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                nullptr);
        }
    }
}

template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
void insertBlockIntoMapsImpl(
    JoinPartitions & join_partitions,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    Block * stored_block,
    ConstNullMapPtr null_map,
    size_t stream_index,
    size_t insert_concurrency,
    bool enable_fine_grained_shuffle,
    bool enable_join_spill)
{
    switch (join_partitions[stream_index]->getJoinMapMethod())
    {
    case JoinMapMethod::EMPTY:
        break;
    case JoinMapMethod::CROSS:
        break; /// Do nothing. We have already saved block, and it is enough.

#define M(METHOD)                                                                                \
    case JoinMapMethod::METHOD:                                                                  \
        insertBlockIntoMapsImplType<                                                             \
            STRICTNESS,                                                                          \
            typename KeyGetterForType<JoinMapMethod::METHOD, typename Maps::METHOD##Type>::Type, \
            typename Maps::METHOD##Type>(                                                        \
            join_partitions,                                                                     \
            rows,                                                                                \
            key_columns,                                                                         \
            key_sizes,                                                                           \
            collators,                                                                           \
            stored_block,                                                                        \
            null_map,                                                                            \
            stream_index,                                                                        \
            insert_concurrency,                                                                  \
            enable_fine_grained_shuffle,                                                         \
            enable_join_spill);                                                                  \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}
} // namespace

template <typename Map>
Map & JoinPartition::getHashMap()
{
    assert(!isSpill());
    if (isNecessaryKindToUseRowFlaggedHashMap(kind))
    {
        if (has_other_condition)
            return getMapImpl<Map>(maps_all_full_with_row_flag, join_map_method);
        else
            return getMapImpl<Map>(maps_all_full, join_map_method);
    }
    else if (getFullness(kind))
    {
        if (strictness == ASTTableJoin::Strictness::Any)
            return getMapImpl<Map>(maps_any_full, join_map_method);
        else
            return getMapImpl<Map>(maps_all_full, join_map_method);
    }
    else
    {
        if (strictness == ASTTableJoin::Strictness::Any)
            return getMapImpl<Map>(maps_any, join_map_method);
        else
            return getMapImpl<Map>(maps_all, join_map_method);
    }
}

void JoinPartition::insertBlockIntoMaps(
    JoinPartitions & join_partitions,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const std::vector<size_t> & key_sizes,
    const TiDB::TiDBCollators & collators,
    Block * stored_block,
    ConstNullMapPtr & null_map,
    size_t stream_index,
    size_t insert_concurrency,
    bool enable_fine_grained_shuffle,
    bool enable_join_spill)
{
    auto & current_join_partition = join_partitions[stream_index];
    assert(!current_join_partition->isSpill());
    auto current_kind = current_join_partition->kind;
    if (isNullAwareSemiFamily(current_kind))
    {
        if (current_join_partition->strictness == ASTTableJoin::Strictness::Any)
            insertBlockIntoMapsImpl<ASTTableJoin::Strictness::Any, MapsAny>(
                join_partitions,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                stream_index,
                insert_concurrency,
                enable_fine_grained_shuffle,
                enable_join_spill);
        else
            insertBlockIntoMapsImpl<ASTTableJoin::Strictness::All, MapsAll>(
                join_partitions,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                stream_index,
                insert_concurrency,
                enable_fine_grained_shuffle,
                enable_join_spill);
    }
    else if (isNecessaryKindToUseRowFlaggedHashMap(current_kind))
    {
        if (current_join_partition->has_other_condition)
            insertBlockIntoMapsImpl<ASTTableJoin::Strictness::All, MapsAllFullWithRowFlag>(
                join_partitions,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                stream_index,
                insert_concurrency,
                enable_fine_grained_shuffle,
                enable_join_spill);
        else
            insertBlockIntoMapsImpl<ASTTableJoin::Strictness::All, MapsAllFull>(
                join_partitions,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                stream_index,
                insert_concurrency,
                enable_fine_grained_shuffle,
                enable_join_spill);
    }
    else if (getFullness(current_kind))
    {
        if (current_join_partition->strictness == ASTTableJoin::Strictness::Any)
            insertBlockIntoMapsImpl<ASTTableJoin::Strictness::Any, MapsAnyFull>(
                join_partitions,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                stream_index,
                insert_concurrency,
                enable_fine_grained_shuffle,
                enable_join_spill);
        else
            insertBlockIntoMapsImpl<ASTTableJoin::Strictness::All, MapsAllFull>(
                join_partitions,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                stream_index,
                insert_concurrency,
                enable_fine_grained_shuffle,
                enable_join_spill);
    }
    else
    {
        if (current_join_partition->strictness == ASTTableJoin::Strictness::Any)
            insertBlockIntoMapsImpl<ASTTableJoin::Strictness::Any, MapsAny>(
                join_partitions,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                stream_index,
                insert_concurrency,
                enable_fine_grained_shuffle,
                enable_join_spill);
        else
            insertBlockIntoMapsImpl<ASTTableJoin::Strictness::All, MapsAll>(
                join_partitions,
                rows,
                key_columns,
                key_sizes,
                collators,
                stored_block,
                null_map,
                stream_index,
                insert_concurrency,
                enable_fine_grained_shuffle,
                enable_join_spill);
    }
}
namespace
{
/// code for hash map probe
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Map>
struct Adder;

/// code for row flagged hash map probe
template <typename Map>
struct RowFlaggedHashMapAdder;

template <typename Map>
struct Adder<ASTTableJoin::Kind::LeftOuter, ASTTableJoin::Strictness::Any, Map>
{
    static bool addFound(
        const typename Map::ConstLookupResult & it,
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t /*i*/,
        IColumn::Filter * /*filter*/,
        IColumn::Offset & /*current_offset*/,
        IColumn::Offsets * /*offsets*/,
        const std::vector<size_t> & right_indexes,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertFrom(
                *it->getMapped().block->getByPosition(right_indexes[j]).column.get(),
                it->getMapped().row_num);
        return false;
    }

    static bool addNotFound(
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t /*i*/,
        IColumn::Filter * /*filter*/,
        IColumn::Offset & /*current_offset*/,
        IColumn::Offsets * /*offsets*/,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertDefault();
        return false;
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any, Map>
{
    static bool addFound(
        const typename Map::ConstLookupResult & it,
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t i,
        IColumn::Filter * filter,
        IColumn::Offset & /*current_offset*/,
        IColumn::Offsets * /*offsets*/,
        const std::vector<size_t> & right_indexes,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        (*filter)[i] = 1;

        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertFrom(
                *it->getMapped().block->getByPosition(right_indexes[j]).column.get(),
                it->getMapped().row_num);

        return false;
    }

    static bool addNotFound(
        size_t /*num_columns_to_add*/,
        MutableColumns & /*added_columns*/,
        size_t i,
        IColumn::Filter * filter,
        IColumn::Offset & /*current_offset*/,
        IColumn::Offsets * /*offsets*/,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        (*filter)[i] = 0;
        return false;
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::Anti, ASTTableJoin::Strictness::Any, Map>
{
    static bool addFound(
        const typename Map::ConstLookupResult & /*it*/,
        size_t /*num_columns_to_add*/,
        MutableColumns & /*added_columns*/,
        size_t i,
        IColumn::Filter * filter,
        IColumn::Offset & /*current_offset*/,
        IColumn::Offsets * /*offsets*/,
        const std::vector<size_t> & /*right_indexes*/,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        (*filter)[i] = 0;
        return false;
    }

    static bool addNotFound(
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t i,
        IColumn::Filter * filter,
        IColumn::Offset & /*current_offset*/,
        IColumn::Offsets * /*offsets*/,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        (*filter)[i] = 1;
        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertDefault();
        return false;
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::LeftOuterSemi, ASTTableJoin::Strictness::Any, Map>
{
    static bool addFound(
        const typename Map::ConstLookupResult & /*it*/,
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t /*i*/,
        IColumn::Filter * /*filter*/,
        IColumn::Offset & /*current_offset*/,
        IColumn::Offsets * /*offsets*/,
        const std::vector<size_t> & /*right_indexes*/,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        for (size_t j = 0; j < num_columns_to_add - 1; ++j)
            added_columns[j]->insertDefault();
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_1);
        return false;
    }

    static bool addNotFound(
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t /*i*/,
        IColumn::Filter * /*filter*/,
        IColumn::Offset & /*current_offset*/,
        IColumn::Offsets * /*offsets*/,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        for (size_t j = 0; j < num_columns_to_add - 1; ++j)
            added_columns[j]->insertDefault();
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_0);
        return false;
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::LeftOuterSemi, ASTTableJoin::Strictness::All, Map>
{
    static bool addFound(
        const typename Map::ConstLookupResult & it,
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t i,
        IColumn::Filter * /*filter*/,
        IColumn::Offset & current_offset,
        IColumn::Offsets * offsets,
        const std::vector<size_t> & right_indexes,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped());
             current != nullptr;
             current = current->next)
        {
            for (size_t j = 0; j < num_columns_to_add - 1; ++j)
                added_columns[j]->insertFrom(
                    *current->block->getByPosition(right_indexes[j]).column.get(),
                    current->row_num);
            ++current_offset;
        }
        (*offsets)[i] = current_offset;
        /// we insert only one row to `match-helper` for each row of left block
        /// so before the execution of `HandleOtherConditions`, column sizes of temporary block may be different.
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_1);
        return false;
    }

    static bool addNotFound(
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t i,
        IColumn::Filter * /*filter*/,
        IColumn::Offset & current_offset,
        IColumn::Offsets * offsets,
        ProbeProcessInfo & /*probe_process_info*/)
    {
        ++current_offset;
        (*offsets)[i] = current_offset;

        for (size_t j = 0; j < num_columns_to_add - 1; ++j)
            added_columns[j]->insertDefault();
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_0);
        return false;
    }
};

template <ASTTableJoin::Kind KIND, typename Map>
struct Adder<KIND, ASTTableJoin::Strictness::All, Map>
{
    static_assert(KIND != ASTTableJoin::Kind::RightSemi && KIND != ASTTableJoin::Kind::RightAnti);
    static bool addFound(
        const typename Map::ConstLookupResult & it,
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t i,
        IColumn::Filter * filter,
        IColumn::Offset & current_offset,
        IColumn::Offsets * offsets,
        const std::vector<size_t> & right_indexes,
        ProbeProcessInfo & probe_process_info)
    {
        size_t rows_joined = 0;
        // If there are too many rows in the column to split, record the number of rows that have been expanded for next read.
        // and it means the rows in this block are not joined finish.

        for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped());
             current != nullptr;
             current = current->next)
            ++rows_joined;

        if (current_offset && current_offset + rows_joined > probe_process_info.max_block_size)
        {
            return true;
        }

        for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped());
             current != nullptr;
             current = current->next)
        {
            for (size_t j = 0; j < num_columns_to_add; ++j)
                added_columns[j]->insertFrom(
                    *current->block->getByPosition(right_indexes[j]).column.get(),
                    current->row_num);
        }

        current_offset += rows_joined;
        (*offsets)[i] = current_offset;
        if constexpr (KIND == ASTTableJoin::Kind::Anti)
            /// anti join with other condition is very special: if the row is matched during probe stage, we can not throw it
            /// away because it might failed in other condition, so we add the matched rows to the result, but set (*filter)[i] = 0
            /// to indicate that the row is matched during probe stage, this will be used in handleOtherConditions
            (*filter)[i] = 0;

        return false;
    }

    static bool addNotFound(
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t i,
        IColumn::Filter * filter,
        IColumn::Offset & current_offset,
        IColumn::Offsets * offsets,
        ProbeProcessInfo & probe_process_info)
    {
        if constexpr (KIND == ASTTableJoin::Kind::Inner)
        {
            (*offsets)[i] = current_offset;
        }
        else
        {
            if (current_offset && current_offset + 1 > probe_process_info.max_block_size)
            {
                return true;
            }
            if (KIND == ASTTableJoin::Kind::Anti)
                (*filter)[i] = 1;
            ++current_offset;
            (*offsets)[i] = current_offset;

            for (size_t j = 0; j < num_columns_to_add; ++j)
                added_columns[j]->insertDefault();
        }
        return false;
    }
};


template <typename Map>
struct RowFlaggedHashMapAdder
{
    static bool addFound(
        const typename Map::ConstLookupResult & it,
        size_t num_columns_to_add,
        MutableColumns & added_columns,
        size_t i,
        IColumn::Offset & current_offset,
        IColumn::Offsets * offsets,
        const std::vector<size_t> & right_indexes,
        ProbeProcessInfo & probe_process_info,
        MutableColumnPtr & ptr_col)
    {
        size_t rows_joined = 0;
        // If there are too many rows in the column to split, record the number of rows that have been expanded for next read.
        // and it means the rows in this block are not joined finish.

        for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped());
             current != nullptr;
             current = current->next)
            ++rows_joined;

        if (current_offset && current_offset + rows_joined > probe_process_info.max_block_size)
        {
            return true;
        }

        auto & actual_ptr_col = static_cast<PointerHelper::ColumnType &>(*ptr_col);
        auto & container = static_cast<PointerHelper::ArrayType &>(actual_ptr_col.getData());
        for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped());
             current != nullptr;
             current = current->next)
        {
            for (size_t j = 0; j < num_columns_to_add; ++j)
                added_columns[j]->insertFrom(
                    *current->block->getByPosition(right_indexes[j]).column.get(),
                    current->row_num);

            container.template push_back(reinterpret_cast<std::intptr_t>(current));
            ++current_offset;
        }
        (*offsets)[i] = current_offset;
        return false;
    }

    static bool addNotFound(size_t i, IColumn::Offset & current_offset, IColumn::Offsets * offsets)
    {
        (*offsets)[i] = current_offset;
        return false;
    }
};

template <
    ASTTableJoin::Kind KIND,
    ASTTableJoin::Strictness STRICTNESS,
    typename KeyGetter,
    typename Map,
    bool has_null_map,
    bool row_flagged_map>
void NO_INLINE probeBlockImplTypeCase(
    const JoinPartitions & join_partitions,
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
    const JoinBuildInfo & join_build_info,
    ProbeProcessInfo & probe_process_info,
    MutableColumnPtr & record_mapped_entry_column)
{
    if (rows == 0)
    {
        probe_process_info.all_rows_joined_finish = true;
        return;
    }

    assert(probe_process_info.start_row < rows);
    size_t segment_size = join_partitions.size();
    assert(segment_size > 0);
    std::vector<Map *> all_maps(segment_size, nullptr);
    for (size_t i = 0; i < segment_size; ++i)
    {
        if (join_partitions[i]->isSpill())
        {
            assert(i != probe_process_info.partition_index);
            all_maps[i] = nullptr;
        }
        else
        {
            all_maps[i] = &join_partitions[i]->template getHashMap<Map>();
        }
    }

    size_t num_columns_to_add = right_indexes.size();

    KeyGetter key_getter(key_columns, key_sizes, collators);
    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(key_columns.size());
    Arena pool;
    WeakHash32 build_hash(0); /// reproduce hash values according to build stage
    if (join_build_info.needVirtualDispatchForProbeBlock())
    {
        assert(!(join_build_info.restore_round > 0 && join_build_info.enable_fine_grained_shuffle));
        /// TODO: consider adding a virtual column in Sender side to avoid computing cost and potential inconsistency by heterogeneous envs(AMD64, ARM64)
        /// Note: 1. Not sure, if inconsistency will do happen in heterogeneous envs
        ///       2. Virtual column would take up a little more network bandwidth, might lead to poor performance if network was bottleneck
        /// Currently, the computation cost is tolerable, since it's a very simple crc32 hash algorithm, and heterogeneous envs support is not considered
        computeDispatchHash(
            rows,
            key_columns,
            collators,
            sort_key_containers,
            join_build_info.restore_round,
            build_hash);
    }

    const auto & build_hash_data = build_hash.getData();
    assert(probe_process_info.start_row < rows);
    size_t i;
    bool block_full = false;
    for (i = probe_process_info.start_row; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            if constexpr (row_flagged_map)
            {
                block_full = RowFlaggedHashMapAdder<Map>::addNotFound(i, current_offset, offsets_to_replicate.get());
            }
            /// RightSemi/RightAnti without other conditions, just ignore not matched probe rows
            else if constexpr (KIND != ASTTableJoin::Kind::RightSemi && KIND != ASTTableJoin::Kind::RightAnti)
            {
                block_full = Adder<KIND, STRICTNESS, Map>::addNotFound(
                    num_columns_to_add,
                    added_columns,
                    i,
                    filter.get(),
                    current_offset,
                    offsets_to_replicate.get(),
                    probe_process_info);
            }
        }
        else
        {
            auto key_holder = key_getter.getKeyHolder(i, &pool, sort_key_containers);
            SCOPE_EXIT(keyHolderDiscardKey(key_holder));
            auto key = keyHolderGetKey(key_holder);

            size_t hash_value = 0;
            bool zero_flag = ZeroTraits::check(key);
            if (!zero_flag)
            {
                hash_value = all_maps[probe_process_info.partition_index]->hash(key);
            }

            size_t segment_index = 0;
            if (join_build_info.is_spilled)
            {
                segment_index = probe_process_info.partition_index;
            }
            else if (join_build_info.needVirtualDispatchForProbeBlock())
            {
                /// Need to calculate the correct segment_index so that rows with same key will map to the same segment_index both in Build and Prob
                /// The "reproduce" of segment_index generated in Build phase relies on the facts that:
                /// Possible pipelines(FineGrainedShuffleWriter => ExchangeReceiver => HashBuild)
                /// 1. In FineGrainedShuffleWriter, selector value finally maps to packet_stream_id by '% fine_grained_shuffle_count'
                /// 2. In ExchangeReceiver, build_stream_id = packet_stream_id % build_stream_count;
                /// 3. In HashBuild, build_concurrency decides map's segment size, and build_steam_id decides the segment index
                if (join_build_info.enable_fine_grained_shuffle)
                {
                    auto packet_stream_id = build_hash_data[i] % join_build_info.fine_grained_shuffle_count;
                    if likely (join_build_info.fine_grained_shuffle_count == segment_size)
                        segment_index = packet_stream_id;
                    else
                        segment_index = packet_stream_id % segment_size;
                }
                else
                {
                    segment_index = build_hash_data[i] % join_build_info.build_concurrency;
                }
            }
            else
            {
                segment_index = hash_value % segment_size;
            }

            auto & internal_map = *all_maps[segment_index];
            /// do not require segment lock because in join, the hash table can not be changed in probe stage.
            auto it = internal_map.find(key, hash_value);
            if (it != internal_map.end())
            {
                if constexpr (row_flagged_map)
                {
                    block_full = RowFlaggedHashMapAdder<Map>::addFound(
                        it,
                        num_columns_to_add,
                        added_columns,
                        i,
                        current_offset,
                        offsets_to_replicate.get(),
                        right_indexes,
                        probe_process_info,
                        record_mapped_entry_column);
                }
                else if constexpr (KIND == ASTTableJoin::Kind::RightSemi || KIND == ASTTableJoin::Kind::RightAnti)
                {
                    /// For RightSemi/RightAnti without other conditions, just flag the hash entry is enough
                    it->getMapped().setUsed();
                }
                else
                {
                    it->getMapped().setUsed();
                    block_full = Adder<KIND, STRICTNESS, Map>::addFound(
                        it,
                        num_columns_to_add,
                        added_columns,
                        i,
                        filter.get(),
                        current_offset,
                        offsets_to_replicate.get(),
                        right_indexes,
                        probe_process_info);
                }
            }
            else
            {
                if constexpr (row_flagged_map)
                {
                    block_full
                        = RowFlaggedHashMapAdder<Map>::addNotFound(i, current_offset, offsets_to_replicate.get());
                }
                /// RightSemi/RightAnti without other conditions, just ignore not matched probe rows
                else if constexpr (KIND != ASTTableJoin::Kind::RightSemi && KIND != ASTTableJoin::Kind::RightAnti)
                {
                    block_full = Adder<KIND, STRICTNESS, Map>::addNotFound(
                        num_columns_to_add,
                        added_columns,
                        i,
                        filter.get(),
                        current_offset,
                        offsets_to_replicate.get(),
                        probe_process_info);
                }
            }
        }

        // if block_full is true means that the current offset is greater than max_block_size, we need break the loop.
        if (block_full)
        {
            break;
        }
    }

    probe_process_info.updateEndRow<false>(i);
}

template <
    ASTTableJoin::Kind KIND,
    ASTTableJoin::Strictness STRICTNESS,
    typename KeyGetter,
    typename Map,
    bool row_flagged_map>
void probeBlockImplType(
    const JoinPartitions & join_partitions,
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
    const JoinBuildInfo & join_build_info,
    ProbeProcessInfo & probe_process_info,
    MutableColumnPtr & record_mapped_entry_column)
{
    if (null_map)
    {
        probeBlockImplTypeCase<KIND, STRICTNESS, KeyGetter, Map, true, row_flagged_map>(
            join_partitions,
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
            join_build_info,
            probe_process_info,
            record_mapped_entry_column);
    }
    else
    {
        probeBlockImplTypeCase<KIND, STRICTNESS, KeyGetter, Map, false, row_flagged_map>(
            join_partitions,
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
            join_build_info,
            probe_process_info,
            record_mapped_entry_column);
    }
}
template <
    ASTTableJoin::Kind KIND,
    ASTTableJoin::Strictness STRICTNESS,
    typename KeyGetter,
    typename Map,
    bool has_null_map,
    bool has_filter_map>
std::pair<PaddedPODArray<NASemiJoinResult<KIND, STRICTNESS>>, std::list<NASemiJoinResult<KIND, STRICTNESS> *>> NO_INLINE
probeBlockNullAwareInternal(
    const JoinPartitions & join_partitions,
    Block & block,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    const NALeftSideInfo & left_side_info,
    const NARightSideInfo & right_side_info)
{
    static_assert(
        KIND == ASTTableJoin::Kind::NullAware_Anti || KIND == ASTTableJoin::Kind::NullAware_LeftOuterAnti
        || KIND == ASTTableJoin::Kind::NullAware_LeftOuterSemi);
    static_assert(STRICTNESS == ASTTableJoin::Strictness::Any || STRICTNESS == ASTTableJoin::Strictness::All);

    size_t rows = block.rows();
    KeyGetter key_getter(key_columns, key_sizes, collators);
    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(key_columns.size());

    Arena pool;
    size_t segment_size = join_partitions.size();

    PaddedPODArray<NASemiJoinResult<KIND, STRICTNESS>> res;
    res.reserve(rows);
    std::list<NASemiJoinResult<KIND, STRICTNESS> *> res_list;
    /// We can just consider the result of left outer semi join because `NASemiJoinResult::setResult` will correct
    /// the result if it's not left outer semi join.
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_filter_map)
        {
            if ((*left_side_info.filter_map)[i])
            {
                /// Filter out by left_conditions so the result set is empty.
                res.emplace_back(i, NASemiJoinStep::DONE, nullptr);
                res.back().template setResult<NASemiJoinResultType::FALSE_VALUE>();
                continue;
            }
        }
        if (right_side_info.is_empty)
        {
            /// If right table is empty, the result is false.
            /// E.g. (1,2) in ().
            res.emplace_back(i, NASemiJoinStep::DONE, nullptr);
            res.back().template setResult<NASemiJoinResultType::FALSE_VALUE>();
            continue;
        }
        if constexpr (has_null_map)
        {
            if ((*left_side_info.null_map)[i])
            {
                if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
                {
                    if (key_columns.size() == 1 || right_side_info.has_all_key_null_row
                        || (left_side_info.all_key_null_map && (*left_side_info.all_key_null_map)[i]))
                    {
                        /// Note that right_table_is_empty must be false here so the right table is not empty.
                        /// The result is NULL if
                        ///   1. key column size is 1. E.g. (null) in (1,2).
                        ///   2. right table has a all-key-null row. E.g. (1,null) in ((2,2),(null,null)).
                        ///   3. this row is all-key-null. E.g. (null,null) in ((1,1),(2,2)).
                        res.emplace_back(i, NASemiJoinStep::DONE, nullptr);
                        res.back().template setResult<NASemiJoinResultType::NULL_VALUE>();
                        continue;
                    }
                }
                /// In the worse case, all rows in the right table will be checked.
                /// E.g. (1,null) in ((2,1),(2,3),(2,null),(3,null)) => false.
                if (right_side_info.null_key_check_all_blocks_directly)
                    res.emplace_back(i, NASemiJoinStep::NULL_KEY_CHECK_ALL_BLOCKS, nullptr);
                else
                    res.emplace_back(i, NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS, nullptr);
                res_list.push_back(&res.back());
                continue;
            }
        }

        auto key_holder = key_getter.getKeyHolder(i, &pool, sort_key_containers);
        SCOPE_EXIT(keyHolderDiscardKey(key_holder));
        auto key = keyHolderGetKey(key_holder);
        /// used to calculate key hash
        Map & map = join_partitions[0]->template getHashMap<Map>();

        size_t segment_index = 0;
        size_t hash_value = 0;
        if (!ZeroTraits::check(key))
        {
            hash_value = map.hash(key);
            segment_index = hash_value % segment_size;
        }

        auto & internal_map = join_partitions[segment_index]->template getHashMap<Map>();
        /// do not require segment lock because in join, the hash table can not be changed in probe stage.
        auto it = internal_map.find(key, hash_value);
        if (it != internal_map.end())
        {
            /// Find the matched row(s).
            if (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                /// If strictness is any, the result is true.
                /// E.g. (1,2) in ((1,2),(1,3),(null,3))
                res.emplace_back(i, NASemiJoinStep::DONE, nullptr);
                res.back().template setResult<NASemiJoinResultType::TRUE_VALUE>();
            }
            else
            {
                /// Else the other condition must be checked for these matched right row(s).
                auto map_it = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped());
                res.emplace_back(i, NASemiJoinStep::NOT_NULL_KEY_CHECK_MATCHED_ROWS, static_cast<const void *>(map_it));
                res_list.push_back(&res.back());
            }
            continue;
        }

        /// Not find the matched row.
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
        {
            if (right_side_info.has_all_key_null_row)
            {
                /// If right table has a all-key-null row, the result is NULL.
                /// E.g. (1) in (null) or (1,2) in ((1,3),(null,null)).
                res.emplace_back(i, NASemiJoinStep::DONE, nullptr);
                res.back().template setResult<NASemiJoinResultType::NULL_VALUE>();
                continue;
            }
            else if (key_columns.size() == 1)
            {
                /// If key size is 1 and all key in right table row is not NULL(right_has_all_key_null_row is false),
                /// the result is false.
                /// E.g. (1) in (2,3,4,5).
                res.emplace_back(i, NASemiJoinStep::DONE, nullptr);
                res.back().template setResult<NASemiJoinResultType::FALSE_VALUE>();
                continue;
            }
            /// Then key size is greater than 1 and right table does not have a all-key-null row.
            /// E.g. (1,2) in ((1,3),(1,null)) => NULL, (1,2) in ((1,3),(2,null)) => false.
        }
        /// We have to compare them to the null rows one by one.
        /// In the worse case, all null rows in the right table will be checked.
        /// E.g. (1,2) in ((2,null),(3,null),(null,1),(null,3)) => false.
        res.emplace_back(i, NASemiJoinStep::NOT_NULL_KEY_CHECK_NULL_ROWS, nullptr);
        res_list.push_back(&res.back());
    }
    return std::make_pair(std::move(res), std::move(res_list));
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
std::pair<PaddedPODArray<NASemiJoinResult<KIND, STRICTNESS>>, std::list<NASemiJoinResult<KIND, STRICTNESS> *>> probeBlockNullAwareType(
    const JoinPartitions & join_partitions,
    Block & block,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const TiDB::TiDBCollators & collators,
    const NALeftSideInfo & left_side_info,
    const NARightSideInfo & right_side_info)
{
#define impl(has_null_map, has_filter_map)                                                              \
    return probeBlockNullAwareInternal<KIND, STRICTNESS, KeyGetter, Map, has_null_map, has_filter_map>( \
        join_partitions,                                                                                \
        block,                                                                                          \
        key_columns,                                                                                    \
        key_sizes,                                                                                      \
        collators,                                                                                      \
        left_side_info,                                                                                 \
        right_side_info);

    if (left_side_info.null_map)
    {
        if (left_side_info.filter_map)
        {
            impl(true, true);
        }
        else
        {
            impl(true, false);
        }
    }
    else
    {
        if (left_side_info.filter_map)
        {
            impl(false, true);
        }
        else
        {
            impl(false, false);
        }
    }
}
} // namespace
void JoinPartition::probeBlock(
    const JoinPartitions & join_partitions,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const std::vector<size_t> & key_sizes,
    MutableColumns & added_columns,
    ConstNullMapPtr null_map,
    std::unique_ptr<IColumn::Filter> & filter,
    IColumn::Offset & current_offset,
    std::unique_ptr<IColumn::Offsets> & offsets_to_replicate,
    const std::vector<size_t> & right_indexes,
    const TiDB::TiDBCollators & collators,
    const JoinBuildInfo & join_build_info,
    ProbeProcessInfo & probe_process_info,
    MutableColumnPtr & record_mapped_entry_column)
{
    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;
    const auto & current_partition = join_partitions[probe_process_info.partition_index];
    auto kind = current_partition->kind;
    auto strictness = current_partition->strictness;
    assert(rows == 0 || !current_partition->isSpill());

#define CALL(KIND, STRICTNESS, MAP, row_flagged_map)        \
    probeBlockImpl<KIND, STRICTNESS, MAP, row_flagged_map>( \
        join_partitions,                                    \
        rows,                                               \
        key_columns,                                        \
        key_sizes,                                          \
        added_columns,                                      \
        null_map,                                           \
        filter,                                             \
        current_offset,                                     \
        offsets_to_replicate,                               \
        right_indexes,                                      \
        collators,                                          \
        join_build_info,                                    \
        probe_process_info,                                 \
        record_mapped_entry_column);

    if (kind == LeftOuter && strictness == Any)
        CALL(LeftOuter, Any, MapsAny, false)
    else if (kind == Inner && strictness == Any)
        CALL(Inner, Any, MapsAny, false)
    else if (kind == LeftOuter && strictness == All)
        CALL(LeftOuter, All, MapsAll, false)
    else if (kind == Inner && strictness == All)
        CALL(Inner, All, MapsAll, false)
    else if (kind == Full && strictness == Any)
        CALL(LeftOuter, Any, MapsAnyFull, false)
    else if (kind == RightOuter && strictness == Any)
        CALL(Inner, Any, MapsAnyFull, false)
    else if (kind == Full && strictness == All)
        CALL(LeftOuter, All, MapsAllFull, false)
    else if (kind == RightOuter && strictness == All && !record_mapped_entry_column)
        CALL(Inner, All, MapsAllFull, false)
    else if (kind == RightOuter && strictness == All && record_mapped_entry_column)
        CALL(RightOuter, All, MapsAllFullWithRowFlag, true)
    else if (kind == Anti && strictness == Any)
        CALL(Anti, Any, MapsAny, false)
    else if (kind == Anti && strictness == All)
        CALL(Anti, All, MapsAll, false)
    else if (kind == LeftOuterSemi && strictness == Any)
        CALL(LeftOuterSemi, Any, MapsAny, false)
    else if (kind == LeftOuterSemi && strictness == All)
        CALL(LeftOuterSemi, All, MapsAll, false)
    else if (kind == LeftOuterAnti && strictness == Any)
        CALL(LeftOuterSemi, Any, MapsAny, false)
    else if (kind == LeftOuterAnti && strictness == All)
        CALL(LeftOuterSemi, All, MapsAll, false)
    else if (kind == RightSemi && record_mapped_entry_column)
        CALL(RightSemi, All, MapsAllFullWithRowFlag, true)
    else if (kind == RightSemi && !record_mapped_entry_column)
        CALL(RightSemi, All, MapsAllFull, false)
    else if (kind == RightAnti && record_mapped_entry_column)
        CALL(RightAnti, All, MapsAllFullWithRowFlag, true)
    else if (kind == RightAnti && !record_mapped_entry_column)
        CALL(RightAnti, All, MapsAllFull, false)
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
#undef CALL
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps, bool row_flagged_map>
void JoinPartition::probeBlockImpl(
    const JoinPartitions & join_partitions,
    size_t rows,
    const ColumnRawPtrs & key_columns,
    const std::vector<size_t> & key_sizes,
    MutableColumns & added_columns,
    ConstNullMapPtr null_map,
    std::unique_ptr<IColumn::Filter> & filter,
    IColumn::Offset & current_offset,
    std::unique_ptr<IColumn::Offsets> & offsets_to_replicate,
    const std::vector<size_t> & right_indexes,
    const TiDB::TiDBCollators & collators,
    const JoinBuildInfo & join_build_info,
    ProbeProcessInfo & probe_process_info,
    MutableColumnPtr & record_mapped_entry_column)
{
    const auto & current_join_partition = join_partitions[probe_process_info.partition_index];
    auto method = current_join_partition->join_map_method;
    switch (method)
    {
#define M(METHOD)                                                                                \
    case JoinMapMethod::METHOD:                                                                  \
        probeBlockImplType<                                                                      \
            KIND,                                                                                \
            STRICTNESS,                                                                          \
            typename KeyGetterForType<JoinMapMethod::METHOD, typename Maps::METHOD##Type>::Type, \
            typename Maps::METHOD##Type,                                                         \
            row_flagged_map>(                                                                    \
            join_partitions,                                                                     \
            rows,                                                                                \
            key_columns,                                                                         \
            key_sizes,                                                                           \
            added_columns,                                                                       \
            null_map,                                                                            \
            filter,                                                                              \
            current_offset,                                                                      \
            offsets_to_replicate,                                                                \
            right_indexes,                                                                       \
            collators,                                                                           \
            join_build_info,                                                                     \
            probe_process_info,                                                                  \
            record_mapped_entry_column);                                                         \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
std::pair<PaddedPODArray<NASemiJoinResult<KIND, STRICTNESS>>, std::list<NASemiJoinResult<KIND, STRICTNESS> *>> JoinPartition::
    probeBlockNullAware(
        const JoinPartitions & join_partitions,
        Block & block,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        const TiDB::TiDBCollators & collators,
        const NALeftSideInfo & left_side_info,
        const NARightSideInfo & right_side_info)
{
    auto method = join_partitions[0]->join_map_method;
    switch (method)
    {
#define M(METHOD)                                                                                \
    case JoinMapMethod::METHOD:                                                                  \
        return probeBlockNullAwareType<                                                          \
            KIND,                                                                                \
            STRICTNESS,                                                                          \
            typename KeyGetterForType<JoinMapMethod::METHOD, typename Maps::METHOD##Type>::Type, \
            typename Maps::METHOD##Type>(                                                        \
            join_partitions,                                                                     \
            block,                                                                               \
            key_columns,                                                                         \
            key_sizes,                                                                           \
            collators,                                                                           \
            left_side_info,                                                                      \
            right_side_info);
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

void JoinPartition::releasePartition()
{
    std::unique_lock partition_lock = lockPartition();
    releaseBuildPartitionBlocks(partition_lock);
    releaseProbePartitionBlocks(partition_lock);
    if (!isSpill())
    {
        releasePartitionPoolAndHashMap(partition_lock);
    }
}

#define M(KIND, STRICTNESS, MAPTYPE)                                                                                \
    template std::                                                                                                  \
        pair<PaddedPODArray<NASemiJoinResult<KIND, (STRICTNESS)>>, std::list<NASemiJoinResult<KIND, STRICTNESS> *>> \
        JoinPartition::probeBlockNullAware<KIND, STRICTNESS, MAPTYPE>(                                              \
            const JoinPartitions & join_partitions,                                                                 \
            Block & block,                                                                                          \
            const ColumnRawPtrs & key_columns,                                                                      \
            const Sizes & key_sizes,                                                                                \
            const TiDB::TiDBCollators & collators,                                                                  \
            const NALeftSideInfo & left_side_info,                                                                  \
            const NARightSideInfo & right_side_info);

APPLY_FOR_NULL_AWARE_JOIN(M)
#undef M

} // namespace DB
