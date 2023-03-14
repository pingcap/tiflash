// Copyright 2023 PingCAP, Ltd.
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
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/NonJoinedBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Mpp/HashBaseWriterHelper.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Join.h>
#include <Interpreters/NullAwareSemiJoinHelper.h>
#include <Interpreters/NullableUtils.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>


namespace DB
{
namespace FailPoints
{
extern const char random_join_build_failpoint[];
extern const char random_join_prob_failpoint[];
extern const char exception_mpp_hash_build[];
extern const char exception_mpp_hash_probe[];
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
/// (cartesian/null-aware) (anti) left semi join.
bool isLeftSemiFamily(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::LeftSemi || kind == ASTTableJoin::Kind::LeftAnti
        || kind == ASTTableJoin::Kind::Cross_LeftSemi || kind == ASTTableJoin::Kind::Cross_LeftAnti
        || kind == ASTTableJoin::Kind::NullAware_LeftSemi || kind == ASTTableJoin::Kind::NullAware_LeftAnti;
}
bool isNullAwareSemiFamily(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::NullAware_Anti || kind == ASTTableJoin::Kind::NullAware_LeftAnti
        || kind == ASTTableJoin::Kind::NullAware_LeftSemi;
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
SpillConfig createSpillConfigWithNewSpillId(const SpillConfig & config, const String & new_spill_id)
{
    return SpillConfig(config.spill_dir, new_spill_id, config.max_cached_data_bytes_in_spiller, config.max_spilled_rows_per_file, config.max_spilled_bytes_per_file, config.file_provider);
}
size_t getRestoreJoinBuildConcurrency(size_t total_partitions, size_t spilled_partitions, Int64 join_restore_concurrency, size_t total_concurrency)
{
    if (join_restore_concurrency < 0)
    {
        /// restore serially, so one restore join will take up all the concurrency
        return total_concurrency;
    }
    else if (join_restore_concurrency > 0)
    {
        /// try to restore `join_restore_concurrency` partition at a time, but restore_join_build_concurrency should be at least 2
        return std::max(2, total_concurrency / join_restore_concurrency);
    }
    else
    {
        assert(total_partitions >= spilled_partitions);
        size_t unspilled_partitions = total_partitions - spilled_partitions;
        /// try to restore at most (unspilled_partitions - 1) partitions at a time
        size_t max_concurrent_restore_partition = unspilled_partitions <= 1 ? 1 : unspilled_partitions - 1;
        size_t restore_times = (spilled_partitions + max_concurrent_restore_partition - 1) / max_concurrent_restore_partition;
        size_t restore_build_concurrency = (restore_times * total_concurrency) / spilled_partitions;
        return std::max(2, restore_build_concurrency);
    }
}
UInt64 inline updateHashValue(size_t restore_round, UInt64 x)
{
    static std::vector<UInt64> hash_constants{0xff51afd7ed558ccdULL, 0xc4ceb9fe1a85ec53ULL, 0xde43a68e4d184aa3ULL, 0x86f1fda459fa47c7ULL, 0xd91419add64f471fULL, 0xc18eea9cbe12489eULL, 0x2cb94f36b9fe4c38ULL, 0xef0f50cc5f0c4cbaULL};
    static size_t hash_constants_size = hash_constants.size();
    assert(hash_constants_size > 0 && (hash_constants_size & (hash_constants_size - 1)) == 0);
    assert(restore_round != 0);
    x ^= x >> 33;
    x *= hash_constants[restore_round & (hash_constants_size - 1)];
    x ^= x >> 33;
    x *= hash_constants[(restore_round + 1) & (hash_constants_size - 1)];
    x ^= x >> 33;
    return x;
}
struct JoinBuildInfo
{
    bool enable_fine_grained_shuffle;
    size_t fine_grained_shuffle_count;
    bool enable_spill;
    bool is_spilled;
    size_t build_concurrency;
    size_t restore_round;
    bool needVirtualDispatchForProbeBlock() const
    {
        return enable_fine_grained_shuffle || (enable_spill && !is_spilled);
    }
};
ColumnRawPtrs extractAndMaterializeKeyColumns(const Block & block, Columns & materialized_columns, const Strings & key_columns_names)
{
    ColumnRawPtrs key_columns(key_columns_names.size());
    for (size_t i = 0; i < key_columns_names.size(); ++i)
    {
        key_columns[i] = block.getByName(key_columns_names[i]).column.get();

        if (ColumnPtr converted = key_columns[i]->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns[i] = materialized_columns.back().get();
        }
    }
    return key_columns;
}

void computeDispatchHash(size_t rows,
                         const ColumnRawPtrs & key_columns,
                         const TiDB::TiDBCollators & collators,
                         std::vector<String> & partition_key_containers,
                         size_t join_restore_round,
                         WeakHash32 & hash)
{
    HashBaseWriterHelper::computeHash(rows, key_columns, collators, partition_key_containers, hash);
    if (join_restore_round != 0)
    {
        auto & data = hash.getData();
        for (size_t i = 0; i < rows; ++i)
            data[i] = updateHashValue(join_restore_round, data[i]);
    }
}
} // namespace

const std::string Join::match_helper_prefix = "__left-semi-join-match-helper";
const DataTypePtr Join::match_helper_type = makeNullable(std::make_shared<DataTypeInt8>());

void convertColumnToNullable(ColumnWithTypeAndName & column)
{
    column.type = makeNullable(column.type);
    if (column.column)
        column.column = makeNullable(column.column);
}

Join::Join(
    const Names & key_names_left_,
    const Names & key_names_right_,
    ASTTableJoin::Kind kind_,
    ASTTableJoin::Strictness strictness_,
    const String & req_id,
    bool enable_fine_grained_shuffle_,
    size_t fine_grained_shuffle_count_,
    size_t max_bytes_before_external_join_,
    const SpillConfig & build_spill_config_,
    const SpillConfig & probe_spill_config_,
    Int64 join_restore_concurrency_,
    const TiDB::TiDBCollators & collators_,
    const String & left_filter_column_,
    const String & right_filter_column_,
    const JoinOtherConditions & other_conditions,
    size_t max_block_size_,
    const String & match_helper_name,
    size_t restore_round_)
    : restore_round(restore_round_)
    , match_helper_name(match_helper_name)
    , kind(kind_)
    , strictness(strictness_)
    , key_names_left(key_names_left_)
    , key_names_right(key_names_right_)
    , build_concurrency(0)
    , active_build_concurrency(0)
    , probe_concurrency(0)
    , active_probe_concurrency(0)
    , collators(collators_)
    , left_filter_column(left_filter_column_)
    , right_filter_column(right_filter_column_)
    , other_conditions(other_conditions)
    , original_strictness(strictness)
    , max_block_size(max_block_size_)
    , max_bytes_before_external_join(max_bytes_before_external_join_)
    , build_spill_config(build_spill_config_)
    , probe_spill_config(probe_spill_config_)
    , join_restore_concurrency(join_restore_concurrency_)
    , log(Logger::get(req_id))
    , enable_fine_grained_shuffle(enable_fine_grained_shuffle_)
    , fine_grained_shuffle_count(fine_grained_shuffle_count_)
{
    if (other_conditions.other_cond_expr != nullptr)
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

    String err = other_conditions.validate(isNullAwareSemiFamily(kind));
    if (unlikely(!err.empty()))
        throw Exception("Validate join conditions error: {}" + err);

    LOG_INFO(log, "FineGrainedShuffle flag {}, stream count {}", enable_fine_grained_shuffle, fine_grained_shuffle_count);
}

void Join::meetError(const String & error_message_)
{
    std::unique_lock lock(build_probe_mutex);
    meetErrorImpl(error_message_, lock);
}

void Join::meetErrorImpl(const String & error_message_, std::unique_lock<std::mutex> &)
{
    if (meet_error)
        return;
    meet_error = true;
    error_message = error_message_.empty() ? "Join meet error" : error_message_;
    build_cv.notify_all();
    probe_cv.notify_all();
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

template <typename Maps>
static size_t getPartitionByteCountImpl(const Maps & maps, Join::Type type, size_t partition_index)
{
    switch (type)
    {
    case Join::Type::EMPTY:
        return 0;
    case Join::Type::CROSS:
        return 0;

#define M(NAME)            \
    case Join::Type::NAME: \
        return maps.NAME ? maps.NAME->getSegmentBufferSizeInBytes(partition_index) : 0;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

template <typename Maps>
static size_t clearMapPartition(const Maps & maps, Join::Type type, size_t partition_index)
{
    size_t ret = 0;
    switch (type)
    {
    case Join::Type::EMPTY:
        ret = 0;
        break;
    case Join::Type::CROSS:
        ret = 0;
        break;

#define M(NAME)                                                  \
    case Join::Type::NAME:                                       \
        if (maps.NAME)                                           \
        {                                                        \
            ret = maps.NAME->resetSegmentTable(partition_index); \
        }                                                        \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
    return ret;
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
            initImpl(maps_any, type, getBuildConcurrency());
        else
            initImpl(maps_all, type, getBuildConcurrency());
    }
    else
    {
        if (strictness == ASTTableJoin::Strictness::Any)
            initImpl(maps_any_full, type, getBuildConcurrency());
        else
            initImpl(maps_all_full, type, getBuildConcurrency());
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

size_t Join::getPartitionByteCount(size_t partition_index) const
{
    size_t ret = 0;
    ret += getPartitionByteCountImpl(maps_any, type, partition_index);
    ret += getPartitionByteCountImpl(maps_all, type, partition_index);
    ret += getPartitionByteCountImpl(maps_any_full, type, partition_index);
    ret += getPartitionByteCountImpl(maps_all_full, type, partition_index);
    return ret;
}

size_t Join::getTotalByteCount()
{
    size_t res = 0;
    if (isEnableSpill())
    {
        for (const auto & join_partition : partitions)
            res += join_partition->memory_usage;
    }
    else
    {
        if (type == Type::CROSS)
        {
            for (const auto & block : blocks)
                res += block.bytes();
        }
        else
        {
            for (const auto & block : original_blocks)
                res += block.bytes();
            res += getTotalByteCountImpl(maps_any, type);
            res += getTotalByteCountImpl(maps_all, type);
            res += getTotalByteCountImpl(maps_any_full, type);
            res += getTotalByteCountImpl(maps_all_full, type);

            for (const auto & partition : partitions)
            {
                /// note the return value might not be accurate since it does not use lock, but should be enough for current usage
                res += partition->pool->size();
            }
        }
    }
    if (peak_build_bytes_usage)
        peak_build_bytes_usage = res;

    return res;
}

size_t Join::getPeakBuildBytesUsage()
{
    /// call `getTotalByteCount` first to make sure peak_build_bytes_usage has a meaningful value
    getTotalByteCount();
    return peak_build_bytes_usage;
}

void Join::setBuildConcurrencyAndInitJoinPartition(size_t build_concurrency_)
{
    if (unlikely(build_concurrency > 0))
        throw Exception("Logical error: `setBuildConcurrencyAndInitJoinPartition` shouldn't be called more than once", ErrorCodes::LOGICAL_ERROR);
    /// do not set active_build_concurrency because in compile stage, `joinBlock` will be called to get generate header, if active_build_concurrency
    /// is set here, `joinBlock` will hang when used to get header
    build_concurrency = std::max(1, build_concurrency_);

    partitions.reserve(build_concurrency);
    for (size_t i = 0; i < getBuildConcurrency(); ++i)
    {
        partitions.push_back(std::make_unique<JoinPartition>());
        partitions.back()->pool = std::make_shared<Arena>();
    }

    // init for non-joined-streams.
    if (getFullness(kind) || isNullAwareSemiFamily(kind))
    {
        for (size_t i = 0; i < getBuildConcurrency(); ++i)
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
    if (isLeftJoin(kind) || kind == ASTTableJoin::Kind::Full)
        for (size_t i = 0; i < num_columns_to_add; ++i)
            convertColumnToNullable(sample_block_with_columns_to_add.getByPosition(i));

    if (isLeftSemiFamily(kind))
        sample_block_with_columns_to_add.insert(ColumnWithTypeAndName(Join::match_helper_type, match_helper_name));
}

std::shared_ptr<Join> Join::createRestoreJoin(size_t max_bytes_before_external_join_)
{
    return std::make_shared<Join>(
        key_names_left,
        key_names_right,
        kind,
        original_strictness,
        log->identifier(),
        false,
        0,
        max_bytes_before_external_join_,
        createSpillConfigWithNewSpillId(build_spill_config, fmt::format("{}_hash_join_{}_build", log->identifier(), restore_round + 1)),
        createSpillConfigWithNewSpillId(probe_spill_config, fmt::format("{}_hash_join_{}_probe", log->identifier(), restore_round + 1)),
        join_restore_concurrency,
        collators,
        left_filter_column,
        right_filter_column,
        other_conditions,
        max_block_size,
        match_helper_name,
        restore_round + 1);
}

void Join::initBuild(const Block & sample_block, size_t build_concurrency_)
{
    std::unique_lock lock(rwlock);
    if (unlikely(initialized))
        throw Exception("Logical error: Join has been initialized", ErrorCodes::LOGICAL_ERROR);
    initialized = true;
    setBuildConcurrencyAndInitJoinPartition(build_concurrency_);
    build_sample_block = sample_block;
    build_spiller = std::make_unique<Spiller>(build_spill_config, false, build_concurrency_, build_sample_block, log);
    /// Choose data structure to use for JOIN.
    initMapImpl(chooseMethod(getKeyColumns(key_names_right, sample_block), key_sizes));
    if (type == Type::CROSS)
    {
        /// todo support spill for cross join
        max_bytes_before_external_join = 0;
        LOG_WARNING(log, "Cross join does not support spilling, so set max_bytes_before_external_join = 0");
    }
    if (isNullAwareSemiFamily(kind))
    {
        max_bytes_before_external_join = 0;
        LOG_WARNING(log, "null aware join does not support spilling, so set max_bytes_before_external_join = 0");
    }
    setSampleBlock(sample_block);
    for (size_t i = 0; i < build_concurrency; i++)
    {
        partitions[i]->memory_usage += partitions[i]->pool->size();
        partitions[i]->memory_usage += getPartitionByteCount(i);
    }
}

void Join::initProbe(const Block & sample_block, size_t probe_concurrency_)
{
    std::unique_lock lock(rwlock);
    setProbeConcurrency(probe_concurrency_);
    probe_sample_block = sample_block;
    probe_spiller = std::make_unique<Spiller>(probe_spill_config, false, build_concurrency, probe_sample_block, log);
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
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            if (rows_not_inserted_to_map)
                segment_index_info[segment_index_info.size() - 1].push_back(i);
            continue;
        }
        auto key_holder = key_getter.getKeyHolder(i, &pool, sort_key_containers);
        SCOPE_EXIT(keyHolderDiscardKey(key_holder));
        auto key = keyHolderGetKey(key_holder);

        size_t segment_index = 0;
        size_t hash_value = 0;
        if (!ZeroTraits::check(key))
        {
            hash_value = map.hash(key);
            segment_index = hash_value % segment_size;
        }
        segment_index_info[segment_index].push_back(i);
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
                /// for right/full out join or null-aware semi join, need to record the rows not inserted to map
                auto * elem = reinterpret_cast<Join::RowRefList *>(pool.alloc(sizeof(Join::RowRefList)));
                insertRowToList(rows_not_inserted_to_map, elem, stored_block, index);
            }
        }
        else
        {
            std::lock_guard lk(map.getSegmentMutex(segment_index));
            for (size_t i = 0; i < segment_index_info[segment_index].size(); ++i)
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
    bool enable_fine_grained_shuffle,
    bool enable_join_spill)
{
    if (enable_join_spill)
    {
        /// case 1, join with spill support, the partition level lock is acquired in `insertFromBlock`
        if (null_map)
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
        else
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
        return;
    }
    else if (enable_fine_grained_shuffle)
    {
        /// case 2, join with fine_grained_shuffle, no need to acquire any lock
        if (null_map)
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
        else
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
    }
    else if (insert_concurrency > 1)
    {
        /// case 3, normal join with concurrency > 1, will acquire lock in `insertFromBlockImplTypeCaseWithLock`
        if (null_map)
            insertFromBlockImplTypeCaseWithLock<STRICTNESS, KeyGetter, Map, true>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
        else
            insertFromBlockImplTypeCaseWithLock<STRICTNESS, KeyGetter, Map, false>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
    }
    else
    {
        /// case 4, normal join with concurrency == 1, no need to acquire any lock
        RUNTIME_CHECK(stream_index == 0);
        if (null_map)
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
        else
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(map, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map, stream_index, pool);
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
    bool enable_fine_grained_shuffle,
    bool enable_join_spill)
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
            enable_fine_grained_shuffle,                                                                                                       \
            enable_join_spill);                                                                                                                \
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
    const PaddedPODArray<UInt8> * column_data;
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
        column_data = &static_cast<const ColumnVector<UInt8> *>(column_nullable.getNestedColumnPtr().get())->getData();
    }
    else
    {
        if (!null_map_holder)
        {
            null_map_holder = ColumnVector<UInt8>::create(column->size(), 0);
        }
        column_data = &static_cast<const ColumnVector<UInt8> *>(column.get())->getData();
    }

    MutableColumnPtr mutable_null_map_holder = (*std::move(null_map_holder)).mutate();
    PaddedPODArray<UInt8> & mutable_null_map = static_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();

    for (size_t i = 0, size = column_data->size(); i < size; ++i)
        mutable_null_map[i] |= !(*column_data)[i];

    null_map_holder = std::move(mutable_null_map_holder);

    null_map = &static_cast<const ColumnUInt8 &>(*null_map_holder).getData();
}

/// the block should be valid.
void Join::insertFromBlock(const Block & block, size_t stream_index)
{
    std::shared_lock lock(rwlock);
    assert(stream_index < getBuildConcurrency());
    total_input_build_rows += block.rows();

    if (unlikely(!initialized))
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);
    Block * stored_block = nullptr;

    if (!isEnableSpill())
    {
        {
            std::lock_guard lk(blocks_lock);
            blocks.push_back(block);
            stored_block = &blocks.back();
            original_blocks.push_back(block);
        }
        insertFromBlockInternal(stored_block, stream_index);
    }
    else
    {
        Blocks dispatch_blocks;
        if (enable_fine_grained_shuffle)
        {
            dispatch_blocks.resize(build_concurrency, {});
            dispatch_blocks[stream_index] = block;
        }
        else
        {
            dispatch_blocks = dispatchBlock(key_names_right, block);
        }
        assert(dispatch_blocks.size() == build_concurrency);

        size_t bytes_to_be_added = 0;
        for (const auto & partition_block : dispatch_blocks)
        {
            if (partition_block)
            {
                bytes_to_be_added += partition_block.bytes();
            }
        }
        bool force_spill_partition_blocks = false;
        {
            std::unique_lock lk(build_probe_mutex);
            if (max_bytes_before_external_join && bytes_to_be_added + getTotalByteCount() >= max_bytes_before_external_join)
            {
                force_spill_partition_blocks = true;
            }
        }

        for (size_t j = stream_index; j < build_concurrency + stream_index; ++j)
        {
            stored_block = nullptr;
            size_t i = j % build_concurrency;
            if (!dispatch_blocks[i].rows())
            {
                continue;
            }
            Blocks blocks_to_spill;
            {
                const auto & join_partition = partitions[i];
                std::unique_lock partition_lock(join_partition->partition_mutex);
                partitions[i]->insertBlockForBuild(std::move(dispatch_blocks[i]));
                if (join_partition->spill)
                    blocks_to_spill = trySpillBuildPartition(i, force_spill_partition_blocks, partition_lock);
                else
                    stored_block = &(join_partition->build_partition.blocks.back());
                if (stored_block != nullptr)
                {
                    size_t pool_size_before_insert = join_partition->pool->size();
                    size_t map_size_before_insert = getPartitionByteCount(i);
                    insertFromBlockInternal(stored_block, i);
                    size_t pool_size_after_insert = join_partition->pool->size();
                    size_t map_size_after_insert = getPartitionByteCount(i);
                    if likely (pool_size_after_insert > pool_size_before_insert)
                    {
                        join_partition->memory_usage += (pool_size_after_insert - pool_size_before_insert);
                    }
                    if likely (map_size_after_insert > map_size_before_insert)
                    {
                        join_partition->memory_usage += (map_size_after_insert - map_size_before_insert);
                    }
                    continue;
                }
            }
            build_spiller->spillBlocks(std::move(blocks_to_spill), i);
        }
#ifdef DBMS_PUBLIC_GTEST
        // for join spill to disk gtest
        if (restore_round == 2)
            return;
#endif
        spillMostMemoryUsedPartitionIfNeed();
    }
}

bool Join::isEnableSpill() const
{
    return max_bytes_before_external_join > 0;
}

bool Join::isRestoreJoin() const
{
    return restore_round > 0;
}

void Join::insertFromBlockInternal(Block * stored_block, size_t stream_index)
{
    size_t keys_size = key_names_right.size();

    const Block & block = *stored_block;

    size_t rows = block.rows();

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    Columns materialized_columns;
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names_right);

    if (isNullAwareSemiFamily(kind))
    {
        if (rows > 0 && right_table_is_empty.load(std::memory_order_acquire))
            right_table_is_empty.store(false, std::memory_order_release);

        if (strictness == ASTTableJoin::Strictness::Any)
        {
            if (!right_has_all_key_null_row.load(std::memory_order_acquire))
            {
                /// Note that `extractAllKeyNullMap` must be done before `extractNestedColumnsAndNullMap`
                /// because `extractNestedColumnsAndNullMap` will change the nullable column to its nested column.
                ColumnPtr all_key_null_map_holder;
                ConstNullMapPtr all_key_null_map{};
                extractAllKeyNullMap(key_columns, all_key_null_map_holder, all_key_null_map);

                if (all_key_null_map)
                {
                    for (UInt8 is_null : *all_key_null_map)
                    {
                        if (is_null)
                        {
                            right_has_all_key_null_row.store(true, std::memory_order_release);
                            break;
                        }
                    }
                }
            }
        }
    }

    /// We will insert to the map only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// Reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter will not insert to the maps
    recordFilteredRows(block, right_filter_column, null_map_holder, null_map);

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
    if (isLeftJoin(kind) || kind == ASTTableJoin::Kind::Full)
    {
        for (size_t i = getFullness(kind) ? keys_size : 0; i < size; ++i)
        {
            convertColumnToNullable(stored_block->getByPosition(i));
        }
    }

    bool enable_join_spill = max_bytes_before_external_join;

    if (!isCrossJoin(kind))
    {
        assert(partitions[stream_index]->pool != nullptr);
        /// Fill the hash table.
        if (isNullAwareSemiFamily(kind))
        {
            if (strictness == ASTTableJoin::Strictness::Any)
                insertFromBlockImpl<ASTTableJoin::Strictness::Any>(type, maps_any, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map[stream_index].get(), stream_index, getBuildConcurrency(), *partitions[stream_index]->pool, enable_fine_grained_shuffle, enable_join_spill);
            else
                insertFromBlockImpl<ASTTableJoin::Strictness::All>(type, maps_all, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map[stream_index].get(), stream_index, getBuildConcurrency(), *partitions[stream_index]->pool, enable_fine_grained_shuffle, enable_join_spill);
        }
        else if (getFullness(kind))
        {
            if (strictness == ASTTableJoin::Strictness::Any)
                insertFromBlockImpl<ASTTableJoin::Strictness::Any>(type, maps_any_full, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map[stream_index].get(), stream_index, getBuildConcurrency(), *partitions[stream_index]->pool, enable_fine_grained_shuffle, enable_join_spill);
            else
                insertFromBlockImpl<ASTTableJoin::Strictness::All>(type, maps_all_full, rows, key_columns, key_sizes, collators, stored_block, null_map, rows_not_inserted_to_map[stream_index].get(), stream_index, getBuildConcurrency(), *partitions[stream_index]->pool, enable_fine_grained_shuffle, enable_join_spill);
        }
        else
        {
            if (strictness == ASTTableJoin::Strictness::Any)
                insertFromBlockImpl<ASTTableJoin::Strictness::Any>(type, maps_any, rows, key_columns, key_sizes, collators, stored_block, null_map, nullptr, stream_index, getBuildConcurrency(), *partitions[stream_index]->pool, enable_fine_grained_shuffle, enable_join_spill);
            else
                insertFromBlockImpl<ASTTableJoin::Strictness::All>(type, maps_all, rows, key_columns, key_sizes, collators, stored_block, null_map, nullptr, stream_index, getBuildConcurrency(), *partitions[stream_index]->pool, enable_fine_grained_shuffle, enable_join_spill);
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
    static bool addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & it, size_t num_columns_to_add, MutableColumns & added_columns, size_t /*i*/, IColumn::Filter * /*filter*/, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, const std::vector<size_t> & right_indexes, ProbeProcessInfo & /*probe_process_info*/)
    {
        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertFrom(*it->getMapped().block->getByPosition(right_indexes[j]).column.get(), it->getMapped().row_num);
        return false;
    }

    static bool addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t /*i*/, IColumn::Filter * /*filter*/, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, ProbeProcessInfo & /*probe_process_info*/)
    {
        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertDefault();
        return false;
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::Inner, ASTTableJoin::Strictness::Any, Map>
{
    static bool addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & it, size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, const std::vector<size_t> & right_indexes, ProbeProcessInfo & /*probe_process_info*/)
    {
        (*filter)[i] = 1;

        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertFrom(*it->getMapped().block->getByPosition(right_indexes[j]).column.get(), it->getMapped().row_num);

        return false;
    }

    static bool addNotFound(size_t /*num_columns_to_add*/, MutableColumns & /*added_columns*/, size_t i, IColumn::Filter * filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, ProbeProcessInfo & /*probe_process_info*/)
    {
        (*filter)[i] = 0;
        return false;
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::Anti, ASTTableJoin::Strictness::Any, Map>
{
    static bool addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & /*it*/, size_t /*num_columns_to_add*/, MutableColumns & /*added_columns*/, size_t i, IColumn::Filter * filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, const std::vector<size_t> & /*right_indexes*/, ProbeProcessInfo & /*probe_process_info*/)
    {
        (*filter)[i] = 0;
        return false;
    }

    static bool addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, ProbeProcessInfo & /*probe_process_info*/)
    {
        (*filter)[i] = 1;
        for (size_t j = 0; j < num_columns_to_add; ++j)
            added_columns[j]->insertDefault();
        return false;
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::LeftSemi, ASTTableJoin::Strictness::Any, Map>
{
    static bool addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & /*it*/, size_t num_columns_to_add, MutableColumns & added_columns, size_t /*i*/, IColumn::Filter * /*filter*/, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, const std::vector<size_t> & /*right_indexes*/, ProbeProcessInfo & /*probe_process_info*/)
    {
        for (size_t j = 0; j < num_columns_to_add - 1; ++j)
            added_columns[j]->insertDefault();
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_1);
        return false;
    }

    static bool addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t /*i*/, IColumn::Filter * /*filter*/, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/, ProbeProcessInfo & /*probe_process_info*/)
    {
        for (size_t j = 0; j < num_columns_to_add - 1; ++j)
            added_columns[j]->insertDefault();
        added_columns[num_columns_to_add - 1]->insert(FIELD_INT8_0);
        return false;
    }
};

template <typename Map>
struct Adder<ASTTableJoin::Kind::LeftSemi, ASTTableJoin::Strictness::All, Map>
{
    static bool addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & it, size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * /*filter*/, IColumn::Offset & current_offset, IColumn::Offsets * offsets, const std::vector<size_t> & right_indexes, ProbeProcessInfo & /*probe_process_info*/)
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
        return false;
    }

    static bool addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * /*filter*/, IColumn::Offset & current_offset, IColumn::Offsets * offsets, ProbeProcessInfo & /*probe_process_info*/)
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
    static bool addFound(const typename Map::SegmentType::HashTable::ConstLookupResult & it, size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * filter, IColumn::Offset & current_offset, IColumn::Offsets * offsets, const std::vector<size_t> & right_indexes, ProbeProcessInfo & probe_process_info)
    {
        size_t rows_joined = 0;
        // If there are too many rows in the column to split, record the number of rows that have been expanded for next read.
        // and it means the rows in this block are not joined finish.

        for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped()); current != nullptr; current = current->next)
            ++rows_joined;

        if (current_offset && current_offset + rows_joined > probe_process_info.max_block_size)
        {
            return true;
        }

        for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(it->getMapped()); current != nullptr; current = current->next)
        {
            for (size_t j = 0; j < num_columns_to_add; ++j)
                added_columns[j]->insertFrom(*current->block->getByPosition(right_indexes[j]).column.get(), current->row_num);
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

    static bool addNotFound(size_t num_columns_to_add, MutableColumns & added_columns, size_t i, IColumn::Filter * filter, IColumn::Offset & current_offset, IColumn::Offsets * offsets, ProbeProcessInfo & probe_process_info)
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
    const JoinBuildInfo & join_build_info,
    ProbeProcessInfo & probe_process_info)
{
    if (rows == 0)
    {
        probe_process_info.all_rows_joined_finish = true;
        return;
    }

    assert(probe_process_info.start_row < rows);

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
        computeDispatchHash(rows, key_columns, collators, sort_key_containers, join_build_info.restore_round, build_hash);
    }

    size_t segment_size = map.getSegmentSize();
    const auto & build_hash_data = build_hash.getData();
    assert(probe_process_info.start_row < rows);
    size_t i;
    bool block_full = false;
    for (i = probe_process_info.start_row; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
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
        else
        {
            auto key_holder = key_getter.getKeyHolder(i, &pool, sort_key_containers);
            SCOPE_EXIT(keyHolderDiscardKey(key_holder));
            auto key = keyHolderGetKey(key_holder);

            size_t hash_value = 0;
            bool zero_flag = ZeroTraits::check(key);
            if (!zero_flag)
            {
                hash_value = map.hash(key);
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

            auto & internal_map = map.getSegmentTable(segment_index);
            /// do not require segment lock because in join, the hash table can not be changed in probe stage.
            auto it = internal_map.find(key, hash_value);
            if (it != internal_map.end())
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
            else
                block_full = Adder<KIND, STRICTNESS, Map>::addNotFound(
                    num_columns_to_add,
                    added_columns,
                    i,
                    filter.get(),
                    current_offset,
                    offsets_to_replicate.get(),
                    probe_process_info);
        }

        // if block_full is true means that the current offset is greater than max_block_size, we need break the loop.
        if (block_full)
        {
            break;
        }
    }

    probe_process_info.end_row = i;
    // if i == rows, it means that all probe rows have been joined finish.
    probe_process_info.all_rows_joined_finish = (i == rows);
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
    const JoinBuildInfo & join_build_info,
    ProbeProcessInfo & probe_process_info)
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
            join_build_info,
            probe_process_info);
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
            join_build_info,
            probe_process_info);
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
        for (size_t i = 0; i < nullable_column->size(); ++i)
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
        for (size_t i = 0; i < other_filter_column->size(); ++i)
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
    other_conditions.other_cond_expr->execute(block);

    auto filter_column = ColumnUInt8::create();
    auto & filter = filter_column->getData();
    filter.assign(block.rows(), static_cast<UInt8>(1));
    if (!other_conditions.other_cond_name.empty())
    {
        mergeNullAndFilterResult(block, filter, other_conditions.other_cond_name, false);
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
        if (!other_conditions.other_eq_cond_from_in_name.empty())
        {
            auto orig_filter_column = block.getByName(other_conditions.other_eq_cond_from_in_name).column;
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
        for (size_t i = 0; i < offsets_to_replicate->size(); ++i)
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

        for (size_t i = 0; i < block.columns(); ++i)
            if (i != helper_pos)
                block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        block.safeGetByPosition(helper_pos).column = ColumnNullable::create(std::move(match_col), std::move(match_nullmap));
        return;
    }

    if (!other_conditions.other_eq_cond_from_in_name.empty())
    {
        /// other_eq_filter_from_in_column is used in anti semi join:
        /// if there is a row that return null or false for other_condition, then for anti semi join, this row should be returned.
        /// otherwise, it will check other_eq_filter_from_in_column, if other_eq_filter_from_in_column return false, this row should
        /// be returned, if other_eq_filter_from_in_column return true or null this row should not be returned.
        mergeNullAndFilterResult(block, filter, other_conditions.other_eq_cond_from_in_name, isAntiJoin(kind));
    }

    if (isInnerJoin(kind) && original_strictness == ASTTableJoin::Strictness::All)
    {
        /// inner join, just use other_filter_column to filter result
        for (size_t i = 0; i < block.columns(); ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, -1);
        return;
    }

    for (size_t i = 0, prev_offset = 0; i < offsets_to_replicate->size(); ++i)
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
                /// original strictness = ALL && kind = Anti should not happen
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
        for (size_t i = 0; i < block.columns(); ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        return;
    }
    if (isInnerJoin(kind) || isAntiJoin(kind))
    {
        /// for semi/anti join, filter out not matched rows
        for (size_t i = 0; i < block.columns(); ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(row_filter, -1);
        return;
    }
    throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void Join::joinBlockImpl(Block & block, const Maps & maps, ProbeProcessInfo & probe_process_info) const
{
    size_t keys_size = key_names_left.size();

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    Columns materialized_columns;
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names_left);

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

            /// convert left columns (except keys) to Nullable
            if (std::end(key_names_left) == std::find(key_names_left.begin(), key_names_left.end(), block.getByPosition(i).name))
                convertColumnToNullable(block.getByPosition(i));
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

    std::vector<size_t> right_table_column_indexes;
    right_table_column_indexes.reserve(num_columns_to_add);

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        right_table_column_indexes.push_back(i + existing_columns);
    }

    MutableColumns added_columns;
    added_columns.reserve(num_columns_to_add);

    std::vector<size_t> right_indexes;
    right_indexes.reserve(num_columns_to_add);

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.getByPosition(i);
        RUNTIME_CHECK_MSG(!block.has(src_column.name), "block from probe side has a column with the same name: {} as a column in sample_block_with_columns_to_add", src_column.name);

        added_columns.push_back(src_column.column->cloneEmpty());
        added_columns.back()->reserve(src_column.column->size());
        right_indexes.push_back(num_columns_to_skip + i);
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

    bool enable_spill_join = isEnableSpill();
    JoinBuildInfo join_build_info{enable_fine_grained_shuffle, fine_grained_shuffle_count, enable_spill_join, is_spilled, build_concurrency, restore_round};
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
            join_build_info,                                                                                                                   \
            probe_process_info);                                                                                                               \
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

    size_t process_rows = probe_process_info.end_row - probe_process_info.start_row;

    // if rows equal 0, we could ignore filter and offsets_to_replicate, and do not need to update start row.
    if (likely(rows != 0))
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        if (filter && !(kind == ASTTableJoin::Kind::Anti && strictness == ASTTableJoin::Strictness::All))
        {
            // If ANY INNER | RIGHT JOIN, the result will not be spilt, so the block rows must equal process_rows.
            RUNTIME_CHECK(rows == process_rows);
            for (size_t i = 0; i < existing_columns; ++i)
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(*filter, -1);
        }

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        if (offsets_to_replicate)
        {
            for (size_t i = 0; i < existing_columns; ++i)
            {
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicateRange(probe_process_info.start_row, probe_process_info.end_row, *offsets_to_replicate);
            }

            if (rows != process_rows)
            {
                if (isLeftSemiFamily(kind))
                {
                    auto helper_col = block.getByName(match_helper_name).column;
                    helper_col = helper_col->cut(probe_process_info.start_row, probe_process_info.end_row);
                }
                offsets_to_replicate->assign(offsets_to_replicate->begin() + probe_process_info.start_row, offsets_to_replicate->begin() + probe_process_info.end_row);
            }
        }
    }

    /// handle other conditions
    if (!other_conditions.other_cond_name.empty() || !other_conditions.other_eq_cond_from_in_name.empty())
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
    static size_t calTotalRightRows(const BlocksList & blocks)
    {
        size_t total_rows = 0;
        for (const Block & block_right : blocks)
        {
            size_t rows_right = block_right.rows();
            total_rows += rows_right;
        }
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            total_rows = std::min(total_rows, 1);
        return total_rows;
    }
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t total_right_rows)
    {
        size_t expanded_row_size = 0;
        for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
            dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], i, total_right_rows);

        for (const Block & block_right : blocks)
        {
            size_t rows_right = block_right.rows();
            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
            {
                rows_right = std::min(rows_right, 1);
            }

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn * column_right = block_right.getByPosition(col_num).column.get();
                dst_columns[num_existing_columns + col_num]->insertRangeFrom(*column_right, 0, rows_right);
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
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t total_right_rows)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, blocks, is_row_matched, current_offset, expanded_row_size_after_join, total_right_rows);
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
    static void addFound(MutableColumns & /* dst_columns */, size_t /* num_existing_columns */, ColumnRawPtrs & /* src_left_columns */, size_t /* num_columns_to_add */, size_t start_offset, size_t i, const BlocksList & /* blocks */, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t /* total_right_rows */)
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
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t total_right_rows)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross, ASTTableJoin::Strictness::All>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start_offset, i, blocks, is_row_matched, current_offset, expanded_row_size_after_join, total_right_rows);
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
    static void addFound(MutableColumns & dst_columns, size_t num_existing_columns, ColumnRawPtrs & src_left_columns, size_t num_columns_to_add, size_t start_offset, size_t i, const BlocksList & blocks, IColumn::Filter * is_row_matched, IColumn::Offset & current_offset, IColumn::Offsets * expanded_row_size_after_join, size_t total_right_rows)
    {
        CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add - 1, start_offset, i, blocks, is_row_matched, current_offset, expanded_row_size_after_join, total_right_rows);
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
        RUNTIME_CHECK_MSG(!block.has(src_column.name), "block from probe side has a column with the same name: {} as a column in sample_block_with_columns_to_add", src_column.name);
        block.insert(src_column);
    }

    /// NOTE It would be better to use `reserve`, as well as `replicate` methods to duplicate the values of the left block.
    size_t right_table_rows = 0;
    for (const Block & block_right : blocks)
        right_table_rows += block_right.rows();

    size_t left_rows_per_iter = std::max(rows_left, 1);
    if (max_block_size > 0 && right_table_rows > 0 && other_conditions.other_cond_expr != nullptr
        && CrossJoinAdder<KIND, STRICTNESS>::allRightRowsMaybeAdded())
    {
        /// if other_condition is not null, and all right columns maybe added during join, try to use multiple iter
        /// to make memory usage under control, for anti semi cross join that is converted by not in subquery,
        /// it is likely that other condition may filter out most of the rows
        left_rows_per_iter = std::max(max_block_size / right_table_rows, 1);
    }

    std::vector<size_t> right_column_index;
    for (size_t i = 0; i < num_columns_to_add; ++i)
        right_column_index.push_back(num_existing_columns + i);

    std::vector<Block> result_blocks;
    auto total_right_rows = CrossJoinAdder<ASTTableJoin::Kind::Cross, STRICTNESS>::calTotalRightRows(blocks);
    for (size_t start = 0; start <= rows_left; start += left_rows_per_iter)
    {
        size_t end = std::min(start + left_rows_per_iter, rows_left);
        MutableColumns dst_columns(block.columns());
        for (size_t i = 0; i < block.columns(); ++i)
        {
            dst_columns[i] = block.getByPosition(i).column->cloneEmpty();
            size_t reserved_rows = total_right_rows * (end - start);
            if likely (reserved_rows > 0)
                dst_columns[i]->reserve(reserved_rows);
        }
        IColumn::Offset current_offset = 0;
        std::unique_ptr<IColumn::Filter> is_row_matched = std::make_unique<IColumn::Filter>(end - start);
        std::unique_ptr<IColumn::Offsets> expanded_row_size_after_join = std::make_unique<IColumn::Offsets>(end - start);
        for (size_t i = start; i < end; ++i)
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
                CrossJoinAdder<KIND, STRICTNESS>::addFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start, i, blocks, is_row_matched.get(), current_offset, expanded_row_size_after_join.get(), total_right_rows);
            }
            else
            {
                CrossJoinAdder<KIND, STRICTNESS>::addNotFound(dst_columns, num_existing_columns, src_left_columns, num_columns_to_add, start, i, is_row_matched.get(), current_offset, expanded_row_size_after_join.get());
            }
        }
        auto block_per_iter = block.cloneWithColumns(std::move(dst_columns));
        if (other_conditions.other_cond_expr != nullptr)
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
        block = vstackBlocks(std::move(result_blocks));
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

void Join::checkTypes(const Block & block) const
{
    checkTypesOfKeys(block, sample_block_with_keys);
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map, bool has_filter_map>
void NO_INLINE joinBlockImplNullAwareInternal(
    const Map & map,
    Block & block,
    size_t left_columns,
    const BlocksList & right_blocks,
    const PaddedPODArray<Join::RowRefList *> & null_rows,
    size_t max_block_size,
    const JoinOtherConditions & other_conditions,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ConstNullMapPtr & null_map,
    const ConstNullMapPtr & filter_map,
    const ConstNullMapPtr & all_key_null_map,
    const TiDB::TiDBCollators & collators,
    bool right_has_all_key_null_row,
    bool right_table_is_empty)
{
    static_assert(KIND == ASTTableJoin::Kind::NullAware_Anti || KIND == ASTTableJoin::Kind::NullAware_LeftAnti
                  || KIND == ASTTableJoin::Kind::NullAware_LeftSemi);
    static_assert(STRICTNESS == ASTTableJoin::Strictness::Any || STRICTNESS == ASTTableJoin::Strictness::All);

    size_t rows = block.rows();
    KeyGetter key_getter(key_columns, key_sizes, collators);
    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(key_columns.size());

    Arena pool;
    size_t segment_size = map.getSegmentSize();

    PaddedPODArray<NASemiJoinResult<KIND, STRICTNESS>> res;
    res.reserve(rows);
    std::list<NASemiJoinResult<KIND, STRICTNESS> *> res_list;
    /// We can just consider the result of semi join because `NASemiJoinResult::setResult` will correct
    /// the result if it's not semi join.
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_filter_map)
        {
            if ((*filter_map)[i])
            {
                /// Filter out by left_conditions so the result set is empty.
                res.emplace_back(i, NASemiJoinStep::DONE, nullptr);
                res.back().template setResult<NASemiJoinResultType::FALSE_VALUE>();
                continue;
            }
        }
        if (right_table_is_empty)
        {
            /// If right table is empty, the result is false.
            /// E.g. (1,2) in ().
            res.emplace_back(i, NASemiJoinStep::DONE, nullptr);
            res.back().template setResult<NASemiJoinResultType::FALSE_VALUE>();
            continue;
        }
        if constexpr (has_null_map)
        {
            if ((*null_map)[i])
            {
                if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
                {
                    if (key_columns.size() == 1 || right_has_all_key_null_row || (all_key_null_map && (*all_key_null_map)[i]))
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
                /// Check null rows first to speed up getting the NULL result if possible.
                /// In the worse case, all rows in the right table will be checked.
                /// E.g. (1,null) in ((2,1),(2,3),(2,null),(3,null)) => false.
                res.emplace_back(i, NASemiJoinStep::NULL_KEY_CHECK_NULL_ROWS, nullptr);
                res_list.push_back(&res.back());
                continue;
            }
        }

        auto key_holder = key_getter.getKeyHolder(i, &pool, sort_key_containers);
        SCOPE_EXIT(keyHolderDiscardKey(key_holder));
        auto key = keyHolderGetKey(key_holder);

        size_t segment_index = 0;
        size_t hash_value = 0;
        if (!ZeroTraits::check(key))
        {
            hash_value = map.hash(key);
            segment_index = hash_value % segment_size;
        }

        auto & internal_map = map.getSegmentTable(segment_index);
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
            if (right_has_all_key_null_row)
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
    RUNTIME_ASSERT(res.size() == rows, "NASemiJoinResult size {} must be equal to block size {}", res.size(), rows);

    size_t right_columns = block.columns() - left_columns;

    if (!res_list.empty())
    {
        NASemiJoinHelper<KIND, STRICTNESS, typename Map::mapped_type::Base_t> helper(
            block,
            left_columns,
            right_columns,
            right_blocks,
            null_rows,
            max_block_size,
            other_conditions);

        helper.joinResult(res_list);

        RUNTIME_CHECK_MSG(res_list.empty(), "NASemiJoinResult list must be empty after calculating join result");
    }

    /// Now all results are known.

    std::unique_ptr<IColumn::Filter> filter;
    if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
        filter = std::make_unique<IColumn::Filter>(rows);

    MutableColumns added_columns(right_columns);
    for (size_t i = 0; i < right_columns; ++i)
        added_columns[i] = block.getByPosition(i + left_columns).column->cloneEmpty();

    PaddedPODArray<Int8> * left_semi_column_data = nullptr;
    PaddedPODArray<UInt8> * left_semi_null_map = nullptr;

    if constexpr (KIND == ASTTableJoin::Kind::NullAware_LeftSemi || KIND == ASTTableJoin::Kind::NullAware_LeftAnti)
    {
        auto * left_semi_column = typeid_cast<ColumnNullable *>(added_columns[right_columns - 1].get());
        left_semi_column_data = &typeid_cast<ColumnVector<Int8> &>(left_semi_column->getNestedColumn()).getData();
        left_semi_null_map = &left_semi_column->getNullMapColumn().getData();
        left_semi_column_data->reserve(rows);
        left_semi_null_map->reserve(rows);
    }

    size_t rows_for_anti = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        auto result = res[i].getResult();
        if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
        {
            if (result == NASemiJoinResultType::TRUE_VALUE)
            {
                // If the result is true, this row should be kept.
                (*filter)[i] = 1;
                ++rows_for_anti;
            }
            else
            {
                // If the result is null or false, this row should be filtered.
                (*filter)[i] = 0;
            }
        }
        else
        {
            switch (result)
            {
            case NASemiJoinResultType::FALSE_VALUE:
                left_semi_column_data->push_back(0);
                left_semi_null_map->push_back(0);
                break;
            case NASemiJoinResultType::TRUE_VALUE:
                left_semi_column_data->push_back(1);
                left_semi_null_map->push_back(0);
                break;
            case NASemiJoinResultType::NULL_VALUE:
                left_semi_column_data->push_back(0);
                left_semi_null_map->push_back(1);
                break;
            }
        }
    }

    for (size_t i = 0; i < right_columns; ++i)
    {
        if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
            added_columns[i]->insertManyDefaults(rows_for_anti);
        else if (i < right_columns - 1)
        {
            /// The last column is match_helper_name.
            added_columns[i]->insertManyDefaults(rows);
        }
        block.getByPosition(i + left_columns).column = std::move(added_columns[i]);
    }

    if constexpr (KIND == ASTTableJoin::Kind::NullAware_Anti)
    {
        for (size_t i = 0; i < left_columns; ++i)
            block.getByPosition(i).column = block.getByPosition(i).column->filter(*filter, rows_for_anti);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
void NO_INLINE joinBlockImplNullAwareCast(
    const Map & map,
    Block & block,
    size_t left_columns,
    const BlocksList & right_blocks,
    const PaddedPODArray<Join::RowRefList *> & null_rows,
    size_t max_block_size,
    const JoinOtherConditions & other_conditions,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ConstNullMapPtr & null_map,
    const ConstNullMapPtr & filter_map,
    const ConstNullMapPtr & all_key_null_map,
    const TiDB::TiDBCollators & collators,
    bool right_has_all_key_null_row,
    bool right_table_is_empty)
{
#define impl(has_null_map, has_filter_map)                                                          \
    joinBlockImplNullAwareInternal<KIND, STRICTNESS, KeyGetter, Map, has_null_map, has_filter_map>( \
        map,                                                                                        \
        block,                                                                                      \
        left_columns,                                                                               \
        right_blocks,                                                                               \
        null_rows,                                                                                  \
        max_block_size,                                                                             \
        other_conditions,                                                                           \
        key_columns,                                                                                \
        key_sizes,                                                                                  \
        null_map,                                                                                   \
        filter_map,                                                                                 \
        all_key_null_map,                                                                           \
        collators,                                                                                  \
        right_has_all_key_null_row,                                                                 \
        right_table_is_empty);

    if (null_map)
    {
        if (filter_map)
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
        if (filter_map)
        {
            impl(false, true);
        }
        else
        {
            impl(false, false);
        }
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void Join::joinBlockImplNullAware(Block & block, const Maps & maps) const
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

    /// Note that `extractAllKeyNullMap` must be done before `extractNestedColumnsAndNullMap`
    /// because `extractNestedColumnsAndNullMap` will change the nullable column to its nested column.
    ColumnPtr all_key_null_map_holder;
    ConstNullMapPtr all_key_null_map{};
    extractAllKeyNullMap(key_columns, all_key_null_map_holder, all_key_null_map);

    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    ColumnPtr filter_map_holder;
    ConstNullMapPtr filter_map{};
    recordFilteredRows(block, left_filter_column, filter_map_holder, filter_map);

    size_t existing_columns = block.columns();

    /// Add new columns to the block.
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.getByPosition(i);
        RUNTIME_CHECK_MSG(!block.has(src_column.name), "block from probe side has a column with the same name: {} as a column in sample_block_with_columns_to_add", src_column.name);
        block.insert(src_column);
    }

    switch (type)
    {
#define M(TYPE)                                                                                                                                         \
    case Join::Type::TYPE:                                                                                                                              \
        joinBlockImplNullAwareCast<KIND, STRICTNESS, typename KeyGetterForType<Join::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
            *maps.TYPE,                                                                                                                                 \
            block,                                                                                                                                      \
            existing_columns,                                                                                                                           \
            blocks,                                                                                                                                     \
            rows_with_null_keys,                                                                                                                        \
            max_block_size,                                                                                                                             \
            other_conditions,                                                                                                                           \
            key_columns,                                                                                                                                \
            key_sizes,                                                                                                                                  \
            null_map,                                                                                                                                   \
            filter_map,                                                                                                                                 \
            all_key_null_map,                                                                                                                           \
            collators,                                                                                                                                  \
            right_has_all_key_null_row.load(std::memory_order_relaxed),                                                                                 \
            right_table_is_empty.load(std::memory_order_relaxed));                                                                                      \
        break;
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_join_prob_failpoint);
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

void Join::finishOneBuild()
{
    std::unique_lock lock(build_probe_mutex);
    if (active_build_concurrency == 1)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_mpp_hash_build);
    }
    --active_build_concurrency;
    if (active_build_concurrency == 0)
    {
        workAfterBuildFinish();
        build_cv.notify_all();
    }
}

void Join::workAfterBuildFinish()
{
    if (isNullAwareSemiFamily(kind))
    {
        rows_with_null_keys.reserve(rows_not_inserted_to_map.size());
        for (const auto & i : rows_not_inserted_to_map)
        {
            Join::RowRefList * p = i->next;
            if (p != nullptr)
                rows_with_null_keys.emplace_back(p);
        }
    }

    if (isEnableSpill())
    {
        if (hasPartitionSpilled())
        {
            trySpillBuildPartitions(true);
            tryMarkBuildSpillFinish();
        }
    }
}

void Join::workAfterProbeFinish()
{
    if (isEnableSpill())
    {
        if (hasPartitionSpilled())
        {
            trySpillProbePartitions(true);
            tryMarkProbeSpillFinish();
            if (!needReturnNonJoinedData())
            {
                releaseAllPartitions();
            }
        }
    }
}

void Join::waitUntilAllBuildFinished() const
{
    std::unique_lock lock(build_probe_mutex);
    build_cv.wait(lock, [&]() {
        return active_build_concurrency == 0 || meet_error || is_canceled;
    });
    if (meet_error)
        throw Exception(error_message);
}

void Join::finishOneProbe()
{
    std::unique_lock lock(build_probe_mutex);
    if (active_probe_concurrency == 1)
    {
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_mpp_hash_probe);
    }
    --active_probe_concurrency;
    if (active_probe_concurrency == 0)
    {
        workAfterProbeFinish();
        probe_cv.notify_all();
    }
}

void Join::waitUntilAllProbeFinished() const
{
    std::unique_lock lock(build_probe_mutex);
    probe_cv.wait(lock, [&]() {
        return active_probe_concurrency == 0 || meet_error || is_canceled;
    });
    if (meet_error)
        throw Exception(error_message);
}


void Join::finishOneNonJoin(size_t partition_index)
{
    while (partition_index < build_concurrency)
    {
        std::unique_lock partition_lock(partitions[partition_index]->partition_mutex);
        releaseBuildPartitionBlocks(partition_index, partition_lock);
        releaseProbePartitionBlocks(partition_index, partition_lock);
        if (!partitions[partition_index]->spill)
        {
            releaseBuildPartitionHashTable(partition_index, partition_lock);
        }
        partition_index += build_concurrency;
    }
}

Block Join::joinBlock(ProbeProcessInfo & probe_process_info) const
{
    std::shared_lock lock(rwlock);

    probe_process_info.updateStartRow();

    Block block = probe_process_info.block;

    using enum ASTTableJoin::Strictness;
    using enum ASTTableJoin::Kind;

    if (kind == Left && strictness == Any)
        joinBlockImpl<Left, Any>(block, maps_any, probe_process_info);
    else if (kind == Inner && strictness == Any)
        joinBlockImpl<Inner, Any>(block, maps_any, probe_process_info);
    else if (kind == Left && strictness == All)
        joinBlockImpl<Left, All>(block, maps_all, probe_process_info);
    else if (kind == Inner && strictness == All)
        joinBlockImpl<Inner, All>(block, maps_all, probe_process_info);
    else if (kind == Full && strictness == Any)
        joinBlockImpl<Left, Any>(block, maps_any_full, probe_process_info);
    else if (kind == Right && strictness == Any)
        joinBlockImpl<Inner, Any>(block, maps_any_full, probe_process_info);
    else if (kind == Full && strictness == All)
        joinBlockImpl<Left, All>(block, maps_all_full, probe_process_info);
    else if (kind == Right && strictness == All)
        joinBlockImpl<Inner, All>(block, maps_all_full, probe_process_info);
    else if (kind == Anti && strictness == Any)
        joinBlockImpl<Anti, Any>(block, maps_any, probe_process_info);
    else if (kind == Anti && strictness == All)
        joinBlockImpl<Anti, All>(block, maps_all, probe_process_info);
    else if (kind == LeftSemi && strictness == Any)
        joinBlockImpl<LeftSemi, Any>(block, maps_any, probe_process_info);
    else if (kind == LeftSemi && strictness == All)
        joinBlockImpl<LeftSemi, All>(block, maps_all, probe_process_info);
    else if (kind == LeftAnti && strictness == Any)
        joinBlockImpl<LeftSemi, Any>(block, maps_any, probe_process_info);
    else if (kind == LeftAnti && strictness == All)
        joinBlockImpl<LeftSemi, All>(block, maps_all, probe_process_info);
    else if (kind == Cross && strictness == All)
        joinBlockImplCross<Cross, All>(block);
    else if (kind == Cross && strictness == Any)
        joinBlockImplCross<Cross, Any>(block);
    else if (kind == Cross_Left && strictness == All)
        joinBlockImplCross<Cross_Left, All>(block);
    else if (kind == Cross_Left && strictness == Any)
        joinBlockImplCross<Cross_Left, Any>(block);
    else if (kind == Cross_Anti && strictness == All)
        joinBlockImplCross<Cross_Anti, All>(block);
    else if (kind == Cross_Anti && strictness == Any)
        joinBlockImplCross<Cross_Anti, Any>(block);
    else if (kind == Cross_LeftSemi && strictness == All)
        joinBlockImplCross<Cross_LeftSemi, All>(block);
    else if (kind == Cross_LeftSemi && strictness == Any)
        joinBlockImplCross<Cross_LeftSemi, Any>(block);
    else if (kind == Cross_LeftAnti && strictness == All)
        joinBlockImplCross<Cross_LeftSemi, All>(block);
    else if (kind == Cross_LeftAnti && strictness == Any)
        joinBlockImplCross<Cross_LeftSemi, Any>(block);
    else if (kind == NullAware_Anti && strictness == All)
        joinBlockImplNullAware<NullAware_Anti, All>(block, maps_all);
    else if (kind == NullAware_Anti && strictness == Any)
        joinBlockImplNullAware<NullAware_Anti, Any>(block, maps_any);
    else if (kind == NullAware_LeftSemi && strictness == All)
        joinBlockImplNullAware<NullAware_LeftSemi, All>(block, maps_all);
    else if (kind == NullAware_LeftSemi && strictness == Any)
        joinBlockImplNullAware<NullAware_LeftSemi, Any>(block, maps_any);
    else if (kind == NullAware_LeftAnti && strictness == All)
        joinBlockImplNullAware<NullAware_LeftAnti, All>(block, maps_all);
    else if (kind == NullAware_LeftAnti && strictness == Any)
        joinBlockImplNullAware<NullAware_LeftAnti, Any>(block, maps_any);
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);

    /// for (cartesian)antiLeftSemi join, the meaning of "match-helper" is `non-matched` instead of `matched`.
    if (kind == LeftAnti || kind == Cross_LeftAnti)
    {
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(block.getByName(match_helper_name).column.get());
        const auto & vec_matched = static_cast<const ColumnVector<Int8> *>(nullable_column->getNestedColumnPtr().get())->getData();

        auto col_non_matched = ColumnInt8::create(vec_matched.size());
        auto & vec_non_matched = col_non_matched->getData();

        for (size_t i = 0; i < vec_matched.size(); ++i)
            vec_non_matched[i] = !vec_matched[i];

        block.getByName(match_helper_name).column = ColumnNullable::create(std::move(col_non_matched), std::move(nullable_column->getNullMapColumnPtr()));
    }

    if (isCrossJoin(kind) || isNullAwareSemiFamily(kind))
    {
        probe_process_info.all_rows_joined_finish = true;
    }

    return block;
}

bool Join::needReturnNonJoinedData() const
{
    return getFullness(kind);
}

BlockInputStreamPtr Join::createStreamWithNonJoinedRows(const Block & left_sample_block, size_t index, size_t step, size_t max_block_size) const
{
    return std::make_shared<NonJoinedBlockInputStream>(*this, left_sample_block, index, step, max_block_size);
}

Blocks Join::dispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    size_t num_shards = build_concurrency;
    size_t num_cols = from_block.columns();
    Blocks result(num_shards);
    if (num_shards == 1)
    {
        result[0] = from_block;
        return result;
    }

    IColumn::Selector selector = selectDispatchBlock(key_columns_names, from_block);


    for (size_t i = 0; i < num_shards; ++i)
        result[i] = from_block.cloneEmpty();

    for (size_t i = 0; i < num_cols; ++i)
    {
        auto dispatched_columns = from_block.getByPosition(i).column->scatter(num_shards, selector);
        assert(result.size() == dispatched_columns.size());
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            result[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }
    return result;
}

IColumn::Selector Join::hashToSelector(const WeakHash32 & hash) const
{
    size_t num_shards = build_concurrency;
    const auto & data = hash.getData();
    size_t num_rows = data.size();

    IColumn::Selector selector(num_rows);

    if unlikely (enable_fine_grained_shuffle && fine_grained_shuffle_count != build_concurrency)
    {
        for (size_t i = 0; i < num_rows; ++i)
        {
            selector[i] = data[i] % fine_grained_shuffle_count;
            selector[i] = selector[i] % num_shards;
        }
    }
    else
    {
        if (num_shards & (num_shards - 1))
        {
            for (size_t i = 0; i < num_rows; ++i)
            {
                selector[i] = data[i] % num_shards;
            }
        }
        else
        {
            for (size_t i = 0; i < num_rows; ++i)
            {
                selector[i] = data[i] & (num_shards - 1);
            }
        }
    }

    return selector;
}

IColumn::Selector Join::selectDispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    Columns materialized_columns;
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(from_block, materialized_columns, key_columns_names);

    size_t num_rows = from_block.rows();
    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(key_columns.size());

    WeakHash32 hash(0);
    computeDispatchHash(num_rows, key_columns, collators, sort_key_containers, restore_round, hash);
    return hashToSelector(hash);
}

void Join::spillMostMemoryUsedPartitionIfNeed()
{
    Int64 target_partition_index = -1;
    size_t max_bytes = 0;
    Blocks blocks_to_spill;

    {
        std::unique_lock lk(build_probe_mutex);
#ifdef DBMS_PUBLIC_GTEST
        // for join spill to disk gtest
        if (restore_round == 1 && spilled_partition_indexes.size() >= partitions.size() / 2)
            return;
#endif
        if (!disable_spill && restore_round >= 4)
        {
            LOG_DEBUG(log, fmt::format("restore round reach to 4, spilling will be disabled."));
            disable_spill = true;
            return;
        }
        if ((max_bytes_before_external_join && getTotalByteCount() <= max_bytes_before_external_join) || disable_spill)
        {
            return;
        }
        for (size_t j = 0; j < partitions.size(); ++j)
        {
            if (!partitions[j]->spill && (target_partition_index == -1 || partitions[j]->memory_usage > max_bytes))
            {
                target_partition_index = j;
                max_bytes = partitions[j]->memory_usage;
            }
        }
        if (target_partition_index == -1)
        {
            return;
        }

        RUNTIME_CHECK_MSG(build_concurrency > 1, "spilling is not is not supported when stream size = 1, please increase max_threads or set max_bytes_before_external_join = 0.");
        is_spilled = true;

        LOG_DEBUG(log, fmt::format("all bytes used : {} in join, partition size : {}", getTotalByteCount(), partitions.size()));
        LOG_DEBUG(log, fmt::format("make round : {}, partition : {} spill.", restore_round, target_partition_index));

        std::unique_lock partition_lock(partitions[target_partition_index]->partition_mutex);
        partitions[target_partition_index]->spill = true;
        releaseBuildPartitionHashTable(target_partition_index, partition_lock);
        spilled_partition_indexes.push_back(target_partition_index);
        blocks_to_spill = trySpillBuildPartition(target_partition_index, true, partition_lock);
    }
    build_spiller->spillBlocks(std::move(blocks_to_spill), target_partition_index);
    LOG_DEBUG(log, fmt::format("all bytes used after spill : {}", getTotalByteCount()));
}

void Join::tryMarkBuildSpillFinish()
{
    if (!spilled_partition_indexes.empty())
    {
        build_spiller->finishSpill();
    }
}

void Join::tryMarkProbeSpillFinish()
{
    if (!spilled_partition_indexes.empty())
    {
        probe_spiller->finishSpill();
    }
}

bool Join::getPartitionSpilled(size_t partition_index)
{
    return partitions[partition_index]->spill;
}


bool Join::hasPartitionSpilledWithLock()
{
    std::unique_lock lk(build_probe_mutex);
    return hasPartitionSpilled();
}

bool Join::hasPartitionSpilled()
{
    return !spilled_partition_indexes.empty();
}

RestoreInfo Join::getOneRestoreStream(size_t max_block_size)
{
    std::unique_lock lock(build_probe_mutex);
    if (meet_error)
        throw Exception(error_message);
    try
    {
        LOG_DEBUG(log, fmt::format("restore_build_streams {}, restore_probe_streams {}, restore_non_joined_data_streams {}", restore_build_streams.size(), restore_build_streams.size(), restore_non_joined_data_streams.size()));
        assert(restore_build_streams.size() == restore_probe_streams.size() && restore_build_streams.size() == restore_non_joined_data_streams.size());
        auto get_back_stream = [](BlockInputStreams & streams) {
            BlockInputStreamPtr stream = streams.back();
            streams.pop_back();
            return stream;
        };
        if (!restore_build_streams.empty())
        {
            auto build_stream = get_back_stream(restore_build_streams);
            auto probe_stream = get_back_stream(restore_probe_streams);
            auto non_joined_data_stream = get_back_stream(restore_non_joined_data_streams);
            if (restore_build_streams.empty())
            {
                spilled_partition_indexes.pop_front();
            }
            return {restore_join, non_joined_data_stream, build_stream, probe_stream};
        }
        if (spilled_partition_indexes.empty())
        {
            return {};
        }
        auto spilled_partition_index = spilled_partition_indexes.front();
        RUNTIME_CHECK_MSG(partitions[spilled_partition_index]->spill, "should not restore unspilled partition.");
        if (restore_join_build_concurrency <= 0)
            restore_join_build_concurrency = getRestoreJoinBuildConcurrency(partitions.size(), spilled_partition_indexes.size(), join_restore_concurrency, probe_concurrency);
        /// for restore join we make sure that the bulid concurrency is at least 2, so it can be spill again
        assert(restore_join_build_concurrency >= 2);
        LOG_DEBUG(log, "partition {}, round {}, build concurrency {}", spilled_partition_index, restore_round, restore_join_build_concurrency);
        restore_build_streams = build_spiller->restoreBlocks(spilled_partition_index, restore_join_build_concurrency, true);
        restore_probe_streams = probe_spiller->restoreBlocks(spilled_partition_index, restore_join_build_concurrency, true);
        restore_non_joined_data_streams.resize(restore_join_build_concurrency, nullptr);
        RUNTIME_CHECK_MSG(restore_build_streams.size() == static_cast<size_t>(restore_join_build_concurrency), "restore streams size must equal to restore_join_build_concurrency");
        auto new_max_bytes_before_external_join = static_cast<size_t>(max_bytes_before_external_join * (static_cast<double>(restore_join_build_concurrency) / build_concurrency));
        restore_join = createRestoreJoin(std::max(1, new_max_bytes_before_external_join));
        restore_join->initBuild(build_sample_block, restore_join_build_concurrency);
        restore_join->setInitActiveBuildConcurrency();
        restore_join->initProbe(probe_sample_block, restore_join_build_concurrency);
        for (Int64 i = 0; i < restore_join_build_concurrency; i++)
        {
            restore_build_streams[i] = std::make_shared<HashJoinBuildBlockInputStream>(restore_build_streams[i], restore_join, i, log->identifier());
        }
        auto build_stream = get_back_stream(restore_build_streams);
        auto probe_stream = get_back_stream(restore_probe_streams);
        if (restore_build_streams.empty())
        {
            spilled_partition_indexes.pop_front();
        }
        if (needReturnNonJoinedData())
        {
            for (Int64 i = 0; i < restore_join_build_concurrency; i++)
                restore_non_joined_data_streams[i] = restore_join->createStreamWithNonJoinedRows(probe_stream->getHeader(), i, restore_join_build_concurrency, max_block_size);
        }
        auto non_joined_data_stream = get_back_stream(restore_non_joined_data_streams);
        return {restore_join, non_joined_data_stream, build_stream, probe_stream};
    }
    catch (...)
    {
        restore_build_streams.clear();
        restore_probe_streams.clear();
        restore_non_joined_data_streams.clear();
        auto err_message = getCurrentExceptionMessage(false, true);
        meetErrorImpl(err_message, lock);
        throw Exception(err_message);
    }
}

void Join::dispatchProbeBlock(Block & block, std::list<std::tuple<size_t, Block>> & partition_blocks_list)
{
    Blocks partition_blocks = dispatchBlock(key_names_left, block);
    for (size_t i = 0; i < partition_blocks.size(); ++i)
    {
        if (partition_blocks[i].rows() == 0)
            continue;
        Blocks blocks_to_spill;
        bool need_spill = false;
        {
            std::unique_lock partition_lock(partitions[i]->partition_mutex);
            if (getPartitionSpilled(i))
            {
                //                insertBlockToProbePartition(std::move(partition_blocks[i]), i);
                partitions[i]->insertBlockForProbe(std::move(partition_blocks[i]));
                blocks_to_spill = trySpillProbePartition(i, false, partition_lock);
                need_spill = true;
            }
        }
        if (need_spill)
        {
            probe_spiller->spillBlocks(std::move(blocks_to_spill), i);
        }
        else
            partition_blocks_list.push_back({i, partition_blocks[i]});
    }
}

Blocks Join::trySpillBuildPartition(size_t partition_index, bool force, std::unique_lock<std::mutex> & partition_lock)
{
    const auto & join_partition = partitions[partition_index];
    if (join_partition->spill
        && ((force && join_partition->build_partition.bytes) || join_partition->build_partition.bytes >= build_spill_config.max_cached_data_bytes_in_spiller))
    {
        auto ret = join_partition->build_partition.original_blocks;
        releaseBuildPartitionBlocks(partition_index, partition_lock);
        if unlikely (join_partition->memory_usage != 0)
        {
            join_partition->memory_usage = 0;
            LOG_WARNING(log, "Incorrect memory usage after spill");
        }
        return ret;
    }
    else
    {
        return {};
    }
}

void Join::trySpillBuildPartitions(bool force)
{
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        Blocks blocks_to_spill;
        {
            std::unique_lock partition_lock(partitions[i]->partition_mutex);
            blocks_to_spill = trySpillBuildPartition(i, force, partition_lock);
        }
        build_spiller->spillBlocks(std::move(blocks_to_spill), i);
    }
}

Blocks Join::trySpillProbePartition(size_t partition_index, bool force, std::unique_lock<std::mutex> & partition_lock)
{
    const auto & join_partition = partitions[partition_index];
    if (join_partition->spill && ((force && join_partition->probe_partition.bytes) || join_partition->probe_partition.bytes >= probe_spill_config.max_cached_data_bytes_in_spiller))
    {
        auto ret = partitions[partition_index]->probe_partition.blocks;
        releaseProbePartitionBlocks(partition_index, partition_lock);
        if unlikely (join_partition->memory_usage != 0)
        {
            join_partition->memory_usage = 0;
            LOG_WARNING(log, "Incorrect memory usage after spill");
        }
        return ret;
    }
    else
        return {};
}

void Join::trySpillProbePartitions(bool force)
{
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        Blocks blocks_to_spill;
        {
            std::unique_lock partition_lock(partitions[i]->partition_mutex);
            blocks_to_spill = trySpillProbePartition(i, force, partition_lock);
        }
        probe_spiller->spillBlocks(std::move(blocks_to_spill), i);
    }
}

void Join::releaseBuildPartitionBlocks(size_t partition_index, std::unique_lock<std::mutex> &)
{
    const auto & join_partition = partitions[partition_index];
    if likely (join_partition->memory_usage >= join_partition->build_partition.bytes)
        join_partition->memory_usage -= join_partition->build_partition.bytes;
    else
        join_partition->memory_usage = 0;
    join_partition->build_partition.bytes = 0;
    join_partition->build_partition.rows = 0;
    join_partition->build_partition.blocks.clear();
    join_partition->build_partition.original_blocks.clear();
}

void Join::releaseBuildPartitionHashTable(size_t partition_index, std::unique_lock<std::mutex> &)
{
    size_t map_bytes = clearMapPartition(maps_any, type, partition_index);
    map_bytes += clearMapPartition(maps_all, type, partition_index);
    map_bytes += clearMapPartition(maps_any_full, type, partition_index);
    map_bytes += clearMapPartition(maps_all_full, type, partition_index);
    const auto & join_partition = partitions[partition_index];
    if (getFullness(kind))
    {
        rows_not_inserted_to_map[partition_index].reset();
        rows_not_inserted_to_map[partition_index] = std::make_unique<RowRefList>();
    }
    size_t pool_bytes = join_partition->pool->size();
    join_partition->pool.reset();
    if likely (join_partition->memory_usage >= map_bytes + pool_bytes)
        join_partition->memory_usage -= (map_bytes + pool_bytes);
    else
        join_partition->memory_usage = 0;
}

void Join::releaseProbePartitionBlocks(size_t partition_index, std::unique_lock<std::mutex> &)
{
    const auto & join_partition = partitions[partition_index];
    join_partition->probe_partition.blocks.clear();
    if likely (join_partition->memory_usage >= join_partition->probe_partition.bytes)
        join_partition->memory_usage -= join_partition->probe_partition.bytes;
    else
        join_partition->memory_usage = 0;
    join_partition->probe_partition.bytes = 0;
    join_partition->probe_partition.rows = 0;
}

void Join::releaseAllPartitions()
{
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        std::unique_lock partition_lock(partitions[i]->partition_mutex);
        releaseBuildPartitionBlocks(i, partition_lock);
        releaseProbePartitionBlocks(i, partition_lock);
        if (!partitions[i]->spill)
        {
            releaseBuildPartitionHashTable(i, partition_lock);
        }
    }
}

void ProbeProcessInfo::resetBlock(Block && block_, size_t partition_index_)
{
    block = std::move(block_);
    partition_index = partition_index_;
    start_row = 0;
    end_row = 0;
    all_rows_joined_finish = false;
    // If the probe block size is greater than max_block_size, we will set max_block_size to the probe block size to avoid some unnecessary split.
    max_block_size = std::max(max_block_size, block.rows());
}

void ProbeProcessInfo::updateStartRow()
{
    assert(start_row <= end_row);
    start_row = end_row;
}
} // namespace DB
