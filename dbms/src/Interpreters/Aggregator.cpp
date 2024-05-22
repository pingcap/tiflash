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

#include <AggregateFunctions/AggregateFunctionArray.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/ThresholdUtils.h>
#include <Common/typeid_cast.h>
#include <DataStreams/AggHashTableToBlocksBlockInputStream.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Aggregator.h>

#include <array>
#include <cassert>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
extern const int EMPTY_DATA_PASSED;
extern const int CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

namespace FailPoints
{
extern const char random_aggregate_create_state_failpoint[];
extern const char random_aggregate_merge_failpoint[];
extern const char force_agg_on_partial_block[];
extern const char random_fail_in_resize_callback[];
} // namespace FailPoints

#define AggregationMethodName(NAME) AggregatedDataVariants::AggregationMethod_##NAME
#define AggregationMethodNameTwoLevel(NAME) AggregatedDataVariants::AggregationMethod_##NAME##_two_level
#define AggregationMethodType(NAME) AggregatedDataVariants::Type::NAME
#define AggregationMethodTypeTwoLevel(NAME) AggregatedDataVariants::Type::NAME##_two_level
#define ToAggregationMethodPtr(NAME, ptr) (reinterpret_cast<AggregationMethodName(NAME) *>(ptr))
#define ToAggregationMethodPtrTwoLevel(NAME, ptr) (reinterpret_cast<AggregationMethodNameTwoLevel(NAME) *>(ptr))

AggregatedDataVariants::~AggregatedDataVariants()
{
    if (aggregator && !aggregator->all_aggregates_has_trivial_destructor)
    {
        try
        {
            aggregator->destroyAllAggregateStates(*this);
        }
        catch (...)
        {
            tryLogCurrentException(aggregator->log, __PRETTY_FUNCTION__);
        }
    }
    destroyAggregationMethodImpl();
}

bool AggregatedDataVariants::tryMarkNeedSpill()
{
    assert(!need_spill);
    if (empty())
        return false;
    if (!isTwoLevel())
    {
        /// Data can only be flushed to disk if a two-level aggregation is supported.
        if (!isConvertibleToTwoLevel())
            return false;
        convertToTwoLevel();
    }
    need_spill = true;
    return true;
}

void AggregatedDataVariants::destroyAggregationMethodImpl()
{
    if (!aggregation_method_impl)
        return;

#define M(NAME, IS_TWO_LEVEL)                                                            \
    case AggregationMethodType(NAME):                                                    \
    {                                                                                    \
        delete reinterpret_cast<AggregationMethodName(NAME) *>(aggregation_method_impl); \
        aggregation_method_impl = nullptr;                                               \
        break;                                                                           \
    }
    switch (type)
    {
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    default:
        break;
    }
#undef M
}

void AggregatedDataVariants::init(Type variants_type)
{
    destroyAggregationMethodImpl();

    switch (variants_type)
    {
    case Type::EMPTY:
        break;
    case Type::without_key:
        break;

#define M(NAME, IS_TWO_LEVEL)                                                                \
    case AggregationMethodType(NAME):                                                        \
    {                                                                                        \
        aggregation_method_impl = std::make_unique<AggregationMethodName(NAME)>().release(); \
        if (!aggregator->params.key_ref_agg_func.empty())                                    \
            RUNTIME_CHECK_MSG(                                                               \
                AggregationMethodName(NAME)::canUseKeyRefAggFuncOptimization(),              \
                "cannot use key_ref_agg_func opt for method {}",                             \
                getMethodName());                                                            \
        break;                                                                               \
    }

        APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M

    default:
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    type = variants_type;
}

size_t AggregatedDataVariants::getBucketNumberForTwoLevelHashTable(Type type)
{
    switch (type)
    {
#define M(NAME)                                                        \
    case AggregationMethodType(NAME):                                  \
    {                                                                  \
        return AggregationMethodNameTwoLevel(NAME)::Data::NUM_BUCKETS; \
    }

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

#undef M

    default:
        throw Exception("Wrong data variant passed.", ErrorCodes::LOGICAL_ERROR);
    }
}

void AggregatedDataVariants::setResizeCallbackIfNeeded(size_t thread_num) const
{
    if (aggregator)
    {
        auto agg_spill_context = aggregator->agg_spill_context;
        if (agg_spill_context->isSpillEnabled() && agg_spill_context->isInAutoSpillMode())
        {
            auto resize_callback = [agg_spill_context, thread_num]() {
                if (agg_spill_context->supportFurtherSpill()
                    && agg_spill_context->isThreadMarkedForAutoSpill(thread_num))
                    return false;
                bool ret = true;
                fiu_do_on(FailPoints::random_fail_in_resize_callback, {
                    if (agg_spill_context->supportFurtherSpill())
                    {
                        ret = !agg_spill_context->markThreadForAutoSpill(thread_num);
                    }
                });
                return ret;
            };
#define M(NAME)                                                                                         \
    case AggregationMethodType(NAME):                                                                   \
    {                                                                                                   \
        ToAggregationMethodPtr(NAME, aggregation_method_impl)->data.setResizeCallback(resize_callback); \
        break;                                                                                          \
    }
            switch (type)
            {
                APPLY_FOR_VARIANTS_TWO_LEVEL(M)
            default:
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
            }
#undef M
        }
    }
}

void AggregatedDataVariants::convertToTwoLevel()
{
    switch (type)
    {
#define M(NAME)                                                                           \
    case AggregationMethodType(NAME):                                                     \
    {                                                                                     \
        if (aggregator)                                                                   \
            LOG_TRACE(                                                                    \
                aggregator->log,                                                          \
                "Converting aggregation data type `{}` to `{}`.",                         \
                getMethodName(AggregationMethodType(NAME)),                               \
                getMethodName(AggregationMethodTypeTwoLevel(NAME)));                      \
        auto ori_ptr = ToAggregationMethodPtr(NAME, aggregation_method_impl);             \
        auto two_level = std::make_unique<AggregationMethodNameTwoLevel(NAME)>(*ori_ptr); \
        delete ori_ptr;                                                                   \
        aggregation_method_impl = two_level.release();                                    \
        type = AggregationMethodTypeTwoLevel(NAME);                                       \
        break;                                                                            \
    }

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

#undef M

    default:
        throw Exception("Wrong data variant passed.", ErrorCodes::LOGICAL_ERROR);
    }
    aggregator->useTwoLevelHashTable();
}


Block Aggregator::getHeader(bool final) const
{
    return params.getHeader(final);
}

/// when there is no input data and current aggregation still need to generate a result(for example,
/// select count(*) from t need to return 0 even if there is no data) the aggregator will use this
/// source header block as the fake input of aggregation
Block Aggregator::getSourceHeader() const
{
    return params.src_header;
}

Block Aggregator::Params::getHeader(
    const Block & src_header,
    const Block & intermediate_header,
    const ColumnNumbers & keys,
    const AggregateDescriptions & aggregates,
    const KeyRefAggFuncMap & key_ref_agg_func,
    bool final)
{
    Block res;

    if (intermediate_header)
    {
        res = intermediate_header.cloneEmpty();

        if (final)
        {
            for (const auto & aggregate : aggregates)
            {
                auto & elem = res.getByName(aggregate.column_name);

                elem.type = aggregate.function->getReturnType();
                elem.column = elem.type->createColumn();
            }
        }
    }
    else
    {
        for (const auto & key : keys)
        {
            // For final stage, key optimization is enabled, so no need to output columns.
            // An CopyColumn Action will handle this.
            const auto & key_col = src_header.safeGetByPosition(key);
            if (final && key_ref_agg_func.find(key_col.name) != key_ref_agg_func.end())
                continue;

            res.insert(key_col.cloneEmpty());
        }

        for (const auto & aggregate : aggregates)
        {
            size_t arguments_size = aggregate.arguments.size();
            DataTypes argument_types(arguments_size);
            for (size_t j = 0; j < arguments_size; ++j)
                argument_types[j] = src_header.safeGetByPosition(aggregate.arguments[j]).type;

            DataTypePtr type;
            if (final)
                type = aggregate.function->getReturnType();
            else
                type = std::make_shared<DataTypeAggregateFunction>(
                    aggregate.function,
                    argument_types,
                    aggregate.parameters);

            res.insert({type, aggregate.column_name});
        }
    }

    return materializeBlock(res);
}


Aggregator::Aggregator(
    const Params & params_,
    const String & req_id,
    size_t concurrency,
    const RegisterOperatorSpillContext & register_operator_spill_context)
    : params(params_)
    , log(Logger::get(req_id))
    , is_cancelled([]() { return false; })
{
    aggregate_functions.resize(params.aggregates_size);
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_functions[i] = params.aggregates[i].function.get();

    /// Initialize sizes of aggregation states and its offsets.
    offsets_of_aggregate_states.resize(params.aggregates_size);
    total_size_of_aggregate_states = 0;
    all_aggregates_has_trivial_destructor = true;

    // aggreate_states will be aligned as below:
    // |<-- state_1 -->|<-- pad_1 -->|<-- state_2 -->|<-- pad_2 -->| .....
    //
    // pad_N will be used to match alignment requirement for each next state.
    // The address of state_1 is aligned based on maximum alignment requirements in states
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        offsets_of_aggregate_states[i] = total_size_of_aggregate_states;
        total_size_of_aggregate_states += params.aggregates[i].function->sizeOfData();

        // aggreate states are aligned based on maximum requirement
        align_aggregate_states = std::max(align_aggregate_states, params.aggregates[i].function->alignOfData());

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < params.aggregates_size)
        {
            size_t alignment_of_next_state = params.aggregates[i + 1].function->alignOfData();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0)
                throw Exception("Logical error: alignOfData is not 2^N", ErrorCodes::LOGICAL_ERROR);

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            total_size_of_aggregate_states = (total_size_of_aggregate_states + alignment_of_next_state - 1)
                / alignment_of_next_state * alignment_of_next_state;
        }

        if (!params.aggregates[i].function->hasTrivialDestructor())
            all_aggregates_has_trivial_destructor = false;
    }

    method_chosen = chooseAggregationMethod();
    RUNTIME_CHECK_MSG(method_chosen != AggregatedDataVariants::Type::EMPTY, "Invalid aggregation method");
    agg_spill_context = std::make_shared<AggSpillContext>(
        concurrency,
        params.spill_config,
        params.getMaxBytesBeforeExternalGroupBy(),
        log);
    if (agg_spill_context->supportSpill())
    {
        bool is_convertible_to_two_level = AggregatedDataVariants::isConvertibleToTwoLevel(method_chosen);
        if (!is_convertible_to_two_level)
        {
            params.setMaxBytesBeforeExternalGroupBy(0);
            agg_spill_context->disableSpill();
            if (method_chosen != AggregatedDataVariants::Type::without_key)
                LOG_WARNING(
                    log,
                    "Aggregation does not support spill because aggregator hash table does not support two level");
        }
    }
    if (register_operator_spill_context != nullptr)
        register_operator_spill_context(agg_spill_context);
    if (agg_spill_context->isSpillEnabled())
    {
        /// init spiller if needed
        /// for aggregation, the input block is sorted by bucket number
        /// so it can work with MergingAggregatedMemoryEfficientBlockInputStream
        agg_spill_context->buildSpiller(getHeader(false));
    }
}


inline bool IsTypeNumber64(const DataTypePtr & type)
{
    return type->isNumber() && type->getSizeOfValueInMemory() == sizeof(uint64_t);
}

#define APPLY_FOR_AGG_FAST_PATH_TYPES(M) \
    M(Number64)                          \
    M(StringBin)                         \
    M(StringBinPadding)

enum class AggFastPathType
{
#define M(NAME) NAME,
    APPLY_FOR_AGG_FAST_PATH_TYPES(M)
#undef M
};

AggregatedDataVariants::Type ChooseAggregationMethodTwoKeys(const AggFastPathType * fast_path_types)
{
    auto tp1 = fast_path_types[0];
    auto tp2 = fast_path_types[1];
    switch (tp1)
    {
    case AggFastPathType::Number64:
    {
        switch (tp2)
        {
        case AggFastPathType::Number64:
            return AggregatedDataVariants::Type::serialized; // unreachable. keys64 or keys128 will be used before
        case AggFastPathType::StringBin:
            return AggregatedDataVariants::Type::two_keys_num64_strbin;
        case AggFastPathType::StringBinPadding:
            return AggregatedDataVariants::Type::two_keys_num64_strbinpadding;
        }
    }
    case AggFastPathType::StringBin:
    {
        switch (tp2)
        {
        case AggFastPathType::Number64:
            return AggregatedDataVariants::Type::two_keys_strbin_num64;
        case AggFastPathType::StringBin:
            return AggregatedDataVariants::Type::two_keys_strbin_strbin;
        case AggFastPathType::StringBinPadding:
            return AggregatedDataVariants::Type::serialized; // rare case
        }
    }
    case AggFastPathType::StringBinPadding:
    {
        switch (tp2)
        {
        case AggFastPathType::Number64:
            return AggregatedDataVariants::Type::two_keys_strbinpadding_num64;
        case AggFastPathType::StringBin:
            return AggregatedDataVariants::Type::serialized; // rare case
        case AggFastPathType::StringBinPadding:
            return AggregatedDataVariants::Type::two_keys_strbinpadding_strbinpadding;
        }
    }
    }
}

// return AggregatedDataVariants::Type::serialized if can NOT determine fast path.
AggregatedDataVariants::Type ChooseAggregationMethodFastPath(
    size_t keys_size,
    const DataTypes & types_not_null,
    const TiDB::TiDBCollators & collators)
{
    std::array<AggFastPathType, 2> fast_path_types{};

    if (keys_size == fast_path_types.max_size())
    {
        for (size_t i = 0; i < keys_size; ++i)
        {
            const auto & type = types_not_null[i];
            if (type->isString())
            {
                if (collators.empty() || !collators[i])
                {
                    // use original way
                    return AggregatedDataVariants::Type::serialized;
                }
                else
                {
                    switch (collators[i]->getCollatorType())
                    {
                    case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
                    case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
                    case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
                    case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
                    {
                        fast_path_types[i] = AggFastPathType::StringBinPadding;
                        break;
                    }
                    case TiDB::ITiDBCollator::CollatorType::BINARY:
                    {
                        fast_path_types[i] = AggFastPathType::StringBin;
                        break;
                    }
                    default:
                    {
                        // for CI COLLATION, use original way
                        return AggregatedDataVariants::Type::serialized;
                    }
                    }
                }
            }
            else if (IsTypeNumber64(type))
            {
                fast_path_types[i] = AggFastPathType::Number64;
            }
            else
            {
                return AggregatedDataVariants::Type::serialized;
            }
        }
        return ChooseAggregationMethodTwoKeys(fast_path_types.data());
    }
    return AggregatedDataVariants::Type::serialized;
}

AggregatedDataVariants::Type Aggregator::chooseAggregationMethod()
{
    /// If no keys. All aggregating to single row.
    if (params.keys_size == 0)
        return AggregatedDataVariants::Type::without_key;

    /// Check if at least one of the specified keys is nullable.
    DataTypes types_removed_nullable;
    types_removed_nullable.reserve(params.keys.size());
    bool has_nullable_key = false;

    for (const auto & pos : params.keys)
    {
        const auto & type
            = (params.src_header ? params.src_header : params.intermediate_header).safeGetByPosition(pos).type;

        if (type->isNullable())
        {
            has_nullable_key = true;
            types_removed_nullable.push_back(removeNullable(type));
        }
        else
            types_removed_nullable.push_back(type);
    }

    /** Returns ordinary (not two-level) methods, because we start from them.
      * Later, during aggregation process, data may be converted (partitioned) to two-level structure, if cardinality is high.
      */

    size_t keys_bytes = 0;
    size_t num_fixed_contiguous_keys = 0;

    key_sizes.resize(params.keys_size);
    for (size_t j = 0; j < params.keys_size; ++j)
    {
        if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion()
                && (params.collators.empty() || params.collators[j] == nullptr))
            {
                ++num_fixed_contiguous_keys;
                key_sizes[j] = types_removed_nullable[j]->getSizeOfValueInMemory();
                keys_bytes += key_sizes[j];
            }
        }
    }

    if (has_nullable_key)
    {
        if (params.keys_size == num_fixed_contiguous_keys)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                return AggregatedDataVariants::Type::nullable_keys128;
            if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
                return AggregatedDataVariants::Type::nullable_keys256;
        }

        /// Fallback case.
        return AggregatedDataVariants::Type::serialized;
    }

    /// No key has been found to be nullable.
    const DataTypes & types_not_null = types_removed_nullable;
    assert(!has_nullable_key);

    /// Single numeric key.
    if (params.keys_size == 1 && types_not_null[0]->isValueRepresentedByNumber())
    {
        size_t size_of_field = types_not_null[0]->getSizeOfValueInMemory();
        if (size_of_field == 1)
            return AggregatedDataVariants::Type::key8;
        if (size_of_field == 2)
            return AggregatedDataVariants::Type::key16;
        if (size_of_field == 4)
            return AggregatedDataVariants::Type::key32;
        if (size_of_field == 8)
            return AggregatedDataVariants::Type::key64;
        if (size_of_field == 16)
            return AggregatedDataVariants::Type::keys128;
        if (size_of_field == 32)
            return AggregatedDataVariants::Type::keys256;
        if (size_of_field == sizeof(Decimal256))
            return AggregatedDataVariants::Type::key_int256;
        throw Exception(
            "Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.",
            ErrorCodes::LOGICAL_ERROR);
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (params.keys_size == num_fixed_contiguous_keys)
    {
        if (keys_bytes <= 2)
            return AggregatedDataVariants::Type::keys16;
        if (keys_bytes <= 4)
            return AggregatedDataVariants::Type::keys32;
        if (keys_bytes <= 8)
            return AggregatedDataVariants::Type::keys64;
        if (keys_bytes <= 16)
            return AggregatedDataVariants::Type::keys128;
        if (keys_bytes <= 32)
            return AggregatedDataVariants::Type::keys256;
    }

    /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
    if (params.keys_size == 1 && types_not_null[0]->isString())
    {
        if (params.collators.empty() || !params.collators[0])
        {
            // use original way. `Type::one_key_strbin` will generate empty column.
            return AggregatedDataVariants::Type::key_string;
        }
        else
        {
            switch (params.collators[0]->getCollatorType())
            {
            case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
            case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
            case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
            case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
            {
                return AggregatedDataVariants::Type::one_key_strbinpadding;
            }
            case TiDB::ITiDBCollator::CollatorType::BINARY:
            {
                return AggregatedDataVariants::Type::one_key_strbin;
            }
            default:
            {
                // for CI COLLATION, use original way
                return AggregatedDataVariants::Type::key_string;
            }
            }
        }
    }

    if (params.keys_size == 1 && types_not_null[0]->isFixedString())
        return AggregatedDataVariants::Type::key_fixed_string;

    return ChooseAggregationMethodFastPath(params.keys_size, types_not_null, params.collators);
}


void Aggregator::createAggregateStates(AggregateDataPtr & aggregate_data) const
{
    for (size_t j = 0; j < params.aggregates_size; ++j)
    {
        try
        {
            /** An exception may occur if there is a shortage of memory.
              * In order that then everything is properly destroyed, we "roll back" some of the created states.
              * The code is not very convenient.
              */
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_aggregate_create_state_failpoint);
            aggregate_functions[j]->create(aggregate_data + offsets_of_aggregate_states[j]);
        }
        catch (...)
        {
            for (size_t rollback_j = 0; rollback_j < j; ++rollback_j)
                aggregate_functions[rollback_j]->destroy(aggregate_data + offsets_of_aggregate_states[rollback_j]);

            throw;
        }
    }
}


/** It's interesting - if you remove `noinline`, then gcc for some reason will inline this function, and the performance decreases (~ 10%).
  * (Probably because after the inline of this function, more internal functions no longer be inlined.)
  * Inline does not make sense, since the inner loop is entirely inside this function.
  */
template <typename Method>
void NO_INLINE Aggregator::executeImpl(
    Method & method,
    Arena * aggregates_pool,
    AggProcessInfo & agg_process_info,
    TiDB::TiDBCollators & collators) const
{
    typename Method::State state(agg_process_info.key_columns, key_sizes, collators);

    executeImplBatch(method, state, aggregates_pool, agg_process_info);
}

template <typename Method>
std::optional<typename Method::EmplaceResult> Aggregator::emplaceKey(
    Method & method,
    typename Method::State & state,
    size_t index,
    Arena & aggregates_pool,
    std::vector<std::string> & sort_key_containers) const
{
    try
    {
        return state.emplaceKey(method.data, index, aggregates_pool, sort_key_containers);
    }
    catch (ResizeException &)
    {
        return {};
    }
}

template <typename Method>
ALWAYS_INLINE void Aggregator::executeImplBatch(
    Method & method,
    typename Method::State & state,
    Arena * aggregates_pool,
    AggProcessInfo & agg_process_info) const
{
    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(params.keys_size, "");
    size_t agg_size = agg_process_info.end_row - agg_process_info.start_row;
    fiu_do_on(FailPoints::force_agg_on_partial_block, {
        if (agg_size > 0 && agg_process_info.start_row == 0)
            agg_size = std::max(agg_size / 2, 1);
    });

    /// Optimization for special case when there are no aggregate functions.
    if (params.aggregates_size == 0)
    {
        /// For all rows.
        AggregateDataPtr place = aggregates_pool->alloc(0);
        for (size_t i = 0; i < agg_size; ++i)
        {
            auto emplace_result_hold
                = emplaceKey(method, state, agg_process_info.start_row, *aggregates_pool, sort_key_containers);
            if likely (emplace_result_hold.has_value())
            {
                emplace_result_hold.value().setMapped(place);
                ++agg_process_info.start_row;
            }
            else
            {
                LOG_INFO(log, "HashTable resize throw ResizeException since the data is already marked for spill");
                break;
            }
        }
        return;
    }

    /// Optimization for special case when aggregating by 8bit key.
    if constexpr (std::is_same_v<Method, AggregatedDataVariants::AggregationMethod_key8>)
    {
        for (AggregateFunctionInstruction * inst = agg_process_info.aggregate_functions_instructions.data(); inst->that;
             ++inst)
        {
            inst->batch_that->addBatchLookupTable8(
                agg_process_info.start_row,
                agg_size,
                reinterpret_cast<AggregateDataPtr *>(method.data.data()),
                inst->state_offset,
                [&](AggregateDataPtr & aggregate_data) {
                    aggregate_data
                        = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                    createAggregateStates(aggregate_data);
                },
                state.getKeyData(),
                inst->batch_arguments,
                aggregates_pool);
        }
        agg_process_info.start_row += agg_size;
        return;
    }

    /// Generic case.

    std::unique_ptr<AggregateDataPtr[]> places(new AggregateDataPtr[agg_size]);
    std::optional<size_t> processed_rows;

    for (size_t i = agg_process_info.start_row; i < agg_process_info.start_row + agg_size; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result_holder = emplaceKey(method, state, i, *aggregates_pool, sort_key_containers);
        if unlikely (!emplace_result_holder.has_value())
        {
            LOG_INFO(log, "HashTable resize throw ResizeException since the data is already marked for spill");
            break;
        }

        auto & emplace_result = emplace_result_holder.value();

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.isInserted())
        {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.setMapped(nullptr);

            aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data);

            emplace_result.setMapped(aggregate_data);
        }
        else
            aggregate_data = emplace_result.getMapped();

        places[i - agg_process_info.start_row] = aggregate_data;
        processed_rows = i;
    }

    if (processed_rows)
    {
        /// Add values to the aggregate functions.
        for (AggregateFunctionInstruction * inst = agg_process_info.aggregate_functions_instructions.data(); inst->that;
             ++inst)
        {
            inst->batch_that->addBatch(
                agg_process_info.start_row,
                *processed_rows - agg_process_info.start_row + 1,
                places.get(),
                inst->state_offset,
                inst->batch_arguments,
                aggregates_pool);
        }
        agg_process_info.start_row = *processed_rows + 1;
    }
}

void NO_INLINE
Aggregator::executeWithoutKeyImpl(AggregatedDataWithoutKey & res, AggProcessInfo & agg_process_info, Arena * arena)
{
    size_t agg_size = agg_process_info.end_row - agg_process_info.start_row;
    fiu_do_on(FailPoints::force_agg_on_partial_block, {
        if (agg_size > 0 && agg_process_info.start_row == 0)
            agg_size = std::max(agg_size / 2, 1);
    });
    /// Adding values
    for (AggregateFunctionInstruction * inst = agg_process_info.aggregate_functions_instructions.data(); inst->that;
         ++inst)
    {
        inst->batch_that->addBatchSinglePlace(
            agg_process_info.start_row,
            agg_size,
            res + inst->state_offset,
            inst->batch_arguments,
            arena);
    }
    agg_process_info.start_row += agg_size;
}


void Aggregator::prepareAggregateInstructions(
    Columns columns,
    AggregateColumns & aggregate_columns,
    Columns & materialized_columns,
    AggregateFunctionInstructions & aggregate_functions_instructions)
{
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_columns[i].resize(params.aggregates[i].arguments.size());

    aggregate_functions_instructions.resize(params.aggregates_size + 1);
    aggregate_functions_instructions[params.aggregates_size].that = nullptr;

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
        {
            aggregate_columns[i][j] = columns.at(params.aggregates[i].arguments[j]).get();
            if (ColumnPtr converted = aggregate_columns[i][j]->convertToFullColumnIfConst())
            {
                materialized_columns.push_back(converted);
                aggregate_columns[i][j] = materialized_columns.back().get();
            }
        }

        aggregate_functions_instructions[i].arguments = aggregate_columns[i].data();
        aggregate_functions_instructions[i].state_offset = offsets_of_aggregate_states[i];

        auto * that = aggregate_functions[i];
        /// Unnest consecutive trailing -State combinators
        while (const auto * func = typeid_cast<const AggregateFunctionState *>(that))
            that = func->getNestedFunction().get();
        aggregate_functions_instructions[i].that = that;

        if (const auto * func = typeid_cast<const AggregateFunctionArray *>(that))
        {
            UNUSED(func);
            throw Exception("Not support AggregateFunctionArray", ErrorCodes::NOT_IMPLEMENTED);
        }
        else
            aggregate_functions_instructions[i].batch_arguments = aggregate_columns[i].data();

        aggregate_functions_instructions[i].batch_that = that;
    }
}

void Aggregator::AggProcessInfo::prepareForAgg()
{
    if (prepare_for_agg_done)
        return;
    RUNTIME_CHECK_MSG(block, "Block must be set before execution aggregation");
    start_row = 0;
    end_row = block.rows();
    input_columns = block.getColumns();
    materialized_columns.reserve(aggregator->params.keys_size);
    key_columns.resize(aggregator->params.keys_size);
    aggregate_columns.resize(aggregator->params.aggregates_size);

    /** Constant columns are not supported directly during aggregation.
      * To make them work anyway, we materialize them.
      */
    for (size_t i = 0; i < aggregator->params.keys_size; ++i)
    {
        key_columns[i] = input_columns.at(aggregator->params.keys[i]).get();
        if (ColumnPtr converted = key_columns[i]->convertToFullColumnIfConst())
        {
            /// Remember the columns we will work with
            materialized_columns.push_back(converted);
            key_columns[i] = materialized_columns.back().get();
        }
    }

    aggregator->prepareAggregateInstructions(
        input_columns,
        aggregate_columns,
        materialized_columns,
        aggregate_functions_instructions);
    prepare_for_agg_done = true;
}

bool Aggregator::executeOnBlock(AggProcessInfo & agg_process_info, AggregatedDataVariants & result, size_t thread_num)
{
    assert(!result.need_spill);

    if (is_cancelled())
        return true;

    /// `result` will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    /// How to perform the aggregation?
    if (!result.inited())
    {
        result.init(method_chosen);
        result.keys_size = params.keys_size;
        result.key_sizes = key_sizes;
        LOG_TRACE(log, "Aggregation method: `{}`", result.getMethodName());
    }

    agg_process_info.prepareForAgg();

    if (is_cancelled())
        return true;

    if (result.type == AggregatedDataVariants::Type::without_key && !result.without_key)
    {
        AggregateDataPtr place
            = result.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        result.without_key = place;
    }

    /// We select one of the aggregation methods and call it.

    assert(agg_process_info.start_row <= agg_process_info.end_row);
    /// For the case when there are no keys (all aggregate into one row).
    if (result.type == AggregatedDataVariants::Type::without_key)
    {
        executeWithoutKeyImpl(result.without_key, agg_process_info, result.aggregates_pool);
    }
    else
    {
#define M(NAME, IS_TWO_LEVEL)                                              \
    case AggregationMethodType(NAME):                                      \
    {                                                                      \
        executeImpl(                                                       \
            *ToAggregationMethodPtr(NAME, result.aggregation_method_impl), \
            result.aggregates_pool,                                        \
            agg_process_info,                                              \
            params.collators);                                             \
        break;                                                             \
    }

        switch (result.type)
        {
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        default:
            break;
        }

#undef M
    }

    size_t result_size = result.size();
    auto result_size_bytes = result.bytesCount();

    /// worth_convert_to_two_level is set to true if
    /// 1. some other threads already convert to two level
    /// 2. the result size exceeds threshold
    bool worth_convert_to_two_level = use_two_level_hash_table
        || (group_by_two_level_threshold && result_size >= group_by_two_level_threshold)
        || (group_by_two_level_threshold_bytes && result_size_bytes >= group_by_two_level_threshold_bytes);

    /** Converting to a two-level data structure.
      * It allows you to make, in the subsequent, an effective merge - either economical from memory or parallel.
      */
    if (result.isConvertibleToTwoLevel() && worth_convert_to_two_level)
    {
        result.convertToTwoLevel();
        result.setResizeCallbackIfNeeded(thread_num);
    }

    /** Flush data to disk if too much RAM is consumed.
      */
    auto revocable_bytes = result.revocableBytes();
    if (revocable_bytes > 20 * 1024 * 1024)
        LOG_TRACE(log, "Revocable bytes after insert one block {}, thread {}", revocable_bytes, thread_num);
    if (agg_spill_context->updatePerThreadRevocableMemory(revocable_bytes, thread_num))
    {
        assert(!result.empty());
        result.tryMarkNeedSpill();
    }

    return true;
}


void Aggregator::finishSpill()
{
    assert(agg_spill_context->getSpiller() != nullptr);
    agg_spill_context->getSpiller()->finishSpill();
}

BlockInputStreams Aggregator::restoreSpilledData()
{
    assert(agg_spill_context->getSpiller() != nullptr);
    return agg_spill_context->getSpiller()->restoreBlocks(0);
}

void Aggregator::initThresholdByAggregatedDataVariantsSize(size_t aggregated_data_variants_size)
{
    group_by_two_level_threshold = params.getGroupByTwoLevelThreshold();
    group_by_two_level_threshold_bytes
        = getAverageThreshold(params.getGroupByTwoLevelThresholdBytes(), aggregated_data_variants_size);
}

void Aggregator::spill(AggregatedDataVariants & data_variants, size_t thread_num)
{
    assert(data_variants.need_spill);
    agg_spill_context->markSpilled();
    /// Flush only two-level data and possibly overflow data.
#define M(NAME)                                                                                                     \
    case AggregationMethodType(NAME):                                                                               \
    {                                                                                                               \
        spillImpl(data_variants, *ToAggregationMethodPtr(NAME, data_variants.aggregation_method_impl), thread_num); \
        break;                                                                                                      \
    }

    switch (data_variants.type)
    {
        APPLY_FOR_VARIANTS_TWO_LEVEL(M)
    default:
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

#undef M

    /// NOTE Instead of freeing up memory and creating new hash tables and arenas, you can re-use the old ones.
    data_variants.init(data_variants.type);
    data_variants.setResizeCallbackIfNeeded(thread_num);
    data_variants.need_spill = false;
    data_variants.aggregates_pools = Arenas(1, std::make_shared<Arena>());
    data_variants.aggregates_pool = data_variants.aggregates_pools.back().get();
    data_variants.without_key = nullptr;
}

template <typename Method>
Block Aggregator::convertOneBucketToBlock(
    AggregatedDataVariants & data_variants,
    Method & method,
    Arena * arena,
    bool final,
    size_t bucket) const
{
#define FILLER_DEFINE(name, skip_convert_key)                                \
    auto filler_##name = [bucket, &method, arena, this](                     \
                             const Sizes & key_sizes,                        \
                             MutableColumns & key_columns,                   \
                             AggregateColumnsData & aggregate_columns,       \
                             MutableColumns & final_aggregate_columns,       \
                             bool final_) {                                  \
        using METHOD_TYPE = std::decay_t<decltype(method)>;                  \
        using DATA_TYPE = std::decay_t<decltype(method.data.impls[bucket])>; \
        convertToBlockImpl<METHOD_TYPE, DATA_TYPE, skip_convert_key>(        \
            method,                                                          \
            method.data.impls[bucket],                                       \
            key_sizes,                                                       \
            key_columns,                                                     \
            aggregate_columns,                                               \
            final_aggregate_columns,                                         \
            arena,                                                           \
            final_);                                                         \
    }

    FILLER_DEFINE(convert_key, false);
    FILLER_DEFINE(skip_convert_key, true);
#undef FILLER_DEFINE

    // Ignore key optimization if in non-final mode(a.k.a. during spilling process).
    // Because all keys are needed when insert spilled block back into HashMap during restore process.
    size_t convert_key_size = final ? params.keys_size - params.key_ref_agg_func.size() : params.keys_size;

    Block block;
    if (final && params.key_ref_agg_func.size() == params.keys_size)
    {
        block = prepareBlockAndFill(
            data_variants,
            final,
            method.data.impls[bucket].size(),
            filler_skip_convert_key,
            convert_key_size);
    }
    else
    {
        block = prepareBlockAndFill(
            data_variants,
            final,
            method.data.impls[bucket].size(),
            filler_convert_key,
            convert_key_size);
    }

    block.info.bucket_num = bucket;
    return block;
}

template <typename Method>
BlocksList Aggregator::convertOneBucketToBlocks(
    AggregatedDataVariants & data_variants,
    Method & method,
    Arena * arena,
    bool final,
    size_t bucket) const
{
#define FILLER_DEFINE(name, skip_convert_key)                                                         \
    auto filler_##name = [bucket, &method, arena, this](                                              \
                             const Sizes & key_sizes,                                                 \
                             std::vector<MutableColumns> & key_columns_vec,                           \
                             std::vector<AggregateColumnsData> & aggregate_columns_vec,               \
                             std::vector<MutableColumns> & final_aggregate_columns_vec,               \
                             bool final_) {                                                           \
        convertToBlocksImpl<decltype(method), decltype(method.data.impls[bucket]), skip_convert_key>( \
            method,                                                                                   \
            method.data.impls[bucket],                                                                \
            key_sizes,                                                                                \
            key_columns_vec,                                                                          \
            aggregate_columns_vec,                                                                    \
            final_aggregate_columns_vec,                                                              \
            arena,                                                                                    \
            final_);                                                                                  \
    };

    FILLER_DEFINE(convert_key, false);
    FILLER_DEFINE(skip_convert_key, true);
#undef FILLER_DEFINE

    BlocksList blocks;
    size_t convert_key_size = final ? params.keys_size - params.key_ref_agg_func.size() : params.keys_size;

    if (final && params.key_ref_agg_func.size() == params.keys_size)
    {
        blocks = prepareBlocksAndFill(
            data_variants,
            final,
            method.data.impls[bucket].size(),
            filler_skip_convert_key,
            convert_key_size);
    }
    else
    {
        blocks = prepareBlocksAndFill(
            data_variants,
            final,
            method.data.impls[bucket].size(),
            filler_convert_key,
            convert_key_size);
    }


    for (auto & block : blocks)
    {
        block.info.bucket_num = bucket;
    }

    return blocks;
}


template <typename Method>
void Aggregator::spillImpl(AggregatedDataVariants & data_variants, Method & method, size_t thread_num)
{
    RUNTIME_ASSERT(
        agg_spill_context->getSpiller() != nullptr,
        "spiller must not be nullptr in Aggregator when spilling");

    auto block_input_stream
        = std::make_shared<AggHashTableToBlocksBlockInputStream<Method>>(*this, data_variants, method);
    agg_spill_context->getSpiller()->spillBlocksUsingBlockInputStream(block_input_stream, 0, is_cancelled);
    agg_spill_context->finishOneSpill(thread_num);

    /// Pass ownership of the aggregate functions states:
    /// `data_variants` will not destroy them in the destructor, they are now owned by ColumnAggregateFunction objects.
    data_variants.aggregator = nullptr;

    auto [max_block_rows, max_block_bytes] = block_input_stream->maxBlockRowAndBytes();
    LOG_TRACE(
        log,
        "Max size of temporary bucket blocks: {} rows, {:.3f} MiB.",
        max_block_rows,
        (max_block_bytes / 1048576.0));
}


void Aggregator::execute(const BlockInputStreamPtr & stream, AggregatedDataVariants & result, size_t thread_num)
{
    if (is_cancelled())
        return;

    LOG_TRACE(log, "Aggregating");

    Stopwatch watch;

    size_t src_rows = 0;
    size_t src_bytes = 0;
    AggProcessInfo agg_process_info(this);

    /// Read all the data
    while (Block block = stream->read())
    {
        agg_process_info.resetBlock(block);
        bool should_stop = false;
        do
        {
            if unlikely (is_cancelled())
                return;
            if (!executeOnBlock(agg_process_info, result, thread_num))
            {
                should_stop = true;
                break;
            }
            if (result.need_spill)
                spill(result, thread_num);
        } while (!agg_process_info.allBlockDataHandled());

        if (should_stop)
            break;

        src_rows += block.rows();
        src_bytes += block.bytes();
    }

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (result.empty() && params.keys_size == 0 && !params.empty_result_for_aggregation_by_empty_set)
    {
        agg_process_info.resetBlock(stream->getHeader());
        executeOnBlock(agg_process_info, result, thread_num);
        if (result.need_spill)
            spill(result, thread_num);
        assert(agg_process_info.allBlockDataHandled());
    }

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = result.size();
    LOG_TRACE(
        log,
        "Aggregated. {} to {} rows (from {:.3f} MiB) in {:.3f} sec. ({:.3f} rows/sec., {:.3f} MiB/sec.)",
        src_rows,
        rows,
        src_bytes / 1048576.0,
        elapsed_seconds,
        src_rows / elapsed_seconds,
        src_bytes / elapsed_seconds / 1048576.0);
}

template <typename Method, typename Table, bool skip_convert_key>
void Aggregator::convertToBlockImpl(
    Method & method,
    Table & data,
    const Sizes & key_sizes,
    MutableColumns & key_columns,
    AggregateColumnsData & aggregate_columns,
    MutableColumns & final_aggregate_columns,
    Arena * arena,
    bool final) const
{
    if (data.empty())
        return;

    std::vector<IColumn *> raw_key_columns;
    raw_key_columns.reserve(key_columns.size());
    for (auto & column : key_columns)
        raw_key_columns.push_back(column.get());

    if (final)
        convertToBlockImplFinal<Method, Table, skip_convert_key>(
            method,
            data,
            key_sizes,
            std::move(raw_key_columns),
            final_aggregate_columns,
            arena);
    else
        convertToBlockImplNotFinal<Method, Table, skip_convert_key>(
            method,
            data,
            key_sizes,
            std::move(raw_key_columns),
            aggregate_columns);

    /// In order to release memory early.
    data.clearAndShrink();
}

template <typename Method, typename Table, bool skip_convert_key>
void Aggregator::convertToBlocksImpl(
    Method & method,
    Table & data,
    const Sizes & key_sizes,
    std::vector<MutableColumns> & key_columns_vec,
    std::vector<AggregateColumnsData> & aggregate_columns_vec,
    std::vector<MutableColumns> & final_aggregate_columns_vec,
    Arena * arena,
    bool final) const
{
    if (data.empty())
        return;

    std::vector<std::vector<IColumn *>> raw_key_columns_vec;
    raw_key_columns_vec.reserve(key_columns_vec.size());
    for (auto & key_columns : key_columns_vec)
    {
        std::vector<IColumn *> raw_key_columns;
        raw_key_columns.reserve(key_columns.size());
        for (auto & column : key_columns)
        {
            raw_key_columns.push_back(column.get());
        }

        raw_key_columns_vec.push_back(raw_key_columns);
    }

    if (final)
        convertToBlocksImplFinal<decltype(method), decltype(data), skip_convert_key>(
            method,
            data,
            key_sizes,
            std::move(raw_key_columns_vec),
            final_aggregate_columns_vec,
            arena);
    else
        convertToBlocksImplNotFinal<decltype(method), decltype(data), skip_convert_key>(
            method,
            data,
            key_sizes,
            std::move(raw_key_columns_vec),
            aggregate_columns_vec);

    /// In order to release memory early.
    data.clearAndShrink();
}


template <typename Mapped>
inline void Aggregator::insertAggregatesIntoColumns(
    Mapped & mapped,
    MutableColumns & final_aggregate_columns,
    Arena * arena) const
{
    /** Final values of aggregate functions are inserted to columns.
      * Then states of aggregate functions, that are not longer needed, are destroyed.
      *
      * We mark already destroyed states with "nullptr" in data,
      *  so they will not be destroyed in destructor of Aggregator
      * (other values will be destroyed in destructor in case of exception).
      *
      * But it becomes tricky, because we have multiple aggregate states pointed by a single pointer in data.
      * So, if exception is thrown in the middle of moving states for different aggregate functions,
      *  we have to catch exceptions and destroy all the states that are no longer needed,
      *  to keep the data in consistent state.
      *
      * It is also tricky, because there are aggregate functions with "-State" modifier.
      * When we call "insertResultInto" for them, they insert a pointer to the state to ColumnAggregateFunction
      *  and ColumnAggregateFunction will take ownership of this state.
      * So, for aggregate functions with "-State" modifier, the state must not be destroyed
      *  after it has been transferred to ColumnAggregateFunction.
      * But we should mark that the data no longer owns these states.
      */

    size_t insert_i = 0;
    std::exception_ptr exception;

    try
    {
        /// Insert final values of aggregate functions into columns.
        for (; insert_i < params.aggregates_size; ++insert_i)
            aggregate_functions[insert_i]->insertResultInto(
                mapped + offsets_of_aggregate_states[insert_i],
                *final_aggregate_columns[insert_i],
                arena);
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    /** Destroy states that are no longer needed. This loop does not throw.
        *
        * Don't destroy states for "-State" aggregate functions,
        *  because the ownership of this state is transferred to ColumnAggregateFunction
        *  and ColumnAggregateFunction will take care.
        *
        * But it's only for states that has been transferred to ColumnAggregateFunction
        *  before exception has been thrown;
        */
    for (size_t destroy_i = 0; destroy_i < params.aggregates_size; ++destroy_i)
    {
        /// If ownership was not transferred to ColumnAggregateFunction.
        if (destroy_i >= insert_i || !aggregate_functions[destroy_i]->isState())
            aggregate_functions[destroy_i]->destroy(mapped + offsets_of_aggregate_states[destroy_i]);
    }

    /// Mark the cell as destroyed so it will not be destroyed in destructor.
    mapped = nullptr;

    if (exception)
        std::rethrow_exception(exception);
}

template <typename Method>
struct AggregatorMethodInitKeyColumnHelper
{
    Method & method;
    explicit AggregatorMethodInitKeyColumnHelper(Method & method_)
        : method(method_)
    {}
    ALWAYS_INLINE inline void initAggKeys(size_t, std::vector<IColumn *> &) {}
    template <typename Key>
    ALWAYS_INLINE inline void insertKeyIntoColumns(
        const Key & key,
        std::vector<IColumn *> & key_columns,
        const Sizes & sizes,
        const TiDB::TiDBCollators & collators)
    {
        method.insertKeyIntoColumns(key, key_columns, sizes, collators);
    }
};

template <typename Key1Desc, typename Key2Desc, typename TData>
struct AggregatorMethodInitKeyColumnHelper<AggregationMethodFastPathTwoKeysNoCache<Key1Desc, Key2Desc, TData>>
{
    using Method = AggregationMethodFastPathTwoKeysNoCache<Key1Desc, Key2Desc, TData>;
    size_t index{};
    std::function<void(const StringRef &, std::vector<IColumn *> &, size_t)> insert_key_into_columns_function_ptr{};

    Method & method;
    explicit AggregatorMethodInitKeyColumnHelper(Method & method_)
        : method(method_)
    {}

    ALWAYS_INLINE inline void initAggKeys(size_t rows, std::vector<IColumn *> & key_columns)
    {
        index = 0;
        if (key_columns.size() == 1)
        {
            Method::template initAggKeys<Key1Desc>(rows, key_columns[0]);
            insert_key_into_columns_function_ptr
                = AggregationMethodFastPathTwoKeysNoCache<Key1Desc, Key2Desc, TData>::insertKeyIntoColumnsOneKey;
        }
        else if (key_columns.size() == 2)
        {
            Method::template initAggKeys<Key1Desc>(rows, key_columns[0]);
            Method::template initAggKeys<Key2Desc>(rows, key_columns[1]);
            insert_key_into_columns_function_ptr
                = AggregationMethodFastPathTwoKeysNoCache<Key1Desc, Key2Desc, TData>::insertKeyIntoColumnsTwoKey;
        }
        else
        {
            throw Exception("unexpected key_columns size for AggMethodFastPathTwoKey: {}", key_columns.size());
        }
    }
    ALWAYS_INLINE inline void insertKeyIntoColumns(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        const Sizes &,
        const TiDB::TiDBCollators &)
    {
        assert(insert_key_into_columns_function_ptr);
        insert_key_into_columns_function_ptr(key, key_columns, index);
        ++index;
    }
};

template <bool bin_padding, typename TData>
struct AggregatorMethodInitKeyColumnHelper<AggregationMethodOneKeyStringNoCache<bin_padding, TData>>
{
    using Method = AggregationMethodOneKeyStringNoCache<bin_padding, TData>;
    size_t index{};

    Method & method;
    explicit AggregatorMethodInitKeyColumnHelper(Method & method_)
        : method(method_)
    {}

    void initAggKeys(size_t rows, std::vector<IColumn *> & key_columns)
    {
        index = 0;
        RUNTIME_CHECK_MSG(
            key_columns.size() == 1,
            "unexpected key_columns size for AggMethodOneKeyString: {}",
            key_columns.size());
        Method::initAggKeys(rows, key_columns[0]);
    }
    ALWAYS_INLINE inline void insertKeyIntoColumns(
        const StringRef & key,
        std::vector<IColumn *> & key_columns,
        const Sizes &,
        const TiDB::TiDBCollators &)
    {
        method.insertKeyIntoColumns(key, key_columns, index);
        ++index;
    }
};

template <typename Method, typename Table, bool skip_convert_key>
void NO_INLINE Aggregator::convertToBlockImplFinal(
    Method & method,
    Table & data,
    const Sizes & key_sizes,
    std::vector<IColumn *> key_columns,
    MutableColumns & final_aggregate_columns,
    Arena * arena) const
{
    Sizes key_sizes_ref = key_sizes;
    AggregatorMethodInitKeyColumnHelper<Method> agg_keys_helper{method};
    if constexpr (!skip_convert_key)
    {
        auto shuffled_key_sizes = method.shuffleKeyColumns(key_columns, key_sizes);
        if (shuffled_key_sizes)
        {
            // When key_ref_agg_func is not empty, we may reorder key to skip copying some key from HashMap.
            // But this optimization is not compatible with shuffleKeyColumns of AggregationMethodKeysFixed.
            RUNTIME_CHECK(params.key_ref_agg_func.empty());
            key_sizes_ref = *shuffled_key_sizes;
        }
        agg_keys_helper.initAggKeys(data.size(), key_columns);
    }

    data.forEachValue([&](const auto & key [[maybe_unused]], auto & mapped) {
        if constexpr (!skip_convert_key)
        {
            agg_keys_helper.insertKeyIntoColumns(key, key_columns, key_sizes_ref, params.collators);
        }

        insertAggregatesIntoColumns(mapped, final_aggregate_columns, arena);
    });
}

namespace
{
template <typename Method>
std::optional<Sizes> shuffleKeyColumnsForKeyColumnsVec(
    Method & method,
    std::vector<std::vector<IColumn *>> & key_columns_vec,
    const Sizes & key_sizes)
{
    auto shuffled_key_sizes = method.shuffleKeyColumns(key_columns_vec[0], key_sizes);
    for (size_t i = 1; i < key_columns_vec.size(); ++i)
    {
        auto new_key_sizes = method.shuffleKeyColumns(key_columns_vec[i], key_sizes);
        assert(shuffled_key_sizes == new_key_sizes);
    }
    return shuffled_key_sizes;
}
template <typename Method>
std::vector<std::unique_ptr<AggregatorMethodInitKeyColumnHelper<Method>>> initAggKeysForKeyColumnsVec(
    Method & method,
    std::vector<std::vector<IColumn *>> & key_columns_vec,
    size_t max_block_size,
    size_t total_row_count)
{
    std::vector<std::unique_ptr<AggregatorMethodInitKeyColumnHelper<Method>>> agg_keys_helpers;
    size_t block_row_count = max_block_size;
    for (size_t i = 0; i < key_columns_vec.size(); ++i)
    {
        if (i == key_columns_vec.size() - 1 && total_row_count % block_row_count != 0)
            /// update block_row_count for the last block
            block_row_count = total_row_count % block_row_count;
        agg_keys_helpers.push_back(std::make_unique<AggregatorMethodInitKeyColumnHelper<Method>>(method));
        agg_keys_helpers.back()->initAggKeys(block_row_count, key_columns_vec[i]);
    }
    return agg_keys_helpers;
}
} // namespace

template <typename Method, typename Table, bool skip_convert_key>
void NO_INLINE Aggregator::convertToBlocksImplFinal(
    Method & method,
    Table & data,
    const Sizes & key_sizes,
    std::vector<std::vector<IColumn *>> && key_columns_vec,
    std::vector<MutableColumns> & final_aggregate_columns_vec,
    Arena * arena) const
{
    assert(!key_columns_vec.empty());
    std::vector<std::unique_ptr<AggregatorMethodInitKeyColumnHelper<std::decay_t<Method>>>> agg_keys_helpers;
    Sizes key_sizes_ref = key_sizes;
    if constexpr (!skip_convert_key)
    {
        auto shuffled_key_sizes = shuffleKeyColumnsForKeyColumnsVec(method, key_columns_vec, key_sizes);
        if (shuffled_key_sizes)
        {
            RUNTIME_CHECK(params.key_ref_agg_func.empty());
            key_sizes_ref = *shuffled_key_sizes;
        }
        agg_keys_helpers = initAggKeysForKeyColumnsVec(method, key_columns_vec, params.max_block_size, data.size());
    }

    size_t data_index = 0;
    data.forEachValue([&](const auto & key [[maybe_unused]], auto & mapped) {
        size_t key_columns_vec_index = data_index / params.max_block_size;
        if constexpr (!skip_convert_key)
        {
            agg_keys_helpers[key_columns_vec_index]
                ->insertKeyIntoColumns(key, key_columns_vec[key_columns_vec_index], key_sizes_ref, params.collators);
        }
        insertAggregatesIntoColumns(mapped, final_aggregate_columns_vec[key_columns_vec_index], arena);
        ++data_index;
    });
}

template <typename Method, typename Table, bool skip_convert_key>
void NO_INLINE Aggregator::convertToBlockImplNotFinal(
    Method & method,
    Table & data,
    const Sizes & key_sizes,
    std::vector<IColumn *> key_columns,
    AggregateColumnsData & aggregate_columns) const
{
    AggregatorMethodInitKeyColumnHelper<Method> agg_keys_helper{method};
    Sizes key_sizes_ref = key_sizes;
    if constexpr (!skip_convert_key)
    {
        auto shuffled_key_sizes = method.shuffleKeyColumns(key_columns, key_sizes);
        if (shuffled_key_sizes)
        {
            RUNTIME_CHECK(params.key_ref_agg_func.empty());
            key_sizes_ref = *shuffled_key_sizes;
        }
        agg_keys_helper.initAggKeys(data.size(), key_columns);
    }

    data.forEachValue([&](const auto & key [[maybe_unused]], auto & mapped) {
        if constexpr (!skip_convert_key)
        {
            agg_keys_helper.insertKeyIntoColumns(key, key_columns, key_sizes_ref, params.collators);
        }

        /// reserved, so push_back does not throw exceptions
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_columns[i]->push_back(mapped + offsets_of_aggregate_states[i]);

        mapped = nullptr;
    });
}

template <typename Method, typename Table, bool skip_convert_key>
void NO_INLINE Aggregator::convertToBlocksImplNotFinal(
    Method & method,
    Table & data,
    const Sizes & key_sizes,
    std::vector<std::vector<IColumn *>> && key_columns_vec,
    std::vector<AggregateColumnsData> & aggregate_columns_vec) const
{
    std::vector<std::unique_ptr<AggregatorMethodInitKeyColumnHelper<std::decay_t<Method>>>> agg_keys_helpers;
    Sizes key_sizes_ref = key_sizes;
    if constexpr (!skip_convert_key)
    {
        auto shuffled_key_sizes = shuffleKeyColumnsForKeyColumnsVec(method, key_columns_vec, key_sizes);
        if (shuffled_key_sizes)
        {
            RUNTIME_CHECK(params.key_ref_agg_func.empty());
            key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;
        }
        agg_keys_helpers = initAggKeysForKeyColumnsVec(method, key_columns_vec, params.max_block_size, data.size());
    }

    size_t data_index = 0;
    data.forEachValue([&](const auto & key [[maybe_unused]], auto & mapped) {
        size_t key_columns_vec_index = data_index / params.max_block_size;
        if constexpr (!skip_convert_key)
        {
            agg_keys_helpers[key_columns_vec_index]
                ->insertKeyIntoColumns(key, key_columns_vec[key_columns_vec_index], key_sizes_ref, params.collators);
        }

        /// reserved, so push_back does not throw exceptions
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_columns_vec[key_columns_vec_index][i]->push_back(mapped + offsets_of_aggregate_states[i]);

        ++data_index;
        mapped = nullptr;
    });
}

template <typename Filler>
Block Aggregator::prepareBlockAndFill(
    AggregatedDataVariants & data_variants,
    bool final,
    size_t rows,
    Filler && filler,
    size_t convert_key_size) const
{
    MutableColumns key_columns(convert_key_size);
    MutableColumns aggregate_columns(params.aggregates_size);
    MutableColumns final_aggregate_columns(params.aggregates_size);
    AggregateColumnsData aggregate_columns_data(params.aggregates_size);
    // Store size of keys that need to convert.
    Sizes new_key_sizes;
    new_key_sizes.reserve(key_sizes.size());

    Block header = getHeader(final);

    for (size_t i = 0; i < convert_key_size; ++i)
    {
        key_columns[i] = header.safeGetByPosition(i).type->createColumn();
        key_columns[i]->reserve(rows);
        new_key_sizes.push_back(key_sizes[i]);
    }

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        if (!final)
        {
            const auto & aggregate_column_name = params.aggregates[i].column_name;
            aggregate_columns[i] = header.getByName(aggregate_column_name).type->createColumn();

            /// The ColumnAggregateFunction column captures the shared ownership of the arena with the aggregate function states.
            auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);

            for (auto & pool : data_variants.aggregates_pools)
                column_aggregate_func.addArena(pool);

            aggregate_columns_data[i] = &column_aggregate_func.getData();
            aggregate_columns_data[i]->reserve(rows);
        }
        else
        {
            final_aggregate_columns[i] = aggregate_functions[i]->getReturnType()->createColumn();
            final_aggregate_columns[i]->reserve(rows);

            if (aggregate_functions[i]->isState())
            {
                /// The ColumnAggregateFunction column captures the shared ownership of the arena with aggregate function states.
                if (auto * column_aggregate_func
                    = typeid_cast<ColumnAggregateFunction *>(final_aggregate_columns[i].get()))
                    for (auto & pool : data_variants.aggregates_pools)
                        column_aggregate_func->addArena(pool);
            }
        }
    }

    filler(new_key_sizes, key_columns, aggregate_columns_data, final_aggregate_columns, final);

    Block res = header.cloneEmpty();

    for (size_t i = 0; i < convert_key_size; ++i)
        res.getByPosition(i).column = std::move(key_columns[i]);

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        if (final)
            res.getByName(aggregate_column_name).column = std::move(final_aggregate_columns[i]);
        else
            res.getByName(aggregate_column_name).column = std::move(aggregate_columns[i]);
    }

    /// Change the size of the columns-constants in the block.
    size_t columns = header.columns();
    for (size_t i = 0; i < columns; ++i)
        if (res.getByPosition(i).column->isColumnConst())
            res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

    return res;
}

template <typename Filler>
BlocksList Aggregator::prepareBlocksAndFill(
    AggregatedDataVariants & data_variants,
    bool final,
    size_t rows,
    Filler && filler,
    size_t convert_key_size) const
{
    Block header = getHeader(final);

    size_t block_count = (rows + params.max_block_size - 1) / params.max_block_size;
    std::vector<MutableColumns> key_columns_vec;
    std::vector<AggregateColumnsData> aggregate_columns_data_vec;
    std::vector<MutableColumns> aggregate_columns_vec;
    std::vector<MutableColumns> final_aggregate_columns_vec;
    // Store size of keys that need to convert.
    Sizes new_key_sizes;
    new_key_sizes.reserve(convert_key_size);

    size_t block_rows = params.max_block_size;

    for (size_t j = 0; j < block_count; ++j)
    {
        if (j == (block_count - 1) && rows % block_rows != 0)
        {
            block_rows = rows % block_rows;
        }

        key_columns_vec.push_back(MutableColumns(convert_key_size));
        aggregate_columns_data_vec.push_back(AggregateColumnsData(params.aggregates_size));
        aggregate_columns_vec.push_back(MutableColumns(params.aggregates_size));
        final_aggregate_columns_vec.push_back(MutableColumns(params.aggregates_size));

        auto & key_columns = key_columns_vec.back();
        auto & aggregate_columns_data = aggregate_columns_data_vec.back();
        auto & aggregate_columns = aggregate_columns_vec.back();
        auto & final_aggregate_columns = final_aggregate_columns_vec.back();

        for (size_t i = 0; i < convert_key_size; ++i)
        {
            key_columns[i] = header.safeGetByPosition(i).type->createColumn();
            key_columns[i]->reserve(block_rows);
            if (j == 0)
                new_key_sizes.push_back(key_sizes[i]);
        }

        for (size_t i = 0; i < params.aggregates_size; ++i)
        {
            if (!final)
            {
                const auto & aggregate_column_name = params.aggregates[i].column_name;
                aggregate_columns[i] = header.getByName(aggregate_column_name).type->createColumn();

                /// The ColumnAggregateFunction column captures the shared ownership of the arena with the aggregate function states.
                auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);

                for (auto & pool : data_variants.aggregates_pools)
                    column_aggregate_func.addArena(pool);

                aggregate_columns_data[i] = &column_aggregate_func.getData();
                aggregate_columns_data[i]->reserve(block_rows);
            }
            else
            {
                final_aggregate_columns[i] = aggregate_functions[i]->getReturnType()->createColumn();
                final_aggregate_columns[i]->reserve(block_rows);

                if (aggregate_functions[i]->isState())
                {
                    /// The ColumnAggregateFunction column captures the shared ownership of the arena with aggregate function states.
                    if (auto * column_aggregate_func
                        = typeid_cast<ColumnAggregateFunction *>(final_aggregate_columns[i].get()))
                        for (auto & pool : data_variants.aggregates_pools)
                            column_aggregate_func->addArena(pool);
                }
            }
        }
    }

    filler(new_key_sizes, key_columns_vec, aggregate_columns_data_vec, final_aggregate_columns_vec, final);

    BlocksList res_list;
    block_rows = params.max_block_size;
    for (size_t j = 0; j < block_count; ++j)
    {
        Block res = header.cloneEmpty();

        for (size_t i = 0; i < convert_key_size; ++i)
            res.getByPosition(i).column = std::move(key_columns_vec[j][i]);

        for (size_t i = 0; i < params.aggregates_size; ++i)
        {
            const auto & aggregate_column_name = params.aggregates[i].column_name;
            if (final)
                res.getByName(aggregate_column_name).column = std::move(final_aggregate_columns_vec[j][i]);
            else
                res.getByName(aggregate_column_name).column = std::move(aggregate_columns_vec[j][i]);
        }

        if (j == (block_count - 1) && rows % block_rows != 0)
        {
            block_rows = rows % block_rows;
        }

        /// Change the size of the columns-constants in the block.
        size_t columns = header.columns();
        for (size_t i = 0; i < columns; ++i)
            if (res.getByPosition(i).column->isColumnConst())
                res.getByPosition(i).column = res.getByPosition(i).column->cut(0, block_rows);

        res_list.push_back(res);
    }

    if (res_list.empty())
    {
        /// at least one valid block must be returned.
        res_list.push_back(header.cloneWithColumns(header.cloneEmptyColumns()));
    }
    return res_list;
}


BlocksList Aggregator::prepareBlocksAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final) const
{
    size_t rows = 1;

    auto filler = [&data_variants, this](
                      const Sizes &,
                      std::vector<MutableColumns> &,
                      std::vector<AggregateColumnsData> & aggregate_columns_vec,
                      std::vector<MutableColumns> & final_aggregate_columns_vec,
                      bool final_) {
        if (data_variants.type == AggregatedDataVariants::Type::without_key)
        {
            AggregatedDataWithoutKey & data = data_variants.without_key;

            RUNTIME_CHECK_MSG(data, "Wrong data variant passed.");

            if (!final_)
            {
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_columns_vec[0][i]->push_back(data + offsets_of_aggregate_states[i]);
                data = nullptr;
            }
            else
            {
                /// Always single-thread. It's safe to pass current arena from 'aggregates_pool'.
                insertAggregatesIntoColumns(data, final_aggregate_columns_vec[0], data_variants.aggregates_pool);
            }
        }
    };

    BlocksList blocks = prepareBlocksAndFill(data_variants, final, rows, filler, params.keys_size);

    if (final)
        destroyWithoutKey(data_variants);

    return blocks;
}

BlocksList Aggregator::prepareBlocksAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const
{
    size_t rows = data_variants.size();
#define M(NAME, skip_convert_key)                                                                    \
    case AggregationMethodType(NAME):                                                                \
    {                                                                                                \
        auto & tmp_method = *ToAggregationMethodPtr(NAME, data_variants.aggregation_method_impl);    \
        auto & tmp_data = ToAggregationMethodPtr(NAME, data_variants.aggregation_method_impl)->data; \
        convertToBlocksImpl<decltype(tmp_method), decltype(tmp_data), skip_convert_key>(             \
            tmp_method,                                                                              \
            tmp_data,                                                                                \
            key_sizes,                                                                               \
            key_columns_vec,                                                                         \
            aggregate_columns_vec,                                                                   \
            final_aggregate_columns_vec,                                                             \
            data_variants.aggregates_pool,                                                           \
            final_);                                                                                 \
        break;                                                                                       \
    }

#define M_skip_convert_key(NAME) M(NAME, true)
#define M_convert_key(NAME) M(NAME, false)

#define FILLER_DEFINE(name, M_tmp)                                                                            \
    auto filler_##name = [&data_variants, this](                                                              \
                             const Sizes & key_sizes,                                                         \
                             std::vector<MutableColumns> & key_columns_vec,                                   \
                             std::vector<AggregateColumnsData> & aggregate_columns_vec,                       \
                             std::vector<MutableColumns> & final_aggregate_columns_vec,                       \
                             bool final_) {                                                                   \
        switch (data_variants.type)                                                                           \
        {                                                                                                     \
            APPLY_FOR_VARIANTS_SINGLE_LEVEL(M_tmp)                                                            \
        default:                                                                                              \
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT); \
        }                                                                                                     \
    }

    FILLER_DEFINE(convert_key, M_convert_key);
    FILLER_DEFINE(skip_convert_key, M_skip_convert_key);

#undef M
#undef M_skip_convert_key
#undef M_convert_key
#undef FILLER_DEFINE

    size_t convert_key_size = final ? params.keys_size - params.key_ref_agg_func.size() : params.keys_size;

    if (final && params.key_ref_agg_func.size() == params.keys_size)
    {
        return prepareBlocksAndFill(data_variants, final, rows, filler_skip_convert_key, convert_key_size);
    }
    return prepareBlocksAndFill(data_variants, final, rows, filler_convert_key, convert_key_size);
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataImpl(Table & table_dst, Table & table_src, Arena * arena) const
{
    table_src.mergeToViaEmplace(
        table_dst,
        [&](AggregateDataPtr & __restrict dst, AggregateDataPtr & __restrict src, bool inserted) {
            if (!inserted)
            {
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->merge(
                        dst + offsets_of_aggregate_states[i],
                        src + offsets_of_aggregate_states[i],
                        arena);

                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->destroy(src + offsets_of_aggregate_states[i]);
            }
            else
            {
                dst = src;
            }

            src = nullptr;
        });
    table_src.clearAndShrink();
}

void NO_INLINE Aggregator::mergeWithoutKeyDataImpl(ManyAggregatedDataVariants & non_empty_data) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        AggregatedDataWithoutKey & res_data = res->without_key;
        AggregatedDataWithoutKey & current_data = non_empty_data[result_num]->without_key;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(
                res_data + offsets_of_aggregate_states[i],
                current_data + offsets_of_aggregate_states[i],
                res->aggregates_pool);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(current_data + offsets_of_aggregate_states[i]);

        current_data = nullptr;
    }
}


template <typename Method>
void NO_INLINE Aggregator::mergeSingleLevelDataImpl(ManyAggregatedDataVariants & non_empty_data) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        AggregatedDataVariants & current = *non_empty_data[result_num];

        mergeDataImpl<Method>(
            getDataVariant<Method>(*res).data,
            getDataVariant<Method>(current).data,
            res->aggregates_pool);

        /// `current` will not destroy the states of aggregate functions in the destructor
        current.aggregator = nullptr;
    }
}

#define M(NAME)                                                                                \
    template void NO_INLINE Aggregator::mergeSingleLevelDataImpl<AggregationMethodName(NAME)>( \
        ManyAggregatedDataVariants & non_empty_data) const;
APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
#undef M

template <typename Method>
void NO_INLINE Aggregator::mergeBucketImpl(ManyAggregatedDataVariants & data, Int32 bucket, Arena * arena) const
{
    /// We merge all aggregation results to the first.
    AggregatedDataVariantsPtr & res = data[0];
    for (size_t result_num = 1, size = data.size(); result_num < size; ++result_num)
    {
        if (is_cancelled())
            return;

        AggregatedDataVariants & current = *data[result_num];

        mergeDataImpl<Method>(
            getDataVariant<Method>(*res).data.impls[bucket],
            getDataVariant<Method>(current).data.impls[bucket],
            arena);
    }
}


MergingBucketsPtr Aggregator::mergeAndConvertToBlocks(
    ManyAggregatedDataVariants & data_variants,
    bool final,
    size_t max_threads) const
{
    if (unlikely(data_variants.empty()))
        throw Exception("Empty data passed to Aggregator::mergeAndConvertToBlocks.", ErrorCodes::EMPTY_DATA_PASSED);

    LOG_TRACE(log, "Merging aggregated data");

    ManyAggregatedDataVariants non_empty_data;
    non_empty_data.reserve(data_variants.size());
    for (auto & data : data_variants)
        if (!data->empty())
            non_empty_data.push_back(data);

    if (non_empty_data.empty())
        return nullptr;

    if (non_empty_data.size() > 1)
    {
        /// Sort the states in descending order so that the merge is more efficient (since all states are merged into the first).
        std::sort(
            non_empty_data.begin(),
            non_empty_data.end(),
            [](const AggregatedDataVariantsPtr & lhs, const AggregatedDataVariantsPtr & rhs) {
                return lhs->size() > rhs->size();
            });
    }

    /// If at least one of the options is two-level, then convert all the options into two-level ones, if there are not such.
    /// Note - perhaps it would be more optimal not to convert single-level versions before the merge, but merge them separately, at the end.

    bool has_at_least_one_two_level = false;
    for (const auto & variant : non_empty_data)
    {
        if (variant->isTwoLevel())
        {
            has_at_least_one_two_level = true;
            break;
        }
    }

    if (has_at_least_one_two_level)
        for (auto & variant : non_empty_data)
            if (!variant->isTwoLevel())
                variant->convertToTwoLevel();

    AggregatedDataVariantsPtr & first = non_empty_data[0];

    for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
    {
        if (unlikely(first->type != non_empty_data[i]->type))
            throw Exception(
                "Cannot merge different aggregated data variants.",
                ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

        /** Elements from the remaining sets can be moved to the first data set.
          * Therefore, it must own all the arenas of all other sets.
          */
        first->aggregates_pools.insert(
            first->aggregates_pools.end(),
            non_empty_data[i]->aggregates_pools.begin(),
            non_empty_data[i]->aggregates_pools.end());
    }

    // for single level merge, concurrency must be 1.
    size_t merge_concurrency = has_at_least_one_two_level ? std::max(max_threads, 1) : 1;
    return std::make_shared<MergingBuckets>(*this, non_empty_data, final, merge_concurrency);
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImplCase(
    Block & block,
    Arena * aggregates_pool,
    Method & method [[maybe_unused]],
    Table & data) const
{
    ColumnRawPtrs key_columns(params.keys_size);
    AggregateColumnsConstData aggregate_columns(params.aggregates_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        aggregate_columns[i]
            = &typeid_cast<const ColumnAggregateFunction &>(*block.getByName(aggregate_column_name).column).getData();
    }

    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(params.keys_size, "");

    /// in merge stage, don't need to care about the collator because the key is already the sort_key of original string
    typename Method::State state(key_columns, key_sizes, {});

    /// For all rows.
    size_t rows = block.rows();
    std::unique_ptr<AggregateDataPtr[]> places(new AggregateDataPtr[rows]);

    for (size_t i = 0; i < rows; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplaceKey(data, i, *aggregates_pool, sort_key_containers);
        if (emplace_result.isInserted())
        {
            emplace_result.setMapped(nullptr);

            aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data);

            emplace_result.setMapped(aggregate_data);
        }
        else
            aggregate_data = emplace_result.getMapped();

        places[i] = aggregate_data;
    }

    for (size_t j = 0; j < params.aggregates_size; ++j)
    {
        /// Merge state of aggregate functions.
        aggregate_functions[j]->mergeBatch(
            rows,
            places.get(),
            offsets_of_aggregate_states[j],
            aggregate_columns[j]->data(),
            aggregates_pool);
    }


    /// Early release memory.
    block.clear();
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImpl(Block & block, Arena * aggregates_pool, Method & method, Table & data) const
{
    mergeStreamsImplCase(block, aggregates_pool, method, data);
}


void NO_INLINE Aggregator::mergeWithoutKeyStreamsImpl(Block & block, AggregatedDataVariants & result) const
{
    AggregateColumnsConstData aggregate_columns(params.aggregates_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        aggregate_columns[i]
            = &typeid_cast<const ColumnAggregateFunction &>(*block.getByName(aggregate_column_name).column).getData();
    }

    AggregatedDataWithoutKey & res = result.without_key;
    if (!res)
    {
        AggregateDataPtr place
            = result.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        res = place;
    }

    if (block.rows() > 0)
    {
        /// Adding Values
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(
                res + offsets_of_aggregate_states[i],
                (*aggregate_columns[i])[0],
                result.aggregates_pool);
    }

    /// Early release memory.
    block.clear();
}

BlocksList Aggregator::vstackBlocks(BlocksList & blocks, bool final)
{
    RUNTIME_CHECK_MSG(!blocks.empty(), "The input blocks list for Aggregator::vstackBlocks must be non-empty");

    auto bucket_num = blocks.front().info.bucket_num;

    LOG_TRACE(
        log,
        "Merging partially aggregated blocks (bucket = {}). Original method `{}`.",
        bucket_num,
        AggregatedDataVariants::getMethodName(method_chosen));
    Stopwatch watch;

    /** If possible, change 'method' to some_hash64. Otherwise, leave as is.
      * Better hash function is needed because during external aggregation,
      *  we may merge partitions of data with total number of keys far greater than 4 billion.
      */
    auto merge_method = method_chosen;

#define APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION(M) \
    M(key64)                                                    \
    M(key_string)                                               \
    M(key_fixed_string)                                         \
    M(keys128)                                                  \
    M(keys256)                                                  \
    M(serialized)

#define M(NAME)                                                     \
    case AggregationMethodType(NAME):                               \
    {                                                               \
        merge_method = AggregatedDataVariants::Type::NAME##_hash64; \
        break;                                                      \
    }

    switch (merge_method)
    {
        APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION(M)
    default:
        break;
    }
#undef M

#undef APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION

    /// Temporary data for aggregation.
    AggregatedDataVariants result;

    /// result will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    result.init(merge_method);
    result.keys_size = params.keys_size;
    result.key_sizes = key_sizes;

    for (Block & block : blocks)
    {
        if (bucket_num >= 0 && block.info.bucket_num != bucket_num)
            bucket_num = -1;

        if (result.type == AggregatedDataVariants::Type::without_key)
            mergeWithoutKeyStreamsImpl(block, result);

#define M(NAME, IS_TWO_LEVEL)                                                    \
    case AggregationMethodType(NAME):                                            \
    {                                                                            \
        mergeStreamsImpl(                                                        \
            block,                                                               \
            result.aggregates_pool,                                              \
            *ToAggregationMethodPtr(NAME, result.aggregation_method_impl),       \
            ToAggregationMethodPtr(NAME, result.aggregation_method_impl)->data); \
        break;                                                                   \
    }
        switch (result.type)
        {
            APPLY_FOR_AGGREGATED_VARIANTS(M)
        case AggregatedDataVariants::Type::without_key:
            break;
        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
#undef M
    }

    BlocksList return_blocks;
    if (result.type == AggregatedDataVariants::Type::without_key)
        return_blocks = prepareBlocksAndFillWithoutKey(result, final);
    else
        return_blocks = prepareBlocksAndFillSingleLevel(result, final);
    /// NOTE: two-level data is not possible here - chooseAggregationMethod chooses only among single-level methods.

    if (!final)
    {
        /// Pass ownership of aggregate function states from result to ColumnAggregateFunction objects in the resulting block.
        result.aggregator = nullptr;
    }

    size_t rows = 0;
    size_t bytes = 0;
    for (auto block : return_blocks)
    {
        rows += block.rows();
        bytes += block.bytes();
        block.info.bucket_num = bucket_num;
    }
    double elapsed_seconds = watch.elapsedSeconds();
    LOG_TRACE(
        log,
        "Merged partially aggregated blocks. Return {} rows in {} blocks, {:.3f} MiB. in {:.3f} sec. ({:.3f} "
        "rows/sec., {:.3f} MiB/sec.)",
        rows,
        return_blocks.size(),
        bytes / 1048576.0,
        elapsed_seconds,
        rows / elapsed_seconds,
        bytes / elapsed_seconds / 1048576.0);

    return return_blocks;
}


template <typename Method>
void NO_INLINE Aggregator::convertBlockToTwoLevelImpl(
    Method & method,
    Arena * pool,
    ColumnRawPtrs & key_columns,
    const Block & source,
    Blocks & destinations) const
{
    typename Method::State state(key_columns, key_sizes, params.collators);

    std::vector<std::string> sort_key_containers;
    sort_key_containers.resize(params.keys_size, "");

    size_t rows = source.rows();
    size_t columns = source.columns();

    /// Create a 'selector' that will contain bucket index for every row. It will be used to scatter rows to buckets.
    IColumn::Selector selector(rows);

    /// For every row.
    for (size_t i = 0; i < rows; ++i)
    {
        /// Calculate bucket number from row hash.
        auto hash = state.getHash(method.data, i, *pool, sort_key_containers);
        auto bucket = method.data.getBucketFromHash(hash);

        selector[i] = bucket;
    }

    size_t num_buckets = destinations.size();

    for (size_t column_idx = 0; column_idx < columns; ++column_idx)
    {
        const ColumnWithTypeAndName & src_col = source.getByPosition(column_idx);
        MutableColumns scattered_columns = src_col.column->scatter(num_buckets, selector);

        for (size_t bucket = 0, size = num_buckets; bucket < size; ++bucket)
        {
            if (!scattered_columns[bucket]->empty())
            {
                Block & dst = destinations[bucket];
                dst.info.bucket_num = bucket;
                dst.insert({std::move(scattered_columns[bucket]), src_col.type, src_col.name});
            }

            /** Inserted columns of type ColumnAggregateFunction will own states of aggregate functions
              *  by holding shared_ptr to source column. See ColumnAggregateFunction.h
              */
        }
    }
}


Blocks Aggregator::convertBlockToTwoLevel(const Block & block)
{
    if (!block)
        return {};

    AggregatedDataVariants data;

    ColumnRawPtrs key_columns(params.keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    AggregatedDataVariants::Type type = method_chosen;
    data.keys_size = params.keys_size;
    data.key_sizes = key_sizes;

#define M(NAME)                                     \
    case AggregationMethodType(NAME):               \
    {                                               \
        type = AggregationMethodTypeTwoLevel(NAME); \
        break;                                      \
    }

    switch (type)
    {
        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
    default:
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }
#undef M

    data.init(type);

    size_t num_buckets = 0;

#define M(NAME)                                                                                     \
    case AggregationMethodType(NAME):                                                               \
    {                                                                                               \
        num_buckets = ToAggregationMethodPtr(NAME, data.aggregation_method_impl)->data.NUM_BUCKETS; \
        break;                                                                                      \
    }

    switch (data.type)
    {
        APPLY_FOR_VARIANTS_TWO_LEVEL(M)
    default:
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

#undef M


    Blocks splitted_blocks(num_buckets);

#define M(NAME)                                                          \
    case AggregationMethodType(NAME):                                    \
    {                                                                    \
        convertBlockToTwoLevelImpl(                                      \
            *ToAggregationMethodPtr(NAME, data.aggregation_method_impl), \
            data.aggregates_pool,                                        \
            key_columns,                                                 \
            block,                                                       \
            splitted_blocks);                                            \
        break;                                                           \
    }

    switch (data.type)
    {
        APPLY_FOR_VARIANTS_TWO_LEVEL(M)
    default:
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }
#undef M


    return splitted_blocks;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::destroyImpl(Table & table) const
{
    table.forEachMapped([&](AggregateDataPtr & data) {
        /** If an exception (usually a lack of memory, the MemoryTracker throws) arose
          *  after inserting the key into a hash table, but before creating all states of aggregate functions,
          *  then data will be equal nullptr.
          */
        if (nullptr == data)
            return;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(data + offsets_of_aggregate_states[i]);

        data = nullptr;
    });
}


void Aggregator::destroyWithoutKey(AggregatedDataVariants & result) const
{
    AggregatedDataWithoutKey & res_data = result.without_key;

    if (nullptr != res_data)
    {
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(res_data + offsets_of_aggregate_states[i]);

        res_data = nullptr;
    }
}


void Aggregator::destroyAllAggregateStates(AggregatedDataVariants & result)
{
    if (!result.inited())
        return;

    LOG_TRACE(log, "Destroying aggregate states");

    /// In what data structure is the data aggregated?
    if (result.type == AggregatedDataVariants::Type::without_key)
        destroyWithoutKey(result);

#define M(NAME, IS_TWO_LEVEL)                                                                                         \
    case AggregationMethodType(NAME):                                                                                 \
    {                                                                                                                 \
        destroyImpl<AggregationMethodName(NAME)>(ToAggregationMethodPtr(NAME, result.aggregation_method_impl)->data); \
        break;                                                                                                        \
    }

    switch (result.type)
    {
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    case AggregatedDataVariants::Type::without_key:
        break;
    default:
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

#undef M
}


void Aggregator::setCancellationHook(CancellationHook cancellation_hook)
{
    is_cancelled = cancellation_hook;
}

MergingBuckets::MergingBuckets(
    const Aggregator & aggregator_,
    const ManyAggregatedDataVariants & data_,
    bool final_,
    size_t concurrency_)
    : log(Logger::get(aggregator_.log ? aggregator_.log->identifier() : ""))
    , aggregator(aggregator_)
    , data(data_)
    , final(final_)
    , concurrency(concurrency_)
{
    assert(concurrency > 0);
    if (!data.empty())
    {
        is_two_level = data[0]->isTwoLevel();
        if (is_two_level)
        {
            for (size_t i = 0; i < concurrency; ++i)
                two_level_parallel_merge_data.push_back(std::make_unique<BlocksList>());
        }
        else
        {
            // for single level, concurrency must be 1.
            RUNTIME_CHECK(concurrency == 1);
        }

        /// At least we need one arena in first data item per concurrency
        if (concurrency > data[0]->aggregates_pools.size())
        {
            Arenas & first_pool = data[0]->aggregates_pools;
            for (size_t j = first_pool.size(); j < concurrency; ++j)
                first_pool.emplace_back(std::make_shared<Arena>());
        }
    }
}

Block MergingBuckets::getHeader() const
{
    return aggregator.getHeader(final);
}

Block MergingBuckets::getData(size_t concurrency_index)
{
    assert(concurrency_index < concurrency);

    if (unlikely(data.empty()))
        return {};

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_aggregate_merge_failpoint);

    return is_two_level ? getDataForTwoLevel(concurrency_index) : getDataForSingleLevel();
}

Block MergingBuckets::getDataForSingleLevel()
{
    assert(!data.empty());

    Block out_block = popBlocksListFront(single_level_blocks);
    if (likely(out_block))
    {
        return out_block;
    }
    // The bucket number of single level merge can only be 0.
    if (current_bucket_num > 0)
        return {};

    AggregatedDataVariantsPtr & first = data[0];
    if (first->type == AggregatedDataVariants::Type::without_key)
    {
        aggregator.mergeWithoutKeyDataImpl(data);
        single_level_blocks = aggregator.prepareBlocksAndFillWithoutKey(*first, final);
    }
    else
    {
#define M(NAME)                                                                 \
    case AggregationMethodType(NAME):                                           \
    {                                                                           \
        aggregator.mergeSingleLevelDataImpl<AggregationMethodName(NAME)>(data); \
        break;                                                                  \
    }
        switch (first->type)
        {
            APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
#undef M
        single_level_blocks = aggregator.prepareBlocksAndFillSingleLevel(*first, final);
    }
    ++current_bucket_num;
    return popBlocksListFront(single_level_blocks);
}

Block MergingBuckets::getDataForTwoLevel(size_t concurrency_index)
{
    assert(concurrency_index < two_level_parallel_merge_data.size());
    auto & two_level_merge_data = *two_level_parallel_merge_data[concurrency_index];

    Block out_block = popBlocksListFront(two_level_merge_data);
    if (likely(out_block))
        return out_block;

    if (current_bucket_num >= NUM_BUCKETS)
        return {};
    while (true)
    {
        auto local_current_bucket_num = current_bucket_num.fetch_add(1);
        if (unlikely(local_current_bucket_num >= NUM_BUCKETS))
            return {};

        doLevelMerge(local_current_bucket_num, concurrency_index);
        Block block = popBlocksListFront(two_level_merge_data);
        if (likely(block))
            return block;
    }
}

void MergingBuckets::doLevelMerge(Int32 bucket_num, size_t concurrency_index)
{
    auto & two_level_merge_data = *two_level_parallel_merge_data[concurrency_index];
    assert(two_level_merge_data.empty());

    assert(!data.empty());
    auto & merged_data = *data[0];
    auto method = merged_data.type;

    /// Select Arena to avoid race conditions
    Arena * arena = merged_data.aggregates_pools.at(concurrency_index).get();

#define M(NAME)                                                                           \
    case AggregationMethodType(NAME):                                                     \
    {                                                                                     \
        aggregator.mergeBucketImpl<AggregationMethodName(NAME)>(data, bucket_num, arena); \
        two_level_merge_data = aggregator.convertOneBucketToBlocks(                       \
            merged_data,                                                                  \
            *ToAggregationMethodPtr(NAME, merged_data.aggregation_method_impl),           \
            arena,                                                                        \
            final,                                                                        \
            bucket_num);                                                                  \
        break;                                                                            \
    }
    switch (method)
    {
        APPLY_FOR_VARIANTS_TWO_LEVEL(M)
    default:
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }
#undef M
}

#undef AggregationMethodName
#undef AggregationMethodNameTwoLevel
#undef AggregationMethodType
#undef AggregationMethodTypeTwoLevel
#undef ToAggregationMethodPtr
#undef ToAggregationMethodPtrTwoLevel

} // namespace DB
