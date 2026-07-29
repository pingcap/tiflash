// Copyright 2026 PingCAP, Inc.
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
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/MultiStageLateMaterializationTopN.h>

#include <algorithm>
#include <utility>

namespace DB::DM
{
namespace
{
struct SortKeyTypeInfo
{
    SortKeyKind kind;
    SortKeyColumnView (*build_column_view)(const IColumn & column);
    void (*extract_field)(const SortKeyColumnView & column, size_t row, SortKeyField & field);
    int (*compare_column_with_owned)(const SortKeyColumnView & column, size_t row, const SortKeyField & field);
};

std::pair<const IColumn *, const IColumn::Filter *> unwrapNullableColumn(const IColumn & column)
{
    if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(&column))
        return {&nullable_column->getNestedColumn(), &nullable_column->getNullMapData()};

    return {&column, nullptr};
}

template <typename ColumnType>
const ColumnType & castColumn(const IColumn & column)
{
    const auto * typed_column = typeid_cast<const ColumnType *>(&column);
    RUNTIME_CHECK_MSG(
        typed_column != nullptr,
        "Unexpected order by column type in TopN-enhanced multi-stage late materialization, column_type={}",
        column.getName());
    return *typed_column;
}

template <typename ColumnType>
SortKeyColumnView buildSortKeyColumnViewImpl(const IColumn & column)
{
    const auto [nested_column, null_map] = unwrapNullableColumn(column);
    return {
        .data = &castColumn<ColumnType>(*nested_column).getData(),
        .null_map = null_map,
    };
}

template <typename ColumnType>
const typename ColumnType::Container & getColumnData(const SortKeyColumnView & column)
{
    return *static_cast<const typename ColumnType::Container *>(column.data);
}

bool isNullAt(const IColumn::Filter * null_map, size_t row)
{
    return null_map != nullptr && (*null_map)[row] != 0;
}

template <typename ColumnValueType, typename StorageValueType, SortKeyKind kind, typename ColumnType>
void extractFieldImpl(const SortKeyColumnView & column, size_t row, SortKeyField & field)
{
    field.kind = kind;

    field.is_null = isNullAt(column.null_map, row);
    if (field.is_null)
        return;

    const auto & data = getColumnData<ColumnType>(column);
    field.value = static_cast<StorageValueType>(data[row]);
}

template <typename DecimalType, SortKeyKind kind>
void extractDecimalFieldImpl(const SortKeyColumnView & column, size_t row, SortKeyField & field)
{
    field.kind = kind;

    field.is_null = isNullAt(column.null_map, row);
    if (field.is_null)
        return;

    const auto & data = getColumnData<ColumnDecimal<DecimalType>>(column);
    field.value = data[row];
}

template <typename T>
int compareValue(const T & lhs, const T & rhs)
{
    return lhs > rhs ? 1 : (lhs < rhs ? -1 : 0);
}

int compareNull(bool lhs_is_null, bool rhs_is_null)
{
    if (lhs_is_null && rhs_is_null)
        return 0;
    return lhs_is_null ? -1 : 1;
}

template <typename StorageValueType, typename ColumnType>
int compareColumnWithOwnedFieldImpl(const SortKeyColumnView & column, size_t row, const SortKeyField & rhs)
{
    const bool lhs_is_null = isNullAt(column.null_map, row);
    if (lhs_is_null || rhs.is_null)
        return compareNull(lhs_is_null, rhs.is_null);

    const auto & data = getColumnData<ColumnType>(column);
    return compareValue(static_cast<StorageValueType>(data[row]), std::get<StorageValueType>(rhs.value));
}

template <typename ColumnValueType, typename ColumnType>
int compareFloatColumnWithOwnedFieldImpl(const SortKeyColumnView & column, size_t row, const SortKeyField & rhs)
{
    const bool lhs_is_null = isNullAt(column.null_map, row);
    if (lhs_is_null || rhs.is_null)
        return compareNull(lhs_is_null, rhs.is_null);

    const auto & data = getColumnData<ColumnType>(column);
    return CompareHelper<ColumnValueType>::compare(data[row], std::get<ColumnValueType>(rhs.value), -1);
}

template <typename DecimalType>
int compareDecimalColumnWithOwnedFieldImpl(const SortKeyColumnView & column, size_t row, const SortKeyField & rhs)
{
    const bool lhs_is_null = isNullAt(column.null_map, row);
    if (lhs_is_null || rhs.is_null)
        return compareNull(lhs_is_null, rhs.is_null);

    const auto & data = getColumnData<ColumnDecimal<DecimalType>>(column);
    return compareValue(data[row], std::get<DecimalType>(rhs.value));
}

template <typename ColumnValueType, typename StorageValueType, SortKeyKind kind, typename ColumnType>
SortKeyTypeInfo makeSortKeyTypeInfo()
{
    return {
        .kind = kind,
        .build_column_view = buildSortKeyColumnViewImpl<ColumnType>,
        .extract_field = extractFieldImpl<ColumnValueType, StorageValueType, kind, ColumnType>,
        .compare_column_with_owned = compareColumnWithOwnedFieldImpl<StorageValueType, ColumnType>,
    };
}

template <typename ColumnValueType, SortKeyKind kind, typename ColumnType>
SortKeyTypeInfo makeFloatSortKeyTypeInfo()
{
    return {
        .kind = kind,
        .build_column_view = buildSortKeyColumnViewImpl<ColumnType>,
        .extract_field = extractFieldImpl<ColumnValueType, ColumnValueType, kind, ColumnType>,
        .compare_column_with_owned = compareFloatColumnWithOwnedFieldImpl<ColumnValueType, ColumnType>,
    };
}

template <typename DecimalType, SortKeyKind kind>
SortKeyTypeInfo makeDecimalSortKeyTypeInfo()
{
    return {
        .kind = kind,
        .build_column_view = buildSortKeyColumnViewImpl<ColumnDecimal<DecimalType>>,
        .extract_field = extractDecimalFieldImpl<DecimalType, kind>,
        .compare_column_with_owned = compareDecimalColumnWithOwnedFieldImpl<DecimalType>,
    };
}

int compareSortKeyFields(const SortKeyField & lhs, const SortKeyField & rhs)
{
    RUNTIME_CHECK(lhs.kind == rhs.kind);

    if (lhs.is_null || rhs.is_null)
    {
        if (lhs.is_null && rhs.is_null)
            return 0;
        return lhs.is_null ? -1 : 1;
    }

    switch (lhs.kind)
    {
    case SortKeyKind::Int64:
        return compareValue(std::get<Int64>(lhs.value), std::get<Int64>(rhs.value));
    case SortKeyKind::UInt64:
    case SortKeyKind::Date:
    case SortKeyKind::DateTime:
        return compareValue(std::get<UInt64>(lhs.value), std::get<UInt64>(rhs.value));
    case SortKeyKind::Float32:
        return CompareHelper<Float32>::compare(std::get<Float32>(lhs.value), std::get<Float32>(rhs.value), -1);
    case SortKeyKind::Float64:
        return CompareHelper<Float64>::compare(std::get<Float64>(lhs.value), std::get<Float64>(rhs.value), -1);
    case SortKeyKind::Decimal32:
        return compareValue(std::get<Decimal32>(lhs.value), std::get<Decimal32>(rhs.value));
    case SortKeyKind::Decimal64:
        return compareValue(std::get<Decimal64>(lhs.value), std::get<Decimal64>(rhs.value));
    case SortKeyKind::Decimal128:
        return compareValue(std::get<Decimal128>(lhs.value), std::get<Decimal128>(rhs.value));
    case SortKeyKind::Decimal256:
        return compareValue(std::get<Decimal256>(lhs.value), std::get<Decimal256>(rhs.value));
    }
    RUNTIME_CHECK_MSG(false, "Unexpected sort key kind");
    return 0;
}

SortKeyTypeInfo getSortKeyTypeInfo(const DataTypePtr & type)
{
    const auto type_not_null = removeNullable(type);
    if (checkDataType<DataTypeInt8>(type_not_null.get()))
        return makeSortKeyTypeInfo<Int8, Int64, SortKeyKind::Int64, ColumnVector<Int8>>();
    if (checkDataType<DataTypeInt16>(type_not_null.get()))
        return makeSortKeyTypeInfo<Int16, Int64, SortKeyKind::Int64, ColumnVector<Int16>>();
    if (checkDataType<DataTypeInt32>(type_not_null.get()))
        return makeSortKeyTypeInfo<Int32, Int64, SortKeyKind::Int64, ColumnVector<Int32>>();
    if (checkDataType<DataTypeInt64>(type_not_null.get()))
        return makeSortKeyTypeInfo<Int64, Int64, SortKeyKind::Int64, ColumnVector<Int64>>();
    if (checkDataType<DataTypeUInt8>(type_not_null.get()))
        return makeSortKeyTypeInfo<UInt8, UInt64, SortKeyKind::UInt64, ColumnVector<UInt8>>();
    if (checkDataType<DataTypeUInt16>(type_not_null.get()))
        return makeSortKeyTypeInfo<UInt16, UInt64, SortKeyKind::UInt64, ColumnVector<UInt16>>();
    if (checkDataType<DataTypeUInt32>(type_not_null.get()))
        return makeSortKeyTypeInfo<UInt32, UInt64, SortKeyKind::UInt64, ColumnVector<UInt32>>();
    if (checkDataType<DataTypeUInt64>(type_not_null.get()))
        return makeSortKeyTypeInfo<UInt64, UInt64, SortKeyKind::UInt64, ColumnVector<UInt64>>();
    if (checkDataType<DataTypeFloat32>(type_not_null.get()))
        return makeFloatSortKeyTypeInfo<Float32, SortKeyKind::Float32, ColumnVector<Float32>>();
    if (checkDataType<DataTypeFloat64>(type_not_null.get()))
        return makeFloatSortKeyTypeInfo<Float64, SortKeyKind::Float64, ColumnVector<Float64>>();
    if (checkDataType<DataTypeDecimal32>(type_not_null.get()))
        return makeDecimalSortKeyTypeInfo<Decimal32, SortKeyKind::Decimal32>();
    if (checkDataType<DataTypeDecimal64>(type_not_null.get()))
        return makeDecimalSortKeyTypeInfo<Decimal64, SortKeyKind::Decimal64>();
    if (checkDataType<DataTypeDecimal128>(type_not_null.get()))
        return makeDecimalSortKeyTypeInfo<Decimal128, SortKeyKind::Decimal128>();
    if (checkDataType<DataTypeDecimal256>(type_not_null.get()))
        return makeDecimalSortKeyTypeInfo<Decimal256, SortKeyKind::Decimal256>();
    if (checkDataType<DataTypeDate>(type_not_null.get()))
        return makeSortKeyTypeInfo<UInt16, UInt64, SortKeyKind::Date, ColumnVector<UInt16>>();
    if (checkDataType<DataTypeDateTime>(type_not_null.get()))
        return makeSortKeyTypeInfo<UInt32, UInt64, SortKeyKind::DateTime, ColumnVector<UInt32>>();
    if (checkDataType<DataTypeMyDate>(type_not_null.get()))
        return makeSortKeyTypeInfo<UInt64, UInt64, SortKeyKind::Date, ColumnVector<UInt64>>();
    if (checkDataType<DataTypeMyDateTime>(type_not_null.get()))
        return makeSortKeyTypeInfo<UInt64, UInt64, SortKeyKind::DateTime, ColumnVector<UInt64>>();

    throw Exception("Unsupported order by type for TopN-enhanced multi-stage late materialization: " + type->getName());
}
} // namespace

bool RunningLocalTopN::HeapComparator::operator()(const HeapEntry & lhs, const HeapEntry & rhs) const
{
    RUNTIME_CHECK(owner != nullptr);
    return owner->compareOwnedKeys(lhs.key, rhs.key) < 0;
}

RunningLocalTopN::RunningLocalTopN(
    const MultiStageLateMaterializationTopNDescription & description,
    const ColumnDefines & stage1_columns)
    : topk(description.topk)
    , heap(HeapComparator{this})
{
    RUNTIME_CHECK(topk > 0);
    RUNTIME_CHECK(description.order_by_columns.size() <= multi_stage_late_materialization_topn_max_order_by_columns);

    sort_key_columns.reserve(description.order_by_columns.size());
    for (const auto & order_by_column : description.order_by_columns)
    {
        const auto column_it = std::find_if(stage1_columns.begin(), stage1_columns.end(), [&](const auto & column) {
            return column.id == order_by_column.column_id;
        });
        RUNTIME_CHECK_MSG(
            column_it != stage1_columns.end(),
            "Order by column is not found in stage1 columns, column_id={}",
            order_by_column.column_id);

        auto type_info = getSortKeyTypeInfo(column_it->type);
        sort_key_columns.push_back(SortKeyColumnDesc{
            .column_id = order_by_column.column_id,
            .column_pos = static_cast<size_t>(column_it - stage1_columns.begin()),
            .kind = type_info.kind,
            .direction = order_by_column.direction,
            .build_column_view = type_info.build_column_view,
            .extract_field = type_info.extract_field,
            .compare_column_with_owned = type_info.compare_column_with_owned,
        });
    }
}

int RunningLocalTopN::compareOwnedKeys(const OwnedSortKey & lhs, const OwnedSortKey & rhs) const
{
    RUNTIME_CHECK(lhs.size == rhs.size);
    RUNTIME_CHECK(lhs.size == sort_key_columns.size());
    for (size_t i = 0; i < lhs.size; ++i)
    {
        if (const auto cmp = compareSortKeyFields(lhs.fields[i], rhs.fields[i]); cmp != 0)
            return cmp * sort_key_columns[i].direction;
    }
    return 0;
}

int RunningLocalTopN::compareRowWithOwnedKey(
    const std::vector<SortKeyColumnView> & sort_columns,
    size_t row,
    const OwnedSortKey & rhs) const
{
    RUNTIME_CHECK(sort_columns.size() == sort_key_columns.size());
    RUNTIME_CHECK(rhs.size == sort_key_columns.size());

    for (size_t i = 0; i < sort_key_columns.size(); ++i)
    {
        const auto cmp = sort_key_columns[i].compare_column_with_owned(sort_columns[i], row, rhs.fields[i]);
        if (cmp != 0)
            return cmp * sort_key_columns[i].direction;
    }
    return 0;
}

OwnedSortKey RunningLocalTopN::materializeOwnedKey(const std::vector<SortKeyColumnView> & sort_columns, size_t row)
    const
{
    RUNTIME_CHECK(sort_columns.size() == sort_key_columns.size());

    OwnedSortKey key;
    key.size = sort_key_columns.size();
    for (size_t i = 0; i < sort_key_columns.size(); ++i)
        sort_key_columns[i].extract_field(sort_columns[i], row, key.fields[i]);
    return key;
}

RunningLocalTopNUpdateResult RunningLocalTopN::update(
    const Block & stage1_block,
    const IColumn::Filter * residual_filter,
    size_t residual_passed_rows)
{
    ++current_block_sequence;

    const auto rows = stage1_block.rows();
    RunningLocalTopNUpdateResult result;
    result.filter.resize_fill(rows, 0);

    if (residual_passed_rows == 0)
        return result;

    std::vector<ColumnPtr> materialized_const_columns;
    std::vector<SortKeyColumnView> sort_columns;
    sort_columns.reserve(sort_key_columns.size());
    for (const auto & desc : sort_key_columns)
    {
        auto column = stage1_block.getByPosition(desc.column_pos).column;
        if (auto full_column = column->convertToFullColumnIfConst())
        {
            materialized_const_columns.push_back(std::move(full_column));
            column = materialized_const_columns.back();
        }
        sort_columns.push_back(desc.build_column_view(*column));
    }

    auto mark_candidate = [&](size_t row) {
        if (result.filter[row] == 0)
        {
            result.filter[row] = 1;
            ++result.passed_count;
        }
    };
    auto unmark_current_block_candidate = [&](const HeapEntry & entry) {
        if (entry.block_sequence == current_block_sequence && result.filter[entry.row_index_in_stage1_block] != 0)
        {
            result.filter[entry.row_index_in_stage1_block] = 0;
            --result.passed_count;
        }
    };

    for (size_t row = 0; row < rows; ++row)
    {
        if (residual_filter != nullptr && (*residual_filter)[row] == 0)
            continue;

        if (heap.size() < topk)
        {
            mark_candidate(row);
            heap.push(HeapEntry{
                .key = materializeOwnedKey(sort_columns, row),
                .block_sequence = current_block_sequence,
                .row_index_in_stage1_block = static_cast<UInt32>(row),
            });
            continue;
        }

        const auto cmp = compareRowWithOwnedKey(sort_columns, row, heap.top().key);
        // Equal sort keys are not kept as extra candidates. This matches the existing local TopN behavior,
        // where ties are broken locally by the entries already selected by the partial TopN.
        if (cmp < 0)
        {
            auto evicted = heap.top();
            heap.pop();
            unmark_current_block_candidate(evicted);

            mark_candidate(row);
            heap.push(HeapEntry{
                .key = materializeOwnedKey(sort_columns, row),
                .block_sequence = current_block_sequence,
                .row_index_in_stage1_block = static_cast<UInt32>(row),
            });
        }
    }

    return result;
}

} // namespace DB::DM
