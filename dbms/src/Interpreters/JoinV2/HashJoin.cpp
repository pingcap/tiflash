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

#include <Columns/ColumnUtils.h>
#include <Columns/ColumnsCommon.h>
#include <DataStreams/materializeBlock.h>
#include <Interpreters/JoinV2/HashJoin.h>
#include <Interpreters/NullableUtils.h>

namespace DB
{

namespace
{
struct KeyColumns
{
    size_t index;
    const IColumn * column_ptr;
    bool is_nullable;
};
std::vector<KeyColumns> getKeyColumns(const Names & key_names, const Block & block)
{
    size_t keys_size = key_names.size();
    std::vector<KeyColumns> key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns[i].index = block.getPositionByName(key_names[i]);
        key_columns[i].column_ptr = block.getByPosition(key_columns[i].index).column.get();

        /// We will join only keys, where all components are not NULL.
        if (key_columns[i].column_ptr->isColumnNullable())
        {
            key_columns[i].column_ptr
                = &static_cast<const ColumnNullable &>(*key_columns[i].column_ptr).getNestedColumn();
            key_columns[i].is_nullable = true;
        }
    }

    return key_columns;
}

bool canAsColumnString(const IColumn * column)
{
    return typeid_cast<const ColumnString *>(column)
        || (column->isColumnConst()
            && typeid_cast<const ColumnString *>(&static_cast<const ColumnConst *>(column)->getDataColumn()));
}

enum class StringCollatorKind
{
    StringBinary,
    StringBinaryPadding,
    String,
};

StringCollatorKind getStringCollatorKind(const TiDB::TiDBCollators & collators)
{
    if (collators.empty() || !collators[0])
        return StringCollatorKind::StringBinary;

    switch (collators[0]->getCollatorType())
    {
    case TiDB::ITiDBCollator::CollatorType::UTF8MB4_BIN:
    case TiDB::ITiDBCollator::CollatorType::UTF8_BIN:
    case TiDB::ITiDBCollator::CollatorType::LATIN1_BIN:
    case TiDB::ITiDBCollator::CollatorType::ASCII_BIN:
    {
        return StringCollatorKind::StringBinaryPadding;
    }
    case TiDB::ITiDBCollator::CollatorType::BINARY:
    {
        return StringCollatorKind::StringBinary;
    }
    default:
    {
        // for CI COLLATION, use original way
        return StringCollatorKind::String;
    }
    }
}

} // namespace

HashJoin::HashJoin(
    const Names & key_names_left_,
    const Names & key_names_right_,
    ASTTableJoin::Kind kind_,
    const String & req_id,
    const NamesAndTypes & output_columns_,
    const TiDB::TiDBCollators & collators_,
    const JoinNonEqualConditions & non_equal_conditions_,
    size_t max_block_size_)
    : kind(kind_)
    , join_req_id(req_id)
    , key_names_left(key_names_left_)
    , key_names_right(key_names_right_)
    , collators(collators_)
    , non_equal_conditions(non_equal_conditions_)
    , max_block_size(max_block_size_)
    , log(Logger::get(join_req_id))
    , output_columns(output_columns_)
{
    RUNTIME_ASSERT(key_names_left.size() == key_names_right.size());
}

void HashJoin::initRowSchemaAndHashJoinMethod()
{
    size_t keys_size = key_names_right.size();
    if (keys_size == 0)
    {
        method = HashJoinKeyMethod::Cross;
        return;
    }

    auto key_columns = getKeyColumns(key_names_right, right_sample_block);
    RUNTIME_ASSERT(key_columns.size() == keys_size);
    /// Move all raw join key column to the front of the join key.
    Names new_key_names_left, new_key_names_right;
    BoolVec raw_key_flag(keys_size);
    bool is_all_key_fixed = true;
    for (size_t i = 0; i < keys_size; ++i)
    {
        if (key_columns[i].column_ptr->valuesHaveFixedSize())
        {
            new_key_names_left.emplace_back(key_names_left[i]);
            new_key_names_right.emplace_back(key_names_right[i]);
            raw_key_flag[i] = true;
            row_schema.key_column_indexes.push_back(key_columns[i].index);
            row_schema.raw_key_column_indexes.push_back({key_columns[i].index, key_columns[i].is_nullable});
            row_schema.key_column_fixed_size += key_columns[i].column_ptr->sizeOfValueIfFixed();
            continue;
        }
        is_all_key_fixed = false;
        if (canAsColumnString(key_columns[i].column_ptr))
        {
            if (getStringCollatorKind(collators) == StringCollatorKind::StringBinary)
            {
                new_key_names_left.emplace_back(key_names_left[i]);
                new_key_names_right.emplace_back(key_names_right[i]);
                raw_key_flag[i] = true;
                row_schema.key_column_indexes.push_back(key_columns[i].index);
                row_schema.raw_key_column_indexes.push_back({key_columns[i].index, key_columns[i].is_nullable});
                continue;
            }
        }
    }
    if (is_all_key_fixed)
    {
        if (keys_size == 1)
        {
            switch (row_schema.key_column_fixed_size)
            {
            case 1:
                method = HashJoinKeyMethod::OneKey8;
                break;
            case 2:
                method = HashJoinKeyMethod::OneKey16;
                break;
            case 4:
                method = HashJoinKeyMethod::OneKey32;
                break;
            case 8:
                method = HashJoinKeyMethod::OneKey64;
                break;
            case 16:
                method = HashJoinKeyMethod::OneKey128;
                break;
            default:
                method = HashJoinKeyMethod::KeysFixedLonger;
                break;
            }
        }
        else
        {
            switch (row_schema.key_column_fixed_size)
            {
            case 4:
                method = HashJoinKeyMethod::KeysFixed32;
                break;
            case 8:
                method = HashJoinKeyMethod::KeysFixed64;
                break;
            case 16:
                method = HashJoinKeyMethod::KeysFixed128;
                break;
            case 32:
                method = HashJoinKeyMethod::KeysFixed256;
                break;
            default:
                method = HashJoinKeyMethod::KeysFixedLonger;
                break;
            }
        }
    }
    else if (canAsColumnString(key_columns[0].column_ptr))
    {
        switch (getStringCollatorKind(collators))
        {
        case StringCollatorKind::StringBinary:
            method = HashJoinKeyMethod::OneKeyStringBin;
            break;
        case StringCollatorKind::StringBinaryPadding:
            method = HashJoinKeyMethod::OneKeyStringBinPadding;
            break;
        case StringCollatorKind::String:
            method = HashJoinKeyMethod::OneKeyString;
            break;
        }
    }
    else
    {
        // E.g. Decimal256
        method = HashJoinKeyMethod::KeySerialized;
    }

    for (size_t i = 0; i < keys_size; ++i)
    {
        if (!raw_key_flag[i])
        {
            row_schema.key_column_indexes.push_back(key_columns[i].index);
            /// For non-raw join key, the original join key column should be placed in `other_column_indexes`.
            row_schema.other_column_indexes.push_back({key_columns[i].index, false});
            new_key_names_left.emplace_back(key_names_left[i]);
            new_key_names_right.emplace_back(key_names_right[i]);
        }
    }
    key_names_left.swap(new_key_names_left);
    key_names_right.swap(new_key_names_right);

    std::unordered_set<std::string> key_names_right_set;
    for (auto & name : key_names_right)
        key_names_right_set.insert(name);

    for (size_t i = 0; i < right_sample_block.columns(); ++i)
    {
        auto & c = right_sample_block.getByPosition(i);
        if (!key_names_right_set.contains(c.name))
        {
            if (c.column->valuesHaveFixedSize())
            {
                row_schema.other_column_fixed_size += c.column->sizeOfValueIfFixed();
                row_schema.other_column_indexes.push_back({i, true});
            }
            else
            {
                row_schema.other_column_indexes.push_back({i, false});
            }
        }
    }
}

void HashJoin::initBuild(const Block & sample_block, size_t build_concurrency_)
{
    if (unlikely(initialized))
        throw Exception("Logical error: Join has been initialized", ErrorCodes::LOGICAL_ERROR);
    initialized = true;

    right_sample_block = materializeBlock(sample_block);
    size_t num_columns_to_add = right_sample_block.columns();

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        auto & column = right_sample_block.getByPosition(i);
        if (!column.column)
            column.column = column.type->createColumn();
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (isLeftOuterJoin(kind) || kind == ASTTableJoin::Kind::Full)
        for (size_t i = 0; i < num_columns_to_add; ++i)
            convertColumnToNullable(right_sample_block.getByPosition(i));

    initRowSchemaAndHashJoinMethod();

    build_concurrency = build_concurrency_;
    for (size_t i = 0; i < build_concurrency; ++i)
        build_workers_data.emplace_back(std::make_unique<BuildWorkerData>());
    for (size_t i = 0; i < HJ_BUILD_PARTITION_COUNT + 1; ++i)
        partition_column_rows_with_lock.emplace_back(std::make_unique<ColumnRowsWithLock>());
}

void HashJoin::initProbe(const Block & sample_block [[maybe_unused]], size_t probe_concurrency_ [[maybe_unused]]) {}

void HashJoin::insertFromBlock(const Block & b, size_t stream_index)
{
    if unlikely (b.rows() == 0)
        return;

    if (unlikely(!initialized))
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);

    Block block = b;
    const NameSet & probe_output_name_set = has_other_condition
        ? output_columns_names_set_for_other_condition_after_finalize
        : output_column_names_set_after_finalize;

    size_t columns = block.columns();
    for (size_t pos = 0; pos < columns;)
    {
        if (!probe_output_name_set.contains(block.getByPosition(pos).name))
            block.erase(pos);
        else
            ++pos;
    }

    RUNTIME_CHECK_MSG(
        block.columns() == right_sample_block.columns(),
        "block columns {} does not equal to sample block columns {}",
        block.columns(),
        right_sample_block.columns());

    size_t rows = block.rows();

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    /// Note: this variable can't be removed because it will take smart pointers' lifecycle to the end of this function.
    Columns materialized_columns;
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names_right);

    /// We will insert to the map only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);
    /// Reuse null_map to record the filtered rows, the rows contains NULL or does not
    /// match the join filter will not insert to the maps
    recordFilteredRows(block, non_equal_conditions.right_filter_column, null_map_holder, null_map);

    columns = block.columns();

    /// Rare case, when joined columns are constant. To avoid code bloat, simply materialize them.
    for (size_t i = 0; i < columns; ++i)
    {
        ColumnPtr col = block.safeGetByPosition(i).column;
        if (ColumnPtr converted = col->convertToFullColumnIfConst())
            block.safeGetByPosition(i).column = converted;
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (isLeftOuterJoin(kind) || kind == ASTTableJoin::Kind::Full)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            convertColumnToNullable(block.getByPosition(i));
        }
    }

    if (!isCrossJoin(kind))
    {
        convertBlockToRows(
            method,
            needRecordNotInsertRows(kind),
            block,
            rows,
            key_columns,
            collators,
            null_map,
            row_schema,
            partition_column_rows_with_lock,
            *build_workers_data[stream_index]);
    }
}

void HashJoin::finalize(const Names & parent_require [[maybe_unused]]) {}

} // namespace DB