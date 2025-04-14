// Copyright 2025 PingCAP, Inc.
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

#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsVector.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/DistanceProjectionInputStream.h>
#include <Storages/DeltaMerge/convertColumnTypeHelpers.h>

namespace DB::DM
{

const ColumnNumbers DistanceProjectionInputStream::ARG_INDICES = {0, 1};

DistanceProjectionInputStream::DistanceProjectionInputStream(
    SkippableBlockInputStreamPtr input_,
    VectorIndexStreamCtxPtr ctx_)
    : ctx(ctx_)
{
    const auto & inner_header = input_->getHeader();
    // The inner stream should contain vec col at last, and this stream will replace the vec col into distance col.
    RUNTIME_CHECK(inner_header.columns() >= 1);
    RUNTIME_CHECK(inner_header.columns() == getHeader().columns());
    RUNTIME_CHECK(ctx_->dis_ctx.has_value() && ctx_->dis_ctx->col_defs_no_index != nullptr);
    // check if the last position of column is a vector type a vector type is Array<Float32>
    const auto last_col_type = inner_header.safeGetByPosition(inner_header.columns() - 1).type;
    RUNTIME_CHECK(checkDataTypeArray<DataTypeFloat32>(removeNullable(last_col_type).get()));

    ctx_->prepareForDistanceProjStream();
    children.push_back(input_);
}

DistanceProjectionInputStreamPtr DistanceProjectionInputStream::create(
    SkippableBlockInputStreamPtr input,
    VectorIndexStreamCtxPtr ctx)
{
    return std::make_shared<DistanceProjectionInputStream>(input, ctx);
}

void DistanceProjectionInputStream::transform(Block & block)
{
    // This input stream only accepts vector column at the end of the schema, and distance column will replace it.
    RUNTIME_CHECK(block);
    size_t idx = block.columns() - 1;

    auto const & dis_ctx = ctx->dis_ctx.value();

    // create ref_vec_col for distance_function execute.
    auto ref_vec_col = ColumnConst::create(dis_ctx.ref_vec_col, block.rows());

    // build block structure
    Block calc_block;
    const auto & vec_type = (*dis_ctx.col_defs_no_index)[idx].type;
    calc_block.insert({
        block.safeGetByPosition(idx).column,
        vec_type,
    });
    calc_block.insert({
        std::move(ref_vec_col),
        vec_type,
    });

    // calculate the distance result
    auto result_type = dis_ctx.distance_fn->getReturnTypeImpl({vec_type, vec_type});

    MutableColumnPtr result_col = result_type->createColumn();
    result_col->reserve(block.rows());
    calc_block.insert({
        std::move(result_col),
        result_type,
    });

    dis_ctx.distance_fn->executeImpl(calc_block, ARG_INDICES, 2);

    // Replace vec column with the distance column
    block.erase(idx);
    block.insert(ColumnWithTypeAndName{
        std::move(calc_block.safeGetByPosition(2).column),
        dis_ctx.dis_cd.type,
        dis_ctx.dis_cd.name,
        dis_ctx.dis_cd.id});
}

Block DistanceProjectionInputStream::read()
{
    Block res = children.back()->read();
    if (!res)
        return res;

    transform(res);

    return res;
}

} // namespace DB::DM
