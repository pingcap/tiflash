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

#pragma once

#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/DistanceProjectionInputStream_fwd.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{

/// Calculates a distance based on the vector column. The vector column must be placed as the last column in
/// the underlying stream. The distance column will replace the original vector column.
/// Eg: [a, b, vec] -> [a, b, dis]
class DistanceProjectionInputStream : public NopSkippableBlockInputStream
{
public:
    DistanceProjectionInputStream(SkippableBlockInputStreamPtr input_, VectorIndexStreamCtxPtr ctx_);

    static DistanceProjectionInputStreamPtr create(SkippableBlockInputStreamPtr input, VectorIndexStreamCtxPtr ctx);

public: // Implements IBlockInputStream
    String getName() const override { return "ProjectionDistanceColumn"; }

    Block getHeader() const override { return ctx->header; }

    Block read() override;

private:
    VectorIndexStreamCtxPtr ctx;

    /// Calculate the distance and replace the vector column with distance result.
    void transform(Block & block);

    // Indicate which columns are involved in the calculation in the transform function.
    static const ColumnNumbers ARG_INDICES;
};

} // namespace DB::DM
