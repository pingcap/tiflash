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

#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/Selectors.h>

namespace DB::GatherUtils
{
struct SliceFromRightConstantOffsetUnboundedSelectArraySource
    : public ArraySinkSourceSelector<SliceFromRightConstantOffsetUnboundedSelectArraySource>
{
    template <typename Source, typename Sink>
    static void selectSourceSink(Source && source, Sink && sink, size_t & offset)
    {
        sliceFromRightConstantOffsetUnbounded(source, sink, offset);
    }
};

void sliceFromRightConstantOffsetUnbounded(IArraySource & src, IArraySink & sink, size_t offset)
{
    SliceFromRightConstantOffsetUnboundedSelectArraySource::select(src, sink, offset);
}
} // namespace DB::GatherUtils
