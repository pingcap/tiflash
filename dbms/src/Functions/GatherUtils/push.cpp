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
struct ArrayPush : public ArrayAndValueSourceSelectorBySink<ArrayPush>
{
    template <typename ArraySource, typename ValueSource, typename Sink>
    static void selectArrayAndValueSourceBySink(
        ArraySource && array_source,
        ValueSource && value_source,
        Sink && sink,
        bool push_front)
    {
        if (push_front)
            concat(value_source, array_source, sink);
        else
            concat(array_source, value_source, sink);
    }
};


void push(IArraySource & array_source, IValueSource & value_source, IArraySink & sink, bool push_front)
{
    ArrayPush::select(sink, array_source, value_source, push_front);
}
} // namespace DB::GatherUtils
