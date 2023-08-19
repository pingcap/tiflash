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
struct ArrayConcat : public ArraySinkSourceSelector<ArrayConcat>
{
    using Sources = std::vector<std::unique_ptr<IArraySource>>;

    template <typename Source, typename Sink>
    static void selectSourceSink(Source &&, Sink && sink, const Sources & sources)
    {
        using SourceType = typename std::decay<Source>::type;
        concat<SourceType, Sink>(sources, sink);
    }

    template <typename Source, typename Sink>
    static void selectSourceSink(ConstSource<Source> &&, Sink && sink, const Sources & sources)
    {
        using SourceType = typename std::decay<Source>::type;
        concat<SourceType, Sink>(sources, sink);
    }

    template <typename Source, typename Sink>
    static void selectSourceSink(ConstSource<Source> &, Sink && sink, const Sources & sources)
    {
        using SourceType = typename std::decay<Source>::type;
        concat<SourceType, Sink>(sources, sink);
    }
};

void concat(const std::vector<std::unique_ptr<IArraySource>> & sources, IArraySink & sink)
{
    if (sources.empty())
        throw Exception("Concat function should get at least 1 ArraySource", ErrorCodes::LOGICAL_ERROR);
    return ArrayConcat::select(*sources.front(), sink, sources);
}
} // namespace DB::GatherUtils
