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

#include <Common/randomSeed.h>
#include <DataStreams/ConcatBlockInputStream.h>

#include <pcg_random.hpp>
#include <random>


namespace DB
{
BlockInputStreams narrowBlockInputStreams(BlockInputStreams & inputs, size_t width)
{
    size_t size = inputs.size();
    if (size <= width)
        return inputs;

    std::vector<BlockInputStreams> partitions(width);

    using Distribution = std::vector<size_t>;
    Distribution distribution(size);

    for (size_t i = 0; i < size; ++i)
        distribution[i] = i % width;

    pcg64 generator(randomSeed());
    std::shuffle(distribution.begin(), distribution.end(), generator);

    for (size_t i = 0; i < size; ++i)
        partitions[distribution[i]].push_back(inputs[i]);

    BlockInputStreams res(width);
    for (size_t i = 0; i < width; ++i)
        res[i] = std::make_shared<ConcatBlockInputStream>(partitions[i], /*req_id=*/"");

    return res;
}

} // namespace DB
