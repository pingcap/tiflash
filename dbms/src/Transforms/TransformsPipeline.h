// Copyright 2022 PingCAP, Ltd.
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

#include <Transforms/Transforms.h>

namespace DB
{
struct TransformsPipeline
{
    TransformsPipeline() = default;

    void init(size_t concurrency)
    {
        assert(!has_inited);
        has_inited = true;
        for (size_t i = 0; i < concurrency; ++i)
            transforms_vec.emplace_back(std::make_shared<Transforms>());
    }

    template <typename FF>
    void transform(FF && ff)
    {
        assert(has_inited);
        for (const auto & transforms : transforms_vec)
            ff(transforms);
    }

    size_t concurrency() const
    {
        assert(has_inited);
        return transforms_vec.size();
    }

    Block getHeader()
    {
        assert(has_inited);
        assert(!transforms_vec.empty());
        return transforms_vec.back()->getHeader();
    }

    bool has_inited = false;
    std::vector<TransformsPtr> transforms_vec;
};
} // namespace DB
