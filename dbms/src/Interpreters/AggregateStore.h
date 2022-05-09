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

#include <Interpreters/Aggregator.h>

#include <memory>

namespace DB
{
struct AggregateStore
{
    AggregateStore(
        const Aggregator::Params & params,
        size_t max_threads,
        const FileProviderPtr & file_provider_,
        bool is_final_)
        : aggregator(params)
        , file_provider(file_provider_)
        , is_final(is_final_)
    {
        many_data.resize(max_threads);
        for (auto & elem : many_data)
            elem = std::make_shared<AggregatedDataVariants>();
    }

    Aggregator aggregator;
    ManyAggregatedDataVariants many_data;

    FileProviderPtr file_provider;

    bool is_final;

    size_t maxThreads() const { return many_data.size(); }

    Block getHeader() const { return aggregator.getHeader(is_final); }
};

using AggregateStorePtr = std::shared_ptr<AggregateStore>;
}
