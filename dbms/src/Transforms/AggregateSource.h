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

#include <Transforms/FinalAggregateReader.h>
#include <Transforms/Source.h>

namespace DB
{
class AggregateSource : public Source
{
public:
    explicit AggregateSource(
        const FinalAggregateReaderPtr & final_agg_reader_)
        : final_agg_reader(final_agg_reader_)
    {}

    Block read() override
    {
        return final_agg_reader->read();
    }

private:
    FinalAggregateReaderPtr final_agg_reader;
};
} // namespace DB
