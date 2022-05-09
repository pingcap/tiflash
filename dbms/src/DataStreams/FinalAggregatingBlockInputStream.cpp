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

#include <DataStreams/FinalAggregatingBlockInputStream.h>

namespace DB
{
Block FinalAggregatingBlockInputStream::readImpl()
{
    const auto child = children.back();
    while(child->read()) {}

    if (!isCancelled() && aggregate_store->aggregator.hasTemporaryFiles())
    {
        /// It may happen that some data has not yet been flushed,
        ///  because at the time of `onFinishThread` call, no data has been flushed to disk, and then some were.
        for (auto & data : aggregate_store->many_data)
        {
            if (data->isConvertibleToTwoLevel())
                data->convertToTwoLevel();

            if (!data->empty())
                aggregate_store->aggregator.writeToTemporaryFile(*data, aggregate_store->file_provider);
        }
    }


}
}
