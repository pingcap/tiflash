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

#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>

namespace DB
{
Block AggregatingBlockInputStream::getHeader() const
{
    return aggregator.getHeader(final);
}


Block AggregatingBlockInputStream::readImpl()
{
    if (!executed)
    {
        executed = true;
        AggregatedDataVariantsPtr data_variants = std::make_shared<AggregatedDataVariants>();

        CancellationHook hook = [&]() {
            return this->isCancelled();
        };
        aggregator.setCancellationHook(hook);

        aggregator.execute(children.back(), *data_variants, file_provider);

        if (!aggregator.hasTemporaryFiles())
        {
            ManyAggregatedDataVariants many_data{data_variants};
            impl = aggregator.mergeAndConvertToBlocks(many_data, final, 1);
        }
        else
        {
            /** If there are temporary files with partially-aggregated data on the disk,
              *  then read and merge them, spending the minimum amount of memory.
              */

            if (!isCancelled())
            {
                /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
                if (!data_variants->empty())
                    aggregator.writeToTemporaryFile(*data_variants, file_provider);
            }

            const auto & files = aggregator.getTemporaryFiles();
            BlockInputStreams input_streams;
            for (const auto & file : files.files)
            {
                temporary_inputs.emplace_back(std::make_unique<TemporaryFileStream>(file->path(), file_provider));
                input_streams.emplace_back(temporary_inputs.back()->block_in);
            }

            LOG_TRACE(log,
                      "Will merge {} temporary files of size {:.2f} MiB compressed, {:.2f} MiB uncompressed.",
                      files.files.size(),
                      (files.sum_size_compressed / 1048576.0),
                      (files.sum_size_uncompressed / 1048576.0));

            impl = std::make_unique<MergingAggregatedMemoryEfficientBlockInputStream>(input_streams, params, final, 1, 1, log->identifier());
        }
    }

    if (isCancelledOrThrowIfKilled() || !impl)
        return {};

    return impl->read();
}

} // namespace DB
