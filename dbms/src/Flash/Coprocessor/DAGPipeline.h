#pragma once

#include <DataStreams/IBlockInputStream.h>

namespace DB
{
struct DAGPipeline
{
    BlockInputStreams streams;
    /** When executing FULL or RIGHT JOIN, there will be a data stream from which you can read "not joined" rows.
      * It has a special meaning, since reading from it should be done after reading from the main streams.
      * It is appended to the main streams in UnionBlockInputStream or ParallelAggregatingBlockInputStream.
      */
    BlockInputStreams streams_with_non_joined_data;

    BlockInputStreamPtr & firstStream() { return streams.at(0); }

    template <typename Transform>
    void transform(Transform && transform)
    {
        for (auto & stream : streams)
            transform(stream);
        for (auto & stream : streams_with_non_joined_data)
            transform(stream);
    }

    bool hasMoreThanOneStream() const { return streams.size() + streams_with_non_joined_data.size() > 1; }
};
} // namespace

