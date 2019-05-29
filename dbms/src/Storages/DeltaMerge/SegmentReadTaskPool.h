#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB
{
class SegmentReadTaskPool : private boost::noncopyable
{
public:
    using StreamCreator = std::function<BlockInputStreamPtr(const SegmentPtr & segment)>;
    SegmentReadTaskPool(const Block & header_, const Segments & segments_, StreamCreator creator_)
        : header(header_), segments(segments_), creator(creator_)
    {
    }

    Block getHeader() { return header; }

    BlockInputStreamPtr getTask()
    {
        SegmentPtr segment;
        {
            std::lock_guard<std::mutex> lock(mutex);

            if (segments.empty())
                return {};
            segment = segments.back();
            segments.pop_back();
        }
        return creator(segment);
    }

private:
    std::mutex mutex;

    Block         header;
    Segments      segments;
    StreamCreator creator;
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;

} // namespace DB