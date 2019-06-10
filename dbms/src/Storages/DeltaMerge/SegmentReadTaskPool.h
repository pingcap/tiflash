#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB
{
namespace DM
{

class SegmentReadTaskPool : private boost::noncopyable
{
public:
    using StreamCreator = std::function<BlockInputStreamPtr(const SegmentPtr & segment)>;
    SegmentReadTaskPool(const Segments & segments_, StreamCreator creator_) : segments(segments_), creator(creator_) {}

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

    Segments      segments;
    StreamCreator creator;
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;

} // namespace DM
} // namespace DB