#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{
class SegmentReadTaskPool : private boost::noncopyable
{
public:
    SegmentReadTaskPool(const BlockInputStreams & inputs_) : inputs(inputs_), header(inputs.back()->getHeader()) {}

    Block getHeader() { return header; }

    BlockInputStreamPtr getTask()
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (inputs.empty())
            return {};
        auto res = inputs.back();
        inputs.pop_back();
        return res;
    }

private:
    std::mutex mutex;

    BlockInputStreams inputs;
    Block             header;
};

using SegmentReadTaskPoolPtr = std::shared_ptr<SegmentReadTaskPool>;

} // namespace DB