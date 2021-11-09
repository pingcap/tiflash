#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <common/types.h>

#include <memory>
#include <unordered_set>
#include <functional>

namespace DB
{
template<typename FF, typename CF>
inline void visitProfileStreamsInfo(const ProfileStreamsInfo & profile_streams_info, FF && cur_f, CF && child_f)
{
    std::unordered_set<IProfilingBlockInputStream *> visited_set;
    for (const auto & stream_ptr : profile_streams_info.input_streams)
    {
        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream_ptr.get()))
        {
            cur_f(p_stream->getProfileInfo());

            p_stream->forEachProfilingChild([&] (IProfilingBlockInputStream & child)
            {
                auto it = visited_set.find(&child);
                if (it == visited_set.end())
                {
                    visited_set.insert(&child);
                    child_f(child.getProfileInfo());
                }
                return false;
            });
        }
    }
}

template<typename FF>
inline void visitProfileStreamsInfo(const ProfileStreamsInfo & profile_streams_info, FF && ff)
{
    std::unordered_set<IProfilingBlockInputStream *> visited_set;
    for (const auto & stream_ptr : profile_streams_info.input_streams)
    {
        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream_ptr.get()))
        {
            ff(p_stream->getProfileInfo());
        }
    }
}

inline double divide(size_t value1, size_t value2)
{
    if (0 == value2)
    {
        return 0;
    }
    return 1.0 * value1 / value2;
}
}