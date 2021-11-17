#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <common/types.h>
#include <fmt/format.h>

#include <functional>
#include <memory>
#include <unordered_set>

namespace DB
{
inline void throwFailCastException(bool isCasted, const String & source_type, const String & cast_types)
{
    if (!isCasted)
    {
        throw TiFlashException(fmt::format("Fail to cast {} to {}", source_type, cast_types), Errors::Coprocessor::Internal);
    }
}

template <typename FF1, typename FF2>
inline bool elseThen(FF1 ff1, FF2 ff2)
{
    if (!ff1())
    {
        return ff2();
    }
    return true;
}

template <typename T, typename FF>
inline bool castBlockInputStream(const BlockInputStreamPtr & stream_ptr, FF && ff)
{
    if (auto * p_stream = dynamic_cast<T *>(stream_ptr.get()))
    {
        ff(*p_stream);
        return true;
    }
    return false;
}

template <typename FF>
inline void visitBlockInputStreamsWithVisitedSet(std::unordered_set<IBlockInputStream *> & visited_set, const BlockInputStreams & input_streams, FF && ff)
{
    for (const auto & stream_ptr : input_streams)
    {
        auto it = visited_set.find(stream_ptr.get());
        if (it == visited_set.end())
        {
            visited_set.insert(stream_ptr.get());
            ff(stream_ptr);
        }
    }
}

template <typename FF>
inline void visitBlockInputStreams(const BlockInputStreams & input_streams, FF && ff)
{
    std::unordered_set<IBlockInputStream *> visited_set;
    visitBlockInputStreamsWithVisitedSet(visited_set, input_streams, ff);
}

template <typename FF, typename CF>
inline void visitBlockInputStreams(const BlockInputStreams & input_streams, FF && cur_f, CF && child_f)
{
    std::unordered_set<IBlockInputStream *> visited_set;
    visitBlockInputStreamsWithVisitedSet(visited_set, input_streams, [&](const BlockInputStreamPtr & stream_ptr) {
        cur_f(stream_ptr);
        visitBlockInputStreamsWithVisitedSet(visited_set, stream_ptr->getChildren(), child_f);
    });
}

template <typename FF>
inline void visitBlockInputStreamsRecursiveWithVisitedSet(std::unordered_set<IBlockInputStream *> & visited_set, const BlockInputStreams & input_streams, FF && ff)
{
    visitBlockInputStreamsWithVisitedSet(visited_set, input_streams, [&](const BlockInputStreamPtr & stream_ptr) {
        if (!ff(stream_ptr))
        {
            visitBlockInputStreamsRecursiveWithVisitedSet(visited_set, stream_ptr->getChildren(), ff);
        }
    });
}

template <typename FF>
inline void visitBlockInputStreamsRecursive(const BlockInputStreams & input_streams, FF && ff)
{
    std::unordered_set<IBlockInputStream *> visited_set;
    visitBlockInputStreamsRecursiveWithVisitedSet(visited_set, input_streams, ff);
}

inline double divide(size_t value1, size_t value2)
{
    if (0 == value2)
    {
        return 0;
    }
    return 1.0 * value1 / value2;
}
} // namespace DB