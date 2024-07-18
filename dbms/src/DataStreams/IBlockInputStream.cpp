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

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <math.h>


namespace DB
{
namespace ErrorCodes
{
extern const int TOO_DEEP_PIPELINE;
}

/** It's safe to access children without mutex as long as these methods are called before first call to read, readPrefix.
  */


String IBlockInputStream::getTreeID() const
{
    std::lock_guard lock(tree_id_mutex);
    if (tree_id.empty())
    {
        FmtBuffer buffer;
        buffer.append(getName());

        if (!children.empty())
        {
            buffer.append("(");
            buffer.joinStr(
                children.cbegin(),
                children.cend(),
                [](const auto & r, FmtBuffer & fb) { fb.append(r->getTreeID()); },
                ", ");
            buffer.append(")");
        }
        tree_id = buffer.toString();
    }

    return tree_id;
}


size_t IBlockInputStream::checkDepth(size_t max_depth) const
{
    return checkDepthImpl(max_depth, max_depth);
}

size_t IBlockInputStream::checkDepthImpl(size_t max_depth, size_t level) const
{
    if (children.empty())
        return 0;

    if (level > max_depth)
        throw Exception(
            fmt::format("Query pipeline is too deep. Maximum: {}", max_depth),
            ErrorCodes::TOO_DEEP_PIPELINE);

    size_t res = 0;
    for (const auto & child : children)
    {
        size_t child_depth = child->checkDepth(level + 1);
        if (child_depth > res)
            res = child_depth;
    }

    return res + 1;
}

void IBlockInputStream::dumpTree(FmtBuffer & buffer, size_t indent, size_t multiplier)
{
    buffer.fmtAppend("{}{}{}", String(indent, ' '), getName(), multiplier > 1 ? fmt::format(" x {}", multiplier) : "");
    if (!extra_info.empty())
        buffer.fmtAppend(": <{}>", extra_info);
    buffer.fmtAppend("; header: {}", getHeader().dumpStructure());
    appendInfo(buffer);
    buffer.append("\n");
    ++indent;

    /// If the subtree is repeated several times, then we output it once with the multiplier.
    using Multipliers = std::map<String, size_t>;
    Multipliers multipliers;

    for (const auto & child : children)
        ++multipliers[child->getTreeID()];

    for (const auto & child : children)
    {
        String id = child->getTreeID();
        size_t & subtree_multiplier = multipliers[id];
        if (subtree_multiplier != 0) /// Already printed subtrees are marked with zero in the array of multipliers.
        {
            child->dumpTree(buffer, indent, subtree_multiplier);
            subtree_multiplier = 0;
        }
    }
}

uint64_t IBlockInputStream::collectCPUTimeNs(bool is_thread_runner)
{
    if (cpu_time_ns_collected)
        return 0;

    cpu_time_ns_collected = true;
    return collectCPUTimeNsImpl(is_thread_runner);
}
} // namespace DB
