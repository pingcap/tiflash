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

#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class ProcessListEntry;

struct BlockIO
{
    /** process_list_entry should be destroyed after in and after out,
      *  since in and out contain pointer to an object inside process_list_entry
      *  (MemoryTracker * current_memory_tracker),
      *  which could be used before destroying of in and out.
      */
    std::shared_ptr<ProcessListEntry> process_list_entry;

    BlockInputStreamPtr in;
    BlockOutputStreamPtr out;

    /// Callbacks for query logging could be set here.
    std::function<void(IBlockInputStream *, IBlockOutputStream *)> finish_callback;
    std::function<void()> exception_callback;

    /// Call these functions if you want to log the request.
    void onFinish()
    {
        if (finish_callback)
            finish_callback(in.get(), out.get());
    }

    void onException() const
    {
        if (exception_callback)
            exception_callback();
    }

    BlockIO & operator=(const BlockIO & rhs)
    {
        if (this == std::addressof(rhs))
        {
            return *this;
        }

        /// We provide the correct order of destruction.
        out = nullptr;
        in = nullptr;
        process_list_entry = nullptr;

        process_list_entry = rhs.process_list_entry;
        in = rhs.in;
        out = rhs.out;

        finish_callback = rhs.finish_callback;
        exception_callback = rhs.exception_callback;

        return *this;
    }

    BlockIO() = default;
    BlockIO(const BlockIO & that) = default;
    ~BlockIO() = default;
};

} // namespace DB
