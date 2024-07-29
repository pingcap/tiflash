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

#include <Common/FmtUtils.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Storages/TableLockHolder.h>

#include <boost/noncopyable.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>


namespace DB
{
class IBlockInputStream;

using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;

struct Progress;

namespace ErrorCodes
{
extern const int OUTPUT_IS_NOT_SORTED;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes


/** Callback to track the progress of the query.
  * Used in IProfilingBlockInputStream and Context.
  * The function takes the number of rows in the last block, the number of bytes in the last block.
  * Note that the callback can be called from different threads.
  */
using ProgressCallback = std::function<void(const Progress & progress)>;

using FilterPtr = IColumn::Filter *;

/** The stream interface for reading data by blocks from the database.
  * Relational operations are supposed to be done also as implementations of this interface.
  */
class IBlockInputStream : private boost::noncopyable
{
public:
    IBlockInputStream() = default;

    /** Get data structure of the stream in a form of "header" block (it is also called "sample block").
      * Header block contains column names, data types, columns of size 0. Constant columns must have corresponding values.
      * It is guaranteed that method "read" returns blocks of exactly that structure.
      */
    virtual Block getHeader() const = 0;

    /** Read next block.
      * If there are no more blocks, return an empty block (for which operator `bool` returns false).
      * NOTE: Only one thread can read from one instance of IBlockInputStream simultaneously.
      * This also applies for readPrefix, readSuffix.
      */
    virtual Block read() = 0;

    /** Read next block.
      * If 'return_filter' is true, and this stream will do some filtering, then this function will return the original block,
      * and returns the filter data by 'res_filter'. The caller is responsible to do filtering itself.
      */
    virtual Block read(FilterPtr & /*res_filter*/, bool /*return_filter*/) { return read(); }

    /** Get information about the last block received.
      */
    virtual BlockExtraInfo getBlockExtraInfo() const
    {
        throw Exception(
            "Method getBlockExtraInfo is not supported by the data stream " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Read something before starting all data or after the end of all data.
      * In the `readSuffix` function, you can implement a finalization that can lead to an exception.
      * readPrefix() must be called before the first call to read().
      * readSuffix() should be called after read() returns an empty block, or after a call to cancel(), but not during read() execution.
      */
    virtual void readPrefix() {}
    virtual void readSuffix() {}

    /** Estimate count of threads to be created through the InputStream.
     *  Note: Since some new threads in some InputStream(e.g. ParallelAggregatingBlockInputStream) won't be clear until runtime.
     *  The result may not be 100% identical to the actual number of threads.
     *  However, most of new threads in certainty are considered
     *  and that's enough to be used to estimate the expected threads load of the system.
     */
    int estimateNewThreadCount()
    {
        int cnt = 0;
        resetNewThreadCountCompute();
        collectNewThreadCount(cnt);
        return cnt;
    }

    /** Estimate the cpu time nanoseconds used by block input stream dag.
      * In this method, streams are divided into two categories:
      * - thread-runner: Called directly by a thread
      * - non-thread-runner: Called by a thread-runner or non-thread-runner
      * Here we should count the execution time of each thread-runner.
      * Note: Because of more threads than vcore, and blocking relationships between streams,
      * the result may not be 100% identical to the actual cpu time nanoseconds.
      */
    uint64_t estimateCPUTimeNs()
    {
        resetCPUTimeCompute();
        // The first stream of stream dag is thread-runner.
        return collectCPUTimeNs(/*is_thread_runner=*/true);
    }

    uint64_t collectCPUTimeNs(bool is_thread_runner);

    virtual ~IBlockInputStream() = default;

    /** To output the data stream transformation tree (query execution plan).
      */
    virtual String getName() const = 0;

    /// If this stream generates data in grouped by some keys, return true.
    virtual bool isGroupedOutput() const { return false; }
    /// If this stream generates data in order by some keys, return true.
    virtual bool isSortedOutput() const { return false; }
    /// In case of isGroupedOutput or isSortedOutput, return corresponding SortDescription
    virtual const SortDescription & getSortDescription() const
    {
        throw Exception("Output of " + getName() + " is not sorted", ErrorCodes::OUTPUT_IS_NOT_SORTED);
    }

    /** Must be called before read, readPrefix.
      */
    void dumpTree(FmtBuffer & buffer, size_t indent = 0, size_t multiplier = 1);

    /** Check the depth of the pipeline.
      * If max_depth is specified and the `depth` is greater - throw an exception.
      * Must be called before read, readPrefix.
      */
    size_t checkDepth(size_t max_depth) const;

    /** Do not allow to drop the table while the blocks stream is alive.
      */
    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }

    void setExtraInfo(String info) { extra_info = std::move(info); }

    template <typename F>
    void forEachChild(F && f)
    {
        /// NOTE: Acquire a read lock, therefore f() should be thread safe
        std::shared_lock lock(children_mutex);

        for (auto & child : children)
            if (f(*child))
                return;
    }

    virtual void collectNewThreadCount(int & cnt)
    {
        if (!thread_cnt_collected)
        {
            thread_cnt_collected = true;
            collectNewThreadCountOfThisLevel(cnt);
            for (auto & child : children)
            {
                if (child)
                    child->collectNewThreadCount(cnt);
            }
        }
    }

    virtual void collectNewThreadCountOfThisLevel(int &) {}

    virtual void appendInfo(FmtBuffer & /*buffer*/) const {};

    virtual bool canHandleSelectiveBlock() const { return false; }

protected:
    virtual uint64_t collectCPUTimeNsImpl(bool /*is_thread_runner*/) { return 0; }

    void resetNewThreadCountCompute()
    {
        if (thread_cnt_collected)
        {
            thread_cnt_collected = false;
            for (auto & child : children)
            {
                if (child)
                    child->resetNewThreadCountCompute();
            }
        }
    }

    void resetCPUTimeCompute()
    {
        if (cpu_time_ns_collected)
        {
            cpu_time_ns_collected = false;
            for (auto & child : children)
            {
                if (child)
                    child->resetCPUTimeCompute();
            }
        }
    }

protected:
    BlockInputStreams children;
    mutable std::shared_mutex children_mutex;
    // flags to avoid duplicated collecting, since some InputStream is shared by multiple inputStreams
    bool thread_cnt_collected = false;
    bool cpu_time_ns_collected = false;

private:
    TableLockHolders table_locks;

    size_t checkDepthImpl(size_t max_depth, size_t level) const;
    mutable std::mutex tree_id_mutex;
    mutable String tree_id;

    /// The info that hints why the inputStream is needed to run.
    String extra_info;

    /// Get text with names of this source and the entire subtree, this function should only be called after the
    /// InputStream tree is constructed.
    String getTreeID() const;
};


} // namespace DB
