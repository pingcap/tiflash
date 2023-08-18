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

#include <Common/MemoryTracker.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Interpreters/SubqueryForSet.h>


namespace DB
{
/** Returns the data from the stream of blocks without changes, but
  * in the `readPrefix` function or before reading the first block
  * initializes all the passed sets.
  */
class CreatingSetsBlockInputStream : public IProfilingBlockInputStream
{
public:
    CreatingSetsBlockInputStream(
        const BlockInputStreamPtr & input,
        const SubqueriesForSets & subqueries_for_sets_,
        const SizeLimits & network_transfer_limits,
        const String & req_id);

    CreatingSetsBlockInputStream(
        const BlockInputStreamPtr & input,
        std::vector<SubqueriesForSets> && subqueries_for_sets_list_,
        const SizeLimits & network_transfer_limits,
        const String & req_id);

    ~CreatingSetsBlockInputStream() = default;

    static constexpr auto name = "CreatingSets";

    String getName() const override { return name; }

    Block getHeader() const override { return children.back()->getHeader(); }

    virtual void collectNewThreadCountOfThisLevel(int & cnt) override
    {
        if (!children.empty())
        {
            cnt += (children.size() - 1);
        }
    }

    virtual void collectNewThreadCount(int & cnt) override
    {
        if (!thread_cnt_collected)
        {
            int cnt_s1 = 0;
            int cnt_s2 = 0;
            thread_cnt_collected = true;
            collectNewThreadCountOfThisLevel(cnt_s1);
            for (int i = 0; i < static_cast<int>(children.size()) - 1; ++i)
            {
                auto & child = children[i];
                if (child)
                    child->collectNewThreadCount(cnt_s1);
            }
            children.back()->collectNewThreadCount(cnt_s2);
            cnt += std::max(cnt_s1, cnt_s2);
        }
    }

protected:
    Block readImpl() override;
    void readPrefixImpl() override;

    uint64_t collectCPUTimeNsImpl(bool is_thread_runner) override;

private:
    void init(const BlockInputStreamPtr & input);

    std::vector<SubqueriesForSets> subqueries_for_sets_list;
    bool created = false;

    SizeLimits network_transfer_limits;

    size_t rows_to_transfer = 0;
    size_t bytes_to_transfer = 0;

    std::mutex exception_mutex;
    std::vector<std::exception_ptr> exception_from_workers;

    const LoggerPtr log;

    void createAll();
    void createOne(SubqueryForSet & subquery);
};

} // namespace DB
