#pragma once

#include <Common/MemoryTracker.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Interpreters/ExpressionAnalyzer.h> /// SubqueriesForSets


namespace DB
{
class DAGContext;
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
        const SizeLimits & network_transfer_limits);

    CreatingSetsBlockInputStream(
        const BlockInputStreamPtr & input,
        std::vector<SubqueriesForSets> && subqueries_for_sets_list_,
        const SizeLimits & network_transfer_limits,
        DAGContext * dag_context_);

    ~CreatingSetsBlockInputStream()
    {
        for (auto & worker : workers)
        {
            if (worker.joinable())
                worker.join();
        }
    }

    static constexpr auto name = "CreatingSets";

    String getName() const override { return name; }

    Block getHeader() const override { return children.back()->getHeader(); }

    /// Takes `totals` only from the main source, not from subquery sources.
    Block getTotals() override;

    virtual void collectNewThreadCountOfThisLevel(int & cnt) override
    {
        if (!children.empty())
        {
            cnt += (children.size() - 1);
        }
    }

    virtual void collectNewThreadCount(int & cnt) override
    {
        if (!collected)
        {
            int cnt_s1 = 0;
            int cnt_s2 = 0;
            collected = true;
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

private:
    void init(const BlockInputStreamPtr & input);

    std::vector<SubqueriesForSets> subqueries_for_sets_list;
    bool created = false;

    SizeLimits network_transfer_limits;

    size_t rows_to_transfer = 0;
    size_t bytes_to_transfer = 0;

    std::vector<std::thread> workers;
    std::mutex exception_mutex;
    std::vector<std::exception_ptr> exception_from_workers;

    DAGContext * dag_context;
    MPPTaskId mpp_task_id = MPPTaskId::unknown_mpp_task_id;
    const LogWithPrefixPtr log;

    void createAll();
    void createOne(SubqueryForSet & subquery);
};

} // namespace DB
