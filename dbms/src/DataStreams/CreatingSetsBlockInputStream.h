#pragma once

#include <Common/MemoryTracker.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/ExpressionAnalyzer.h> /// SubqueriesForSets
#include <Poco/Logger.h>


namespace Poco
{
class Logger;
}

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
        const BlockInputStreamPtr & input, const SubqueriesForSets & subqueries_for_sets_, const SizeLimits & network_transfer_limits);

    CreatingSetsBlockInputStream(const BlockInputStreamPtr & input,
        std::vector<SubqueriesForSets> && subqueries_for_sets_list_,
        const SizeLimits & network_transfer_limits, Int64 mpp_task_id_);
    ~CreatingSetsBlockInputStream()
    {
        for (auto & worker : workers)
        {
            if (worker.joinable())
                worker.join();
        }
    }

    String getName() const override { return "CreatingSets"; }

    Block getHeader() const override { return children.back()->getHeader(); }

    /// Takes `totals` only from the main source, not from subquery sources.
    Block getTotals() override;

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
    Int64 mpp_task_id = 0;

    std::vector<std::thread> workers;
    std::mutex exception_mutex;
    std::vector<std::exception_ptr> exception_from_workers;

    using Logger = Poco::Logger;
    Logger * log = &Logger::get("CreatingSetsBlockInputStream");

    void createAll();
    void createOne(SubqueryForSet & subquery, MemoryTracker * memory_tracker);
};

} // namespace DB
