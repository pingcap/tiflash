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

#include <Common/FailPoint.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/materializeBlock.h>
#include <Interpreters/Join.h>
#include <Interpreters/Set.h>
#include <Storages/IStorage.h>

#include <iomanip>


namespace DB
{
namespace FailPoints
{
extern const char exception_in_creating_set_input_stream[];
extern const char exception_mpp_hash_build[];
} // namespace FailPoints
namespace ErrorCodes
{
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

CreatingSetsBlockInputStream::CreatingSetsBlockInputStream(
    const BlockInputStreamPtr & input,
    std::vector<SubqueriesForSets> && subqueries_for_sets_list_,
    const SizeLimits & network_transfer_limits,
    const String & req_id)
    : subqueries_for_sets_list(std::move(subqueries_for_sets_list_))
    , network_transfer_limits(network_transfer_limits)
    , log(Logger::get(req_id))
{
    init(input);
}

CreatingSetsBlockInputStream::CreatingSetsBlockInputStream(
    const BlockInputStreamPtr & input,
    const SubqueriesForSets & subqueries_for_sets,
    const SizeLimits & network_transfer_limits,
    const String & req_id)
    : network_transfer_limits(network_transfer_limits)
    , log(Logger::get(req_id))
{
    subqueries_for_sets_list.push_back(subqueries_for_sets);
    init(input);
}

void CreatingSetsBlockInputStream::init(const BlockInputStreamPtr & input)
{
    for (auto & subqueries_for_sets : subqueries_for_sets_list)
    {
        for (auto & elem : subqueries_for_sets)
        {
            if (elem.second.source)
            {
                children.push_back(elem.second.source);

                if (elem.second.set)
                    elem.second.set->setHeader(elem.second.source->getHeader());
            }
        }
    }

    children.push_back(input);
}


Block CreatingSetsBlockInputStream::readImpl()
{
    Block res;

    RUNTIME_CHECK(created == true);

    if (isCancelledOrThrowIfKilled())
        return res;

    return children.back()->read();
}


void CreatingSetsBlockInputStream::readPrefixImpl()
{
    createAll();
}


Block CreatingSetsBlockInputStream::getTotals()
{
    auto * input = dynamic_cast<IProfilingBlockInputStream *>(children.back().get());

    if (input)
        return input->getTotals();
    else
        return totals;
}


void CreatingSetsBlockInputStream::createAll()
{
    if (!created)
    {
        for (auto & subqueries_for_sets : subqueries_for_sets_list)
        {
            for (auto & elem : subqueries_for_sets)
            {
                if (elem.second.join)
                    elem.second.join->setBuildTableState(Join::BuildTableState::WAITING);
            }
        }
        Stopwatch watch;
        auto thread_manager = newThreadManager();
        try
        {
            for (auto & subqueries_for_sets : subqueries_for_sets_list)
            {
                for (auto & elem : subqueries_for_sets)
                {
                    if (elem.second.source) /// There could be prepared in advance Set/Join - no source is specified for them.
                    {
                        if (isCancelledOrThrowIfKilled())
                        {
                            thread_manager->wait();
                            return;
                        }
                        thread_manager->schedule(true, "CreatingSets", [this, &item = elem.second] { createOne(item); });
                        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_in_creating_set_input_stream);
                    }
                }
            }
        }
        catch (...)
        {
            thread_manager->wait();
            throw;
        }

        thread_manager->wait();

        if (!exception_from_workers.empty())
        {
            LOG_ERROR(
                log,
                "Creating all tasks takes {} sec with exception and rethrow the first of total {} exceptions",
                watch.elapsedSeconds(),
                exception_from_workers.size());
            std::rethrow_exception(exception_from_workers.front());
        }
        LOG_INFO(
            log,
            "Creating all tasks takes {} sec. ",
            watch.elapsedSeconds());

        created = true;
    }
}

void CreatingSetsBlockInputStream::createOne(SubqueryForSet & subquery)
{
    auto gen_log_msg = [&subquery] {
        if (subquery.set)
            return "Creating set. ";
        if (subquery.join)
            return "Creating join. ";
        if (subquery.table)
            return "Filling temporary table. ";
        return "null subquery";
    };
    Stopwatch watch;
    try
    {
        LOG_INFO(log, "{}", gen_log_msg());
        BlockOutputStreamPtr table_out;
        if (subquery.table)
            table_out = subquery.table->write({}, {});

        bool done_with_set = !subquery.set;
        bool done_with_join = !subquery.join;
        bool done_with_table = !subquery.table;

        if (done_with_set && done_with_join && done_with_table)
            throw Exception("Logical error: nothing to do with subquery", ErrorCodes::LOGICAL_ERROR);

        if (table_out)
            table_out->writePrefix();

        while (Block block = subquery.source->read())
        {
            if (isCancelled())
            {
                LOG_WARNING(log, "Query was cancelled during set / join or temporary table creation.");
                return;
            }

            if (!done_with_set)
            {
                if (!subquery.set->insertFromBlock(block, /*fill_set_elements=*/false))
                    done_with_set = true;
            }

            if (!done_with_table)
            {
                block = materializeBlock(block);
                table_out->write(block);

                rows_to_transfer += block.rows();
                bytes_to_transfer += block.bytes();

                if (!network_transfer_limits.check(
                        rows_to_transfer,
                        bytes_to_transfer,
                        "IN/JOIN external table",
                        ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
                    done_with_table = true;
            }

            if (done_with_set && done_with_join && done_with_table)
            {
                if (auto * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*subquery.source))
                    profiling_in->cancel(false);

                break;
            }
        }


        if (subquery.join)
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_mpp_hash_build);
            subquery.join->setBuildTableState(Join::BuildTableState::SUCCEED);
        }

        if (table_out)
            table_out->writeSuffix();

        watch.stop();

        size_t head_rows = 0;
        if (auto * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*subquery.source))
        {
            const BlockStreamProfileInfo & profile_info = profiling_in->getProfileInfo();

            head_rows = profile_info.rows;

            if (subquery.join)
                subquery.join->setTotals(profiling_in->getTotals());
        }
        if (subquery.join)
            head_rows = subquery.join->getTotalBuildInputRows();

        // avoid generate log message when log level > INFO.
        auto gen_finish_log_msg = [&] {
            FmtBuffer msg;
            msg.append("Created. ");

            if (subquery.set)
                msg.fmtAppend("Set with {} entries from {} rows. ", head_rows > 0 ? subquery.set->getTotalRowCount() : 0, head_rows);
            if (subquery.join)
                msg.fmtAppend("Join with {} entries from {} rows. ", head_rows > 0 ? subquery.join->getTotalRowCount() : 0, head_rows);
            if (subquery.table)
                msg.fmtAppend("Table with {} rows. ", head_rows);

            msg.fmtAppend("In {:.3f} sec. ", watch.elapsedSeconds());
            msg.fmtAppend("using {} threads.", subquery.join ? subquery.join->getBuildConcurrency() : 1);
            return msg.toString();
        };

        LOG_INFO(log, "{}", gen_finish_log_msg());
    }
    catch (...)
    {
        std::unique_lock lock(exception_mutex);
        exception_from_workers.push_back(std::current_exception());
        if (subquery.join)
            subquery.join->setBuildTableState(Join::BuildTableState::FAILED);
        LOG_ERROR(log, "{} throw exception: {} In {} sec. ", gen_log_msg(), getCurrentExceptionMessage(false, true), watch.elapsedSeconds());
    }
}

} // namespace DB
