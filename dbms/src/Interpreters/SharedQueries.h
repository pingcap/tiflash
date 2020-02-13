#pragma once

#include <mutex>

#include <Common/FunctionTimerTask.h>
#include <Poco/Util/Timer.h>
#include <common/logger_useful.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/EmptyBlockInputStream.h>
#include <DataStreams/SharedQueryBlockInputStream.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TIFLASH_BAD_REQUEST;
}

struct SharedQuery
{
    static constexpr Int64 finished_expired_microseconds   = 20L * 60 * 1000 * 1000;      // 20 minutes
    static constexpr Int64 unfinished_expired_microseconds = 12L * 60 * 60 * 1000 * 1000; // 12 hours

    SharedQuery(String query_id_, size_t clients_, const BlockInputStreamPtr & in)
        : query_id(query_id_), clients(clients_), log(&Logger::get("SharedQuery"))
    {
        LOG_TRACE(log, "Create SharedQuery(" << query_id << ")");
        /// We only share BlockInputStream between clients,
        /// other resources in BlockIO should only be used by current thread.
        io.in = in;
    }

    void onClientFinish()
    {
        if (!finished_clients)
        {
            /// Replace the real input stream with a fake one. To release the resources.
            io.in = std::make_shared<EmptyBlockInputStream>(io.in->getHeader());
        }
        ++finished_clients;
        last_finish_time = Poco::Timestamp();

        LOG_TRACE(
            log, "onClientFinish, SharedQuery(" << query_id << "), clients:" << clients << ", finished_clients: " << finished_clients);
    }

    bool isDone()
    {
        /// Some clients connected and consumed all data, and we already waited long enough.
        /// Or This cache exists for too long.
        ///
        /// We keep shared query infos as tombstones here even after they are finished, to stop clients with the same query id comming in.
        Poco::Timestamp now;
        return (finished_clients && (last_finish_time + finished_expired_microseconds) <= now)
            || (last_finish_time + unfinished_expired_microseconds) <= now;
    }

    String  query_id;
    size_t  clients;
    BlockIO io;

    size_t          connected_clients{1};
    size_t          finished_clients{0};
    Poco::Timestamp last_finish_time{};

    Logger * log;
};

using SharedQueryPtr = std::shared_ptr<SharedQuery>;

class SharedQueries
{
public:
    using BlockIOCreator = std::function<BlockIO()>;
    using Queries        = std::unordered_map<String, SharedQueryPtr>;

    BlockIO getOrCreateBlockIO(String query_id, size_t clients, BlockIOCreator creator)
    {
        if (!clients)
            throw Exception("Illegal client count: " + toString(clients));

        std::lock_guard<std::mutex> lock(mutex);

        const auto it = queries.find(query_id);
        if (it != queries.end())
        {
            if (clients != it->second->clients)
            {
                LOG_WARNING(log,
                    "Different client numbers between shared queries with same query_id(" //
                        << query_id << "), former: " << it->second->clients << ", now: " << clients);
            }
            auto & query = *(it->second);
            if (query.connected_clients >= clients)
            {
                query.connected_clients++;
                String msg = "SharedQueries: more clients than they claimed! expected " + toString(clients) + ", got "
                    + toString(query.connected_clients);
                LOG_WARNING(log, msg);
                throw Exception(msg, ErrorCodes::TIFLASH_BAD_REQUEST);
            }
            query.connected_clients++;

            LOG_TRACE(log,
                "getOrCreateBlockIO, query_id: " << query_id << ", clients: " << clients
                                                 << ", connected_clients: " << query.connected_clients);

            return query.io;
        }
        else
        {
            BlockIO io = creator();
            io.in      = std::make_shared<SharedQueryBlockInputStream>(clients, io.in);
            queries.emplace(query_id, std::make_shared<SharedQuery>(query_id, clients, io.in));

            LOG_TRACE(log, "getOrCreateBlockIO, query_id: " << query_id << ", clients: " << clients << ", connected_clients: " << 1);

            return io;
        }
    }

    void onSharedQueryFinish(String query_id)
    {
        std::lock_guard<std::mutex> lock(mutex);

        const auto it = queries.find(query_id);
        if (it == queries.end())
        {
            LOG_WARNING(log,
                "Shared query finished with query_id(" //
                    << query_id << "), while resource cache not exists."
                    << " Maybe this client takes too long before finish");
            return;
        }
        auto & query = *(it->second);
        query.onClientFinish();

        //        if (it->second->isDone())
        //        {
        //            LOG_TRACE(log, "Remove shared query(" << it->second->query_id << ")");
        //            queries.erase(it);
        //        }
    }

    void checkAll()
    {
        std::lock_guard<std::mutex> lock(mutex);

        for (auto it = queries.begin(); it != queries.end();)
        {
            if (it->second->isDone())
            {
                LOG_TRACE(log, "Remove shared query(" << it->second->query_id << ")");
                queries.erase(it++);
            }
            else
                ++it;
        }
    }

    SharedQueries() : log(&Logger::get("SharedQueries"))
    {
        timer.schedule(FunctionTimerTask::create(std::bind(&SharedQueries::checkAll, this)), //
            check_interval_milliseconds,
            check_interval_milliseconds);
    }

    ~SharedQueries()
    {
        timer.cancel();
    }

private:
    static constexpr long check_interval_milliseconds = 20 * 1000; // 20 seconds

    Queries           queries;
    Poco::Util::Timer timer;
    std::mutex        mutex;

    Logger * log;
};

using SharedQueriesPtr = std::shared_ptr<SharedQueries>;
}
