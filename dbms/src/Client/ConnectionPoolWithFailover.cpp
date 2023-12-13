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

#include <Client/ConnectionPoolWithFailover.h>
#include <Common/ProfileEvents.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/isLocalAddress.h>
#include <Interpreters/Settings.h>
#include <Poco/Net/DNS.h>
#include <Poco/Net/NetException.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NETWORK_ERROR;
extern const int SOCKET_TIMEOUT;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


ConnectionPoolWithFailover::ConnectionPoolWithFailover(
    ConnectionPoolPtrs nested_pools_,
    LoadBalancing load_balancing,
    size_t max_tries_,
    time_t decrease_error_period_)
    : Base(
        std::move(nested_pools_),
        max_tries_,
        decrease_error_period_,
        &Poco::Logger::get("ConnectionPoolWithFailover"))
    , default_load_balancing(load_balancing)
{
    const std::string & local_hostname = getFQDNOrHostName();

    hostname_differences.resize(nested_pools.size());
    for (size_t i = 0; i < nested_pools.size(); ++i)
    {
        auto & connection_pool = dynamic_cast<ConnectionPool &>(*nested_pools[i]);
        hostname_differences[i] = getHostNameDifference(local_hostname, connection_pool.getHost());
    }
}

IConnectionPool::Entry ConnectionPoolWithFailover::getImpl(
    const Settings * settings,
    bool /*force_connected*/) // NOLINT
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message) {
        return tryGetEntry(pool, fail_message, settings);
    };

    GetPriorityFunc get_priority;
    switch (settings ? static_cast<LoadBalancing>(settings->load_balancing) : default_load_balancing)
    {
    case LoadBalancing::NEAREST_HOSTNAME:
        get_priority = [&](size_t i) {
            return hostname_differences[i];
        };
        break;
    case LoadBalancing::IN_ORDER:
        get_priority = [](size_t i) {
            return i;
        };
        break;
    case LoadBalancing::RANDOM:
        break;
    }

    return Base::get(try_get_entry, get_priority);
}

std::vector<IConnectionPool::Entry> ConnectionPoolWithFailover::getMany(const Settings * settings, PoolMode pool_mode)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message) {
        return tryGetEntry(pool, fail_message, settings);
    };

    std::vector<TryResult> results = getManyImpl(settings, pool_mode, try_get_entry);

    std::vector<Entry> entries;
    entries.reserve(results.size());
    for (auto & result : results)
        entries.emplace_back(std::move(result.entry));
    return entries;
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyChecked(
    const Settings * settings,
    PoolMode pool_mode,
    const QualifiedTableName & table_to_check)
{
    TryGetEntryFunc try_get_entry = [&](NestedPool & pool, std::string & fail_message) {
        return tryGetEntry(pool, fail_message, settings, &table_to_check);
    };
    return getManyImpl(settings, pool_mode, try_get_entry);
}

std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyImpl(
    const Settings * settings,
    PoolMode pool_mode,
    const TryGetEntryFunc & try_get_entry)
{
    size_t min_entries = 1;
    size_t max_entries;
    if (pool_mode == PoolMode::GET_ALL)
    {
        min_entries = nested_pools.size();
        max_entries = nested_pools.size();
    }
    else if (pool_mode == PoolMode::GET_ONE)
        max_entries = 1;
    else if (pool_mode == PoolMode::GET_MANY)
        max_entries = 1;
    else
        throw DB::Exception("Unknown pool allocation mode", DB::ErrorCodes::LOGICAL_ERROR);

    GetPriorityFunc get_priority;
    switch (settings ? static_cast<LoadBalancing>(settings->load_balancing) : default_load_balancing)
    {
    case LoadBalancing::NEAREST_HOSTNAME:
        get_priority = [&](size_t i) {
            return hostname_differences[i];
        };
        break;
    case LoadBalancing::IN_ORDER:
        get_priority = [](size_t i) {
            return i;
        };
        break;
    case LoadBalancing::RANDOM:
        break;
    }

    /*fallback_to_stale_replicas_for_distributed_queries*/
    bool fallback_to_stale_replicas = true;

    return Base::getMany(min_entries, max_entries, try_get_entry, get_priority, fallback_to_stale_replicas);
}

ConnectionPoolWithFailover::TryResult ConnectionPoolWithFailover::tryGetEntry(
    IConnectionPool & pool,
    std::string & fail_message,
    const Settings * settings,
    const QualifiedTableName * table_to_check)
{
    TryResult result;
    try
    {
        result.entry = pool.get(settings, /* force_connected = */ false);

        String server_name;
        UInt64 server_version_major;
        UInt64 server_version_minor;
        UInt64 server_version_patch;
        if (table_to_check)
            result.entry
                ->getServerVersion(server_name, server_version_major, server_version_minor, server_version_patch);

        if (!table_to_check)
        {
            result.entry->forceConnected();
            result.is_usable = true;
            result.is_up_to_date = true;
            return result;
        }

        /// Only status of the remote table corresponding to the Distributed table is taken into account.
        /// TODO: request status for joined tables also.
        TablesStatusRequest status_request;
        status_request.tables.emplace(*table_to_check);

        TablesStatusResponse status_response = result.entry->getTablesStatus(status_request);
        auto table_status_it = status_response.table_states_by_id.find(*table_to_check);
        if (table_status_it == status_response.table_states_by_id.end())
        {
            fail_message = fmt::format(
                "There is no table {}.{} on server: {}",
                table_to_check->database,
                table_to_check->table,
                result.entry->getDescription());
            LOG_WARNING(log, fail_message);

            return result;
        }

        result.is_usable = true;

        UInt64 max_allowed_delay = settings ? /*max_replica_delay_for_distributed_queries*/ 300 : 0;
        if (!max_allowed_delay)
        {
            result.is_up_to_date = true;
            return result;
        }

        UInt32 delay = table_status_it->second.absolute_delay;

        if (delay < max_allowed_delay)
            result.is_up_to_date = true;
        else
        {
            result.is_up_to_date = false;
            result.staleness = delay;

            LOG_TRACE(
                log,
                "Server {} has unacceptable replica delay for table {}.{}: {}",
                result.entry->getDescription(),
                table_to_check->database,
                table_to_check->table,
                delay);
        }
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT
            && e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throw;

        fail_message = getCurrentExceptionMessage(/* with_stacktrace = */ false);

        if (!result.entry.isNull())
        {
            result.entry->disconnect();
            result.reset();
        }
    }
    return result;
};

} // namespace DB
