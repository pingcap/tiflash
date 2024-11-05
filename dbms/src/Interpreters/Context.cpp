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

#include <Common/Config/ConfigProcessor.h>
#include <Common/DNSCache.h>
#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/Macros.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Common/TiFlashSecurity.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <DataStreams/FormatFactory.h>
#include <Databases/IDatabase.h>
#include <Debug/DBGInvoker.h>
#include <Debug/MockStorage.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <IO/BaseFile/fwd.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/FileProvider/FileProvider.h>
#include <Interpreters/ISecurityManager.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/Quota.h>
#include <Interpreters/RuntimeComponentsFactory.h>
#include <Interpreters/Settings.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Interpreters/SharedQueries.h>
#include <Interpreters/SystemLog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
#include <Poco/Mutex.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/UUID.h>
#include <Server/RaftConfigParser.h>
#include <Server/ServerInfo.h>
#include <Storages/BackgroundProcessingPool.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/DeltaIndexManager.h>
#include <Storages/DeltaMerge/File/ColumnCacheLongTerm.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/Index/VectorIndexCache.h>
#include <Storages/DeltaMerge/LocalIndexerScheduler.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/StoragePool/GlobalStoragePool.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/IStorage.h>
#include <Storages/KVStore/BackgroundService.h>
#include <Storages/KVStore/FFI/JointThreadAllocInfo.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/MarkCache.h>
#include <Storages/Page/PageConstants.h>
#include <Storages/Page/V3/PageStorageImpl.h>
#include <Storages/Page/V3/Universal/UniversalPageStorageService.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <common/logger_useful.h>
#include <fiu.h>
#include <fmt/core.h>

#include <boost/functional/hash/hash.hpp>
#include <pcg_random.hpp>
#include <set>
#include <unordered_map>


namespace ProfileEvents
{
extern const Event ContextLock;
}

namespace CurrentMetrics
{
extern const Metric GlobalStorageRunMode;
} // namespace CurrentMetrics


namespace DB
{
namespace ErrorCodes
{
extern const int DATABASE_ACCESS_DENIED;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_TABLE;
extern const int TABLE_ALREADY_EXISTS;
extern const int TABLE_WAS_NOT_DROPPED;
extern const int DATABASE_ALREADY_EXISTS;
extern const int THERE_IS_NO_SESSION;
extern const int THERE_IS_NO_QUERY;
extern const int NO_ELEMENTS_IN_CONFIG;
extern const int DDL_GUARD_IS_ACTIVE;
extern const int TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT;
extern const int SESSION_NOT_FOUND;
extern const int SESSION_IS_LOCKED;
extern const int CANNOT_GET_CREATE_TABLE_QUERY;
} // namespace ErrorCodes
namespace FailPoints
{
extern const char force_context_path[];
} // namespace FailPoints


/** Set of known objects (environment), that could be used in query.
  * Shared (global) part. Order of members (especially, order of destruction) is very important.
  */
struct ContextShared
{
    Poco::Logger * log = &Poco::Logger::get("Context");

    std::shared_ptr<IRuntimeComponentsFactory> runtime_components_factory;

    /// For access of most of shared objects. Recursive mutex.
    mutable std::recursive_mutex mutex;
    /// Separate mutex for access of dictionaries. Separate mutex to avoid locks when server doing request to itself.
    mutable std::mutex embedded_dictionaries_mutex;
    mutable std::mutex external_dictionaries_mutex;

    std::optional<ServerInfo> server_info;
    String path; /// Path to the primary data directory, with a slash at the end.
    String tmp_path; /// The path to the temporary files that occur when processing the request.
    String flags_path; /// Path to the directory with some control flags for server maintenance.
    String user_files_path; /// Path to the directory with user provided files, usable by 'file' table function.
    PathPool
        path_pool; /// The data directories. RegionPersister and some Storage Engine like DeltaMerge will use this to manage data placement on disks.
    ConfigurationPtr config; /// Global configuration settings.

    Databases databases; /// List of databases and tables in them.
    FormatFactory format_factory; /// Formats.
    String default_profile_name; /// Default profile name used for default values.
    String system_profile_name; /// Profile used by system processes
    std::shared_ptr<ISecurityManager> security_manager; /// Known users.
    Quotas quotas; /// Known quotas for resource use.
    mutable DBGInvoker dbg_invoker; /// Execute inner functions, debug only.
    mutable MarkCachePtr mark_cache; /// Cache of marks in compressed files.
    mutable DM::MinMaxIndexCachePtr minmax_index_cache; /// Cache of minmax index in compressed files.
    mutable DM::VectorIndexCachePtr vector_index_cache;
    mutable DM::ColumnCacheLongTermPtr column_cache_long_term;
    mutable DM::DeltaIndexManagerPtr delta_index_manager; /// Manage the Delta Indies of Segments.
    ProcessList process_list; /// Executing queries at the moment.
    ViewDependencies view_dependencies; /// Current dependencies
    ConfigurationPtr users_config; /// Config with the users, profiles and quotas sections.
    BackgroundProcessingPoolPtr background_pool; /// The thread pool for the background work performed by the tables.
    BackgroundProcessingPoolPtr
        blockable_background_pool; /// The thread pool for the blockable background work performed by the tables.
    BackgroundProcessingPoolPtr
        ps_compact_background_pool; /// The thread pool for the background work performed by the ps v2.
    mutable TMTContextPtr tmt_context; /// Context of TiFlash. Note that this should be free before background_pool.
    MultiVersion<Macros> macros; /// Substitutions extracted from config.
    String format_schema_path; /// Path to a directory that contains schema files used by input formats.

    SharedQueriesPtr shared_queries; /// The cache of shared queries.
    SchemaSyncServicePtr schema_sync_service; /// Schema sync service instance.
    PathCapacityMetricsPtr path_capacity_ptr; /// Path capacity metrics
    FileProviderPtr file_provider; /// File provider.
    IORateLimiter io_rate_limiter;
    PageStorageRunMode storage_run_mode = PageStorageRunMode::ONLY_V3;
    DM::GlobalPageIdAllocatorPtr global_page_id_allocator;
    DM::GlobalStoragePoolPtr global_storage_pool;
    DM::LocalIndexerSchedulerPtr global_local_indexer_scheduler;

    /// The PS instance available on Write Node.
    UniversalPageStorageServicePtr ps_write;

    /// Everything related with Disaggregation.
    SharedContextDisaggPtr ctx_disagg;

    TiFlashSecurityConfigPtr security_config;

    /// Named sessions. The user could specify session identifier to reuse settings and temporary tables in subsequent requests.

    JointThreadInfoJeallocMapPtr joint_memory_allocation_map; /// Joint thread-wise alloc/dealloc map

    std::unordered_set<uint64_t> store_id_blocklist; /// Those store id are blocked from batch cop request.

    class SessionKeyHash
    {
    public:
        size_t operator()(const Context::SessionKey & key) const
        {
            size_t seed = 0;
            boost::hash_combine(seed, key.first);
            boost::hash_combine(seed, key.second);
            return seed;
        }
    };

    using Sessions = std::unordered_map<Context::SessionKey, std::shared_ptr<Context>, SessionKeyHash>;
    using CloseTimes = std::deque<std::vector<Context::SessionKey>>;
    mutable Sessions sessions;
    mutable CloseTimes close_times;
    std::chrono::steady_clock::duration close_interval = std::chrono::seconds(1);
    std::chrono::steady_clock::time_point close_cycle_time = std::chrono::steady_clock::now();
    UInt64 close_cycle = 0;

    bool shutdown_called = false;

    /// Do not allow simultaneous execution of DDL requests on the same table.
    /// table -> exception_message(because table_id is global unique)
    /// For the duration of the operation, an element is placed here, and an object is returned, which deletes the element in the destructor.
    /// In case the element already exists, an exception is thrown. See class DDLGuard below.
    DDLGuard::Map ddl_guard_map;
    /// If you capture mutex and ddl_guards_mutex, then you need to grab them strictly in this order.
    mutable std::mutex ddl_guard_map_mutex;

    Stopwatch uptime_watch;

    Context::ApplicationType application_type = Context::ApplicationType::SERVER;

    pcg64 rng{randomSeed()};

    Context::ConfigReloadCallback config_reload_callback;

    std::shared_ptr<DB::DM::SharedBlockSchemas> shared_block_schemas;

    explicit ContextShared(std::shared_ptr<IRuntimeComponentsFactory> runtime_components_factory_)
        : runtime_components_factory(std::move(runtime_components_factory_))
        , storage_run_mode(PageStorageRunMode::ONLY_V3)
    {
        /// TODO: make it singleton (?)
#ifndef MULTIPLE_CONTEXT_GTEST
        static std::atomic<size_t> num_calls{0};
        if (++num_calls > 1)
        {
            std::cerr << "Attempting to create multiple ContextShared instances. Stack trace:\n"
                      << StackTrace().toString();
            std::cerr.flush();
            std::terminate();
        }
#endif

        initialize();
    }


    ~ContextShared()
    {
        try
        {
            shutdown();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }


    /** Perform a complex job of destroying objects in advance.
      */
    void shutdown()
    {
        if (shutdown_called)
            return;
        shutdown_called = true;

        if (global_storage_pool)
        {
            // shutdown the gc task of global storage pool before
            // shutting down the tables.
            global_storage_pool->shutdown();
        }

        if (ps_write)
        {
            ps_write->shutdown();
        }

        if (tmt_context)
        {
            tmt_context->shutdown();
        }

        if (joint_memory_allocation_map)
        {
            joint_memory_allocation_map->stopThreadAllocInfo();
        }

        if (schema_sync_service)
        {
            schema_sync_service = nullptr;
        }

        /** At this point, some tables may have threads that block our mutex.
          * To complete them correctly, we will copy the current list of tables,
          *  and ask them all to finish their work.
          * Then delete all objects with tables.
          */

        Databases current_databases;

        {
            std::lock_guard lock(mutex);
            current_databases = databases;
        }

        for (auto & database : current_databases)
            database.second->shutdown();

        {
            std::lock_guard lock(mutex);
            databases.clear();
        }
    }

private:
    void initialize() { security_manager = runtime_components_factory->createSecurityManager(); }
};


Context::Context() = default;


std::unique_ptr<Context> Context::createGlobal(std::shared_ptr<IRuntimeComponentsFactory> runtime_components_factory)
{
    std::unique_ptr<Context> res(new Context());
    res->setGlobalContext(*res);
    res->runtime_components_factory = runtime_components_factory;
    res->shared = std::make_shared<ContextShared>(runtime_components_factory);
    res->shared->ctx_disagg = SharedContextDisagg::create(*res);
    res->quota = std::make_shared<QuotaForIntervals>();
    res->timezone_info.init();
    return res;
}

std::unique_ptr<Context> Context::createGlobal()
{
    return createGlobal(std::make_unique<RuntimeComponentsFactory>());
}

Context::~Context()
{
    try
    {
        /// Destroy system logs while at least one Context is alive
        system_logs.reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


std::unique_lock<std::recursive_mutex> Context::getLock() const
{
    return std::unique_lock(shared->mutex);
}

ProcessList & Context::getProcessList()
{
    return shared->process_list;
}
const ProcessList & Context::getProcessList() const
{
    return shared->process_list;
}

void Context::setServerInfo(const ServerInfo & server_info)
{
    shared->server_info = server_info;
}
const std::optional<ServerInfo> & Context::getServerInfo() const
{
    return shared->server_info;
}

Databases Context::getDatabases() const
{
    auto lock = getLock();
    return shared->databases;
}

Context::SessionKey Context::getSessionKey(const String & session_id) const
{
    const auto & user_name = client_info.current_user;

    if (user_name.empty())
        throw Exception("Empty user name.", ErrorCodes::LOGICAL_ERROR);

    return SessionKey(user_name, session_id);
}


void Context::scheduleCloseSession(const Context::SessionKey & key, std::chrono::steady_clock::duration timeout)
{
    const UInt64 close_index = timeout / shared->close_interval + 1;
    const auto new_close_cycle = shared->close_cycle + close_index;

    if (session_close_cycle != new_close_cycle)
    {
        session_close_cycle = new_close_cycle;
        if (shared->close_times.size() < close_index + 1)
            shared->close_times.resize(close_index + 1);
        shared->close_times[close_index].emplace_back(key);
    }
}


std::shared_ptr<Context> Context::acquireSession(
    const String & session_id,
    std::chrono::steady_clock::duration timeout,
    bool session_check) const
{
    auto lock = getLock();

    const auto & key = getSessionKey(session_id);
    auto it = shared->sessions.find(key);

    if (it == shared->sessions.end())
    {
        if (session_check)
            throw Exception("Session not found.", ErrorCodes::SESSION_NOT_FOUND);

        auto new_session = std::make_shared<Context>(*global_context);

        new_session->scheduleCloseSession(key, timeout);

        it = shared->sessions.insert(std::make_pair(key, std::move(new_session))).first;
    }
    else if (it->second->client_info.current_user != client_info.current_user)
    {
        throw Exception("Session belongs to a different user", ErrorCodes::LOGICAL_ERROR);
    }

    const auto & session = it->second;

    if (session->session_is_used)
        throw Exception("Session is locked by a concurrent client.", ErrorCodes::SESSION_IS_LOCKED);
    session->session_is_used = true;

    session->client_info = client_info;

    return session;
}


void Context::releaseSession(const String & session_id, std::chrono::steady_clock::duration timeout)
{
    auto lock = getLock();

    session_is_used = false;
    scheduleCloseSession(getSessionKey(session_id), timeout);
}


std::chrono::steady_clock::duration Context::closeSessions() const
{
    auto lock = getLock();

    const auto now = std::chrono::steady_clock::now();

    if (now < shared->close_cycle_time)
        return shared->close_cycle_time - now;

    const auto current_cycle = shared->close_cycle;

    ++shared->close_cycle;
    shared->close_cycle_time = now + shared->close_interval;

    if (shared->close_times.empty())
        return shared->close_interval;

    auto & sessions_to_close = shared->close_times.front();

    for (const auto & key : sessions_to_close)
    {
        const auto session = shared->sessions.find(key);

        if (session != shared->sessions.end() && session->second->session_close_cycle <= current_cycle)
        {
            if (session->second->session_is_used)
                session->second->scheduleCloseSession(key, std::chrono::seconds(0));
            else
                shared->sessions.erase(session);
        }
    }

    shared->close_times.pop_front();

    return shared->close_interval;
}


static String resolveDatabase(const String & database_name, const String & current_database)
{
    String res = database_name.empty() ? current_database : database_name;
    if (res.empty())
        throw Exception("Default database is not selected", ErrorCodes::UNKNOWN_DATABASE);
    return res;
}


DatabasePtr Context::getDatabase(const String & database_name) const
{
    auto lock = getLock();
    String db = resolveDatabase(database_name, current_database);
    assertDatabaseExists(db);
    return shared->databases[db];
}

DatabasePtr Context::tryGetDatabase(const String & database_name) const
{
    auto lock = getLock();
    String db = resolveDatabase(database_name, current_database);
    auto it = shared->databases.find(db);
    if (it == shared->databases.end())
        return {};
    return it->second;
}

String Context::getPath() const
{
    auto lock = getLock();
    // Now we only make this failpoint for gtest_database.
    fiu_return_on(FailPoints::force_context_path, fmt::format("{}{}/", shared->path, "DatabaseTiFlashTest"));
    return shared->path;
}

String Context::getTemporaryPath() const
{
    auto lock = getLock();
    return shared->tmp_path;
}

String Context::getFlagsPath() const
{
    auto lock = getLock();
    return shared->flags_path;
}

String Context::getUserFilesPath() const
{
    auto lock = getLock();
    return shared->user_files_path;
}

PathPool & Context::getPathPool() const
{
    auto lock = getLock();
    return shared->path_pool;
}

void Context::setPath(const String & path)
{
    auto lock = getLock();

    shared->path = path;

    if (shared->tmp_path.empty())
        shared->tmp_path = shared->path + "tmp/";

    if (shared->flags_path.empty())
        shared->flags_path = shared->path + "flags/";

    if (shared->user_files_path.empty())
        shared->user_files_path = shared->path + "user_files/";
}

void Context::setTemporaryPath(const String & path)
{
    auto lock = getLock();
    shared->tmp_path = path;
}

void Context::setFlagsPath(const String & path)
{
    auto lock = getLock();
    shared->flags_path = path;
}

void Context::setUserFilesPath(const String & path)
{
    auto lock = getLock();
    shared->user_files_path = path;
}

void Context::setPathPool(
    const Strings & main_data_paths,
    const Strings & latest_data_paths,
    const Strings & kvstore_paths,
    PathCapacityMetricsPtr global_capacity_,
    FileProviderPtr file_provider_)
{
    auto lock = getLock();
    shared->path_pool = PathPool(main_data_paths, latest_data_paths, kvstore_paths, global_capacity_, file_provider_);
}

void Context::setConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->config = config;
}

Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    auto lock = getLock();
    return shared->config ? *shared->config : Poco::Util::Application::instance().config();
}

void Context::setUsersConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->users_config = config;
    // parse "users.*"
    shared->security_manager->loadFromConfig(*shared->users_config);
    // parse "quotas.*"
    shared->quotas.loadFromConfig(*shared->users_config);
}

ConfigurationPtr Context::getUsersConfig()
{
    auto lock = getLock();
    return shared->users_config;
}

void Context::setSecurityConfig(Poco::Util::AbstractConfiguration & config, const LoggerPtr & log)
{
    auto lock = getLock();
    shared->security_config = std::make_shared<TiFlashSecurityConfig>(log);
    shared->security_config->init(config);
}

TiFlashSecurityConfigPtr Context::getSecurityConfig()
{
    auto lock = getLock();
    return shared->security_config;
}

void Context::reloadDeltaTreeConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto default_profile_name = config.getString("default_profile", "default");
    String elem = "profiles." + default_profile_name;
    if (!config.has(elem))
    {
        return;
    }
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(elem, config_keys);
    String dt_config_reload_log = "reload delta tree ";
    for (const std::string & key : config_keys)
    {
        if (startsWith(key, "dt"))
        {
            String config_value = config.getString(elem + "." + key);
            if (settings.get(key) == config_value)
            {
                continue;
            }
            dt_config_reload_log
                += fmt::format("config name: {}, old: {}, new: {}; ", key, settings.get(key), config_value);
            settings.set(key, config_value);
        }
    }
    LOG_INFO(shared->log, dt_config_reload_log);
}

void Context::calculateUserSettings()
{
    auto lock = getLock();

    String profile_name = shared->security_manager->getUser(client_info.current_user)->profile;

    /// 1) Set default settings (hardcoded values)
    /// NOTE: we ignore global_context settings (from which it is usually copied)
    /// NOTE: global_context settings are immutable and not auto updated
    settings = Settings();

    /// 2) Apply settings from default profile ("profiles.*" in `users_config`)
    auto default_profile_name = getDefaultProfileName();
    if (profile_name != default_profile_name)
        settings.setProfile(default_profile_name, *shared->users_config);

    /// 3) Apply settings from current user
    settings.setProfile(profile_name, *shared->users_config);
}


void Context::setUser(
    const String & name,
    const String & password,
    const Poco::Net::SocketAddress & address,
    const String & quota_key)
{
    auto lock = getLock();

    auto user_props = shared->security_manager->authorizeAndGetUser(name, password, address.host());

    client_info.current_user = name;
    client_info.current_address = address;
    client_info.current_password = password;

    if (!quota_key.empty())
        client_info.quota_key = quota_key;

    calculateUserSettings();

    setQuota(user_props->quota, quota_key, name, address.host());
}


void Context::setQuota(
    const String & name,
    const String & quota_key,
    const String & user_name,
    const Poco::Net::IPAddress & address)
{
    auto lock = getLock();
    quota = shared->quotas.get(name, quota_key, user_name, address);
}


QuotaForIntervals & Context::getQuota()
{
    auto lock = getLock();
    return *quota;
}

void Context::checkDatabaseAccessRights(const std::string & database_name) const
{
    auto lock = getLock();
    checkDatabaseAccessRightsImpl(database_name);
}

void Context::checkDatabaseAccessRightsImpl(const std::string & database_name) const
{
    if (client_info.current_user.empty() || (database_name == "system"))
    {
        /// An unnamed user, i.e. server, has access to all databases.
        /// All users have access to the database system.
        return;
    }
    if (!shared->security_manager->hasAccessToDatabase(client_info.current_user, database_name))
        throw Exception(fmt::format("Access denied to database {}", database_name), ErrorCodes::DATABASE_ACCESS_DENIED);
}

void Context::addDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
    auto lock = getLock();
    checkDatabaseAccessRightsImpl(from.first);
    checkDatabaseAccessRightsImpl(where.first);
    shared->view_dependencies[from].insert(where);

    // Notify table of dependencies change
    auto table = tryGetTable(from.first, from.second);
    if (table != nullptr)
        table->updateDependencies();
}

void Context::removeDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where)
{
    auto lock = getLock();
    checkDatabaseAccessRightsImpl(from.first);
    checkDatabaseAccessRightsImpl(where.first);
    shared->view_dependencies[from].erase(where);

    // Notify table of dependencies change
    auto table = tryGetTable(from.first, from.second);
    if (table != nullptr)
        table->updateDependencies();
}

Dependencies Context::getDependencies(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);

    if (database_name.empty() && tryGetExternalTable(table_name))
    {
        /// Table is temporary. Access granted.
    }
    else
    {
        checkDatabaseAccessRightsImpl(db);
    }

    auto iter = shared->view_dependencies.find(DatabaseAndTableName(db, table_name));
    if (iter == shared->view_dependencies.end())
        return {};

    return Dependencies(iter->second.begin(), iter->second.end());
}

bool Context::isTableExist(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);

    auto it = shared->databases.find(db);
    return shared->databases.end() != it && it->second->isTableExist(*this, table_name);
}

bool Context::isDatabaseExist(const String & database_name) const
{
    auto lock = getLock();
    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);
    return shared->databases.end() != shared->databases.find(db);
}

bool Context::isExternalTableExist(const String & table_name) const
{
    return external_tables.end() != external_tables.find(table_name);
}


void Context::assertTableExists(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);

    auto it = shared->databases.find(db);
    if (shared->databases.end() == it)
        throw Exception(fmt::format("Database {} doesn't exist", backQuoteIfNeed(db)), ErrorCodes::UNKNOWN_DATABASE);

    if (!it->second->isTableExist(*this, table_name))
        throw Exception(
            fmt::format("Table {}.{} doesn't exist.", backQuoteIfNeed(db), backQuoteIfNeed(table_name)),
            ErrorCodes::UNKNOWN_TABLE);
}


void Context::assertTableDoesntExist(
    const String & database_name,
    const String & table_name,
    bool check_database_access_rights) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    if (check_database_access_rights)
        checkDatabaseAccessRightsImpl(db);

    auto it = shared->databases.find(db);
    if (shared->databases.end() != it && it->second->isTableExist(*this, table_name))
        throw Exception(
            fmt::format("Table {}.{} already exists.", backQuoteIfNeed(db), backQuoteIfNeed(table_name)),
            ErrorCodes::TABLE_ALREADY_EXISTS);
}


void Context::assertDatabaseExists(const String & database_name, bool check_database_access_rights) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    if (check_database_access_rights)
        checkDatabaseAccessRightsImpl(db);

    if (shared->databases.end() == shared->databases.find(db))
        throw Exception(fmt::format("Database {} doesn't exist", backQuoteIfNeed(db)), ErrorCodes::UNKNOWN_DATABASE);
}


void Context::assertDatabaseDoesntExist(const String & database_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);

    if (shared->databases.end() != shared->databases.find(db))
        throw Exception(
            fmt::format("Database {} already exists.", backQuoteIfNeed(db)),
            ErrorCodes::DATABASE_ALREADY_EXISTS);
}


Tables Context::getExternalTables() const
{
    auto lock = getLock();

    Tables res;
    for (const auto & table : external_tables)
        res[table.first] = table.second.first;

    if (session_context && session_context != this)
    {
        Tables buf = session_context->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    else if (global_context && global_context != this)
    {
        Tables buf = global_context->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    return res;
}


StoragePtr Context::tryGetExternalTable(const String & table_name) const
{
    auto jt = external_tables.find(table_name);
    if (external_tables.end() == jt)
        return StoragePtr();

    return jt->second.first;
}


StoragePtr Context::getTable(const String & database_name, const String & table_name) const
{
    Exception exc;
    auto res = getTableImpl(database_name, table_name, &exc);
    if (!res)
        throw Exception(exc);
    return res;
}


StoragePtr Context::tryGetTable(const String & database_name, const String & table_name) const
{
    return getTableImpl(database_name, table_name, nullptr);
}


StoragePtr Context::getTableImpl(const String & database_name, const String & table_name, Exception * exception) const
{
    auto lock = getLock();

    if (database_name.empty())
    {
        StoragePtr res = tryGetExternalTable(table_name);
        if (res)
            return res;
    }

    String db = resolveDatabase(database_name, current_database);
    checkDatabaseAccessRightsImpl(db);

    auto it = shared->databases.find(db);
    if (shared->databases.end() == it)
    {
        if (exception)
            *exception = Exception(
                fmt::format("Database {} doesn't exist", backQuoteIfNeed(db)),
                ErrorCodes::UNKNOWN_DATABASE);
        return {};
    }

    auto table = it->second->tryGetTable(*this, table_name);
    if (!table)
    {
        if (exception)
            *exception = Exception(
                fmt::format("Table {}.{} doesn't exist.", backQuoteIfNeed(db), backQuoteIfNeed(table_name)),
                ErrorCodes::UNKNOWN_TABLE);
        return {};
    }

    return table;
}


void Context::addExternalTable(const String & table_name, const StoragePtr & storage, const ASTPtr & ast)
{
    if (external_tables.end() != external_tables.find(table_name))
        throw Exception(
            fmt::format("Temporary table {} already exists.", backQuoteIfNeed(table_name)),
            ErrorCodes::TABLE_ALREADY_EXISTS);

    external_tables[table_name] = std::pair(storage, ast);
}

StoragePtr Context::tryRemoveExternalTable(const String & table_name)
{
    auto it = external_tables.find(table_name);

    if (external_tables.end() == it)
        return StoragePtr();

    auto storage = it->second.first;
    external_tables.erase(it);
    return storage;
}


StoragePtr Context::executeTableFunction(const ASTPtr & table_expression)
{
    /// Slightly suboptimal.
    auto hash = table_expression->getTreeHash();
    String key = toString(hash.first) + '_' + toString(hash.second);

    StoragePtr & res = table_function_results[key];

    if (!res)
    {
        TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(
            typeid_cast<const ASTFunction *>(table_expression.get())->name,
            *this);

        /// Run it and remember the result
        res = table_function_ptr->execute(table_expression, *this);
    }

    return res;
}


DDLGuard::DDLGuard(
    Map & map_,
    std::mutex & mutex_,
    std::unique_lock<std::mutex> && /*lock*/,
    const String & elem,
    const String & message)
    : map(map_)
    , mutex(mutex_)
{
    bool inserted;
    std::tie(it, inserted) = map.emplace(elem, message);
    if (!inserted)
        throw Exception(it->second, ErrorCodes::DDL_GUARD_IS_ACTIVE);
}

DDLGuard::~DDLGuard()
{
    std::lock_guard lock(mutex);
    map.erase(it);
}

std::unique_ptr<DDLGuard> Context::getDDLGuard(const String & table, const String & message) const
{
    std::unique_lock lock(shared->ddl_guard_map_mutex);
    return std::make_unique<DDLGuard>(
        shared->ddl_guard_map,
        shared->ddl_guard_map_mutex,
        std::move(lock),
        table,
        message);
}


std::unique_ptr<DDLGuard> Context::getDDLGuardIfTableDoesntExist(
    const String & database,
    const String & table,
    const String & message) const
{
    auto lock = getLock();

    auto it = shared->databases.find(database);
    if (shared->databases.end() != it && it->second->isTableExist(*this, table))
        return {};

    return getDDLGuard(table, message);
}


void Context::addDatabase(const String & database_name, const DatabasePtr & database)
{
    auto lock = getLock();

    assertDatabaseDoesntExist(database_name);
    shared->databases[database_name] = database;
}


DatabasePtr Context::detachDatabase(const String & database_name)
{
    auto lock = getLock();

    auto res = getDatabase(database_name);
    shared->databases.erase(database_name);
    return res;
}


ASTPtr Context::getCreateTableQuery(const String & database_name, const String & table_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    assertDatabaseExists(db);

    return shared->databases[db]->getCreateTableQuery(*this, table_name);
}

ASTPtr Context::getCreateExternalTableQuery(const String & table_name) const
{
    auto jt = external_tables.find(table_name);
    if (external_tables.end() == jt)
        throw Exception(
            fmt::format("Temporary table {} doesn't exist", backQuoteIfNeed(table_name)),
            ErrorCodes::UNKNOWN_TABLE);

    return jt->second.second;
}

ASTPtr Context::getCreateDatabaseQuery(const String & database_name) const
{
    auto lock = getLock();

    String db = resolveDatabase(database_name, current_database);
    assertDatabaseExists(db);

    return shared->databases[db]->getCreateDatabaseQuery(*this);
}

void Context::checkIsConfigLoaded() const
{
    if (shared->application_type == ApplicationType::SERVER && !is_config_loaded)
    {
        throw Exception(
            "Configuration are used before load from configure file tiflash.toml, so the user config may not take "
            "effect.",
            ErrorCodes::LOGICAL_ERROR);
    }
}

Settings Context::getSettings() const
{
    checkIsConfigLoaded();
    return settings;
}


void Context::setSettings(const Settings & settings_)
{
    settings = settings_;
}


void Context::setSetting(const String & name, const Field & value)
{
    if (name == "profile")
    {
        auto lock = getLock();
        settings.setProfile(value.safeGet<String>(), *shared->users_config);
    }
    else
        settings.set(name, value);
}


void Context::setSetting(const String & name, const std::string & value)
{
    if (name == "profile")
    {
        auto lock = getLock();
        settings.setProfile(value, *shared->users_config);
    }
    else
        settings.set(name, value);
}


String Context::getCurrentDatabase() const
{
    return current_database;
}


String Context::getCurrentQueryId() const
{
    return client_info.current_query_id;
}


void Context::setCurrentDatabase(const String & name)
{
    auto lock = getLock();
    assertDatabaseExists(name);
    current_database = name;
}


void Context::setCurrentQueryId(const String & query_id)
{
    if (!client_info.current_query_id.empty())
        throw Exception("Logical error: attempt to set query_id twice", ErrorCodes::LOGICAL_ERROR);

    String query_id_to_set = query_id;

    if (query_id_to_set.empty()) /// If the user did not submit his query_id, then we generate it ourselves.
    {
        /// Generate random UUID, but using lower quality RNG,
        ///  because Poco::UUIDGenerator::generateRandom method is using /dev/random, that is very expensive.
        /// NOTE: Actually we don't need to use UUIDs for query identifiers.
        /// We could use any suitable string instead.

        union
        {
            char bytes[16];
            struct
            {
                UInt64 a;
                UInt64 b;
            };
        } random{};

        {
            auto lock = getLock();

            random.a = shared->rng();
            random.b = shared->rng();
        }

        /// Use protected constructor.
        struct UUID : Poco::UUID
        {
            UUID(const char * bytes, Poco::UUID::Version version)
                : Poco::UUID(bytes, version)
            {}
        };

        query_id_to_set = UUID(random.bytes, Poco::UUID::UUID_RANDOM).toString();
    }

    client_info.current_query_id = query_id_to_set;
}


String Context::getDefaultFormat() const
{
    return default_format.empty() ? "TabSeparated" : default_format;
}


void Context::setDefaultFormat(const String & name)
{
    default_format = name;
}

MultiVersion<Macros>::Version Context::getMacros() const
{
    return shared->macros.get();
}

void Context::setMacros(std::unique_ptr<Macros> && macros)
{
    shared->macros.set(std::move(macros));
}

const Context & Context::getQueryContext() const
{
    if (!query_context)
        throw Exception("There is no query", ErrorCodes::THERE_IS_NO_QUERY);
    return *query_context;
}

Context & Context::getQueryContext()
{
    if (!query_context)
        throw Exception("There is no query", ErrorCodes::THERE_IS_NO_QUERY);
    return *query_context;
}

const Context & Context::getSessionContext() const
{
    if (!session_context)
        throw Exception("There is no session", ErrorCodes::THERE_IS_NO_SESSION);
    return *session_context;
}

Context & Context::getSessionContext()
{
    if (!session_context)
        throw Exception("There is no session", ErrorCodes::THERE_IS_NO_SESSION);
    return *session_context;
}

const Context & Context::getGlobalContext() const
{
    if (!global_context)
        throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);
    return *global_context;
}

Context & Context::getGlobalContext()
{
    if (!global_context)
        throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);
    return *global_context;
}

const Settings & Context::getSettingsRef() const
{
    checkIsConfigLoaded();
    return settings;
}

Settings & Context::getSettingsRef()
{
    checkIsConfigLoaded();
    return settings;
}

void Context::setProgressCallback(ProgressCallback callback)
{
    /// Callback is set to a session or to a query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    progress_callback = callback;
}

ProgressCallback Context::getProgressCallback() const
{
    return progress_callback;
}


void Context::setProcessListElement(ProcessList::Element * elem)
{
    /// Set to a session or query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    process_list_elem = elem;
}

ProcessList::Element * Context::getProcessListElement() const
{
    return process_list_elem;
}

void Context::setDAGContext(DAGContext * dag_context_)
{
    dag_context = dag_context_;
}

DAGContext * Context::getDAGContext() const
{
    return dag_context;
}

DBGInvoker & Context::getDBGInvoker() const
{
    auto lock = getLock();
    return shared->dbg_invoker;
}

void Context::setMarkCache(size_t cache_size_in_bytes)
{
    auto lock = getLock();

    if (shared->mark_cache)
        throw Exception("Mark cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->mark_cache = std::make_shared<MarkCache>(cache_size_in_bytes);
}


MarkCachePtr Context::getMarkCache() const
{
    auto lock = getLock();
    return shared->mark_cache;
}


void Context::dropMarkCache() const
{
    auto lock = getLock();
    if (shared->mark_cache)
        shared->mark_cache->reset();
}


void Context::setMinMaxIndexCache(size_t cache_size_in_bytes)
{
    auto lock = getLock();

    if (shared->minmax_index_cache)
        throw Exception("Minmax index cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->minmax_index_cache = std::make_shared<DM::MinMaxIndexCache>(cache_size_in_bytes);
}

DM::MinMaxIndexCachePtr Context::getMinMaxIndexCache() const
{
    auto lock = getLock();
    return shared->minmax_index_cache;
}

void Context::dropMinMaxIndexCache() const
{
    auto lock = getLock();
    if (shared->minmax_index_cache)
        shared->minmax_index_cache->reset();
}

void Context::setVectorIndexCache(size_t cache_entities)
{
    auto lock = getLock();

    RUNTIME_CHECK(!shared->vector_index_cache);

    shared->vector_index_cache = std::make_shared<DM::VectorIndexCache>(cache_entities);
}

DM::VectorIndexCachePtr Context::getVectorIndexCache() const
{
    auto lock = getLock();
    return shared->vector_index_cache;
}

void Context::dropVectorIndexCache() const
{
    auto lock = getLock();
    if (shared->vector_index_cache)
        shared->vector_index_cache.reset();
}

void Context::setColumnCacheLongTerm(size_t cache_size_in_bytes)
{
    auto lock = getLock();

    RUNTIME_CHECK(!shared->column_cache_long_term);

    shared->column_cache_long_term = std::make_shared<DM::ColumnCacheLongTerm>(cache_size_in_bytes);
}

DM::ColumnCacheLongTermPtr Context::getColumnCacheLongTerm() const
{
    auto lock = getLock();
    return shared->column_cache_long_term;
}

void Context::dropColumnCacheLongTerm() const
{
    auto lock = getLock();
    if (shared->column_cache_long_term)
        shared->column_cache_long_term.reset();
}

bool Context::isDeltaIndexLimited() const
{
    // Don't need to use a lock here, as delta_index_manager should be set at starting up.
    if (!shared->delta_index_manager)
        return false;
    return shared->delta_index_manager->isLimit();
}

void Context::setDeltaIndexManager(size_t cache_size_in_bytes)
{
    auto lock = getLock();

    if (shared->delta_index_manager)
        throw Exception("DeltaIndexManager has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->delta_index_manager = std::make_shared<DM::DeltaIndexManager>(cache_size_in_bytes);
}

DM::DeltaIndexManagerPtr Context::getDeltaIndexManager() const
{
    auto lock = getLock();
    return shared->delta_index_manager;
}

void Context::dropCaches() const
{
    auto lock = getLock();

    if (shared->mark_cache)
        shared->mark_cache->reset();
}

BackgroundProcessingPool & Context::initializeBackgroundPool(UInt16 pool_size)
{
    auto lock = getLock();
    if (!shared->background_pool)
        shared->background_pool
            = std::make_shared<BackgroundProcessingPool>(pool_size, "bg-", getJointThreadInfoJeallocMap(lock));
    return *shared->background_pool;
}

BackgroundProcessingPool & Context::getBackgroundPool()
{
    auto lock = getLock();
    return *shared->background_pool;
}

BackgroundProcessingPool & Context::initializeBlockableBackgroundPool(UInt16 pool_size)
{
    auto lock = getLock();
    if (!shared->blockable_background_pool)
        shared->blockable_background_pool
            = std::make_shared<BackgroundProcessingPool>(pool_size, "bg-block-", getJointThreadInfoJeallocMap(lock));
    return *shared->blockable_background_pool;
}

BackgroundProcessingPool & Context::getBlockableBackgroundPool()
{
    // TODO: maybe a better name for the pool
    auto lock = getLock();
    return *shared->blockable_background_pool;
}

BackgroundProcessingPool & Context::getPSBackgroundPool()
{
    auto lock = getLock();
    // use the same size as `background_pool_size`
    if (!shared->ps_compact_background_pool)
        shared->ps_compact_background_pool = std::make_shared<BackgroundProcessingPool>(
            settings.background_pool_size,
            "bg-page-",
            getJointThreadInfoJeallocMap(lock));
    return *shared->ps_compact_background_pool;
}

void Context::createTMTContext(const TiFlashRaftConfig & raft_config, pingcap::ClusterConfig && cluster_config)
{
    auto lock = getLock();
    if (shared->tmt_context)
        throw Exception("TMTContext has already existed", ErrorCodes::LOGICAL_ERROR);
    shared->tmt_context = std::make_shared<TMTContext>(*this, raft_config, cluster_config);
}

bool Context::isTMTContextInited() const
{
    auto lock = getLock();
    return shared->tmt_context != nullptr;
}

TMTContext & Context::getTMTContext() const
{
    auto lock = getLock();
    if (!shared->tmt_context)
        throw Exception("no tmt context");
    return *(shared->tmt_context);
}

void Context::initializePathCapacityMetric( //
    size_t global_capacity_quota, //
    const Strings & main_data_paths,
    const std::vector<size_t> & main_capacity_quota, //
    const Strings & latest_data_paths,
    const std::vector<size_t> & latest_capacity_quota,
    const Strings & remote_cache_paths,
    const std::vector<size_t> & remote_cache_capacity_quota)
{
    auto lock = getLock();
    if (shared->path_capacity_ptr)
        throw Exception("PathCapacityMetrics instance has already existed", ErrorCodes::LOGICAL_ERROR);
    shared->path_capacity_ptr = std::make_shared<PathCapacityMetrics>(
        global_capacity_quota,
        main_data_paths,
        main_capacity_quota,
        latest_data_paths,
        latest_capacity_quota,
        remote_cache_paths,
        remote_cache_capacity_quota);
}

PathCapacityMetricsPtr Context::getPathCapacity() const
{
    auto lock = getLock();
    if (!shared->path_capacity_ptr)
        throw Exception("PathCapacityMetrics is not initialized.", ErrorCodes::LOGICAL_ERROR);
    return shared->path_capacity_ptr;
}

void Context::initializeSchemaSyncService()
{
    auto lock = getLock();
    if (shared->schema_sync_service)
        throw Exception("Schema Sync Service has already been initialized.", ErrorCodes::LOGICAL_ERROR);
    shared->schema_sync_service = std::make_shared<SchemaSyncService>(*global_context);
}

SchemaSyncServicePtr & Context::getSchemaSyncService()
{
    auto lock = getLock();
    return shared->schema_sync_service;
}

void Context::initializeTiFlashMetrics() const
{
    auto lock = getLock();
    (void)TiFlashMetrics::instance();
}

void Context::initializeFileProvider(KeyManagerPtr key_manager, bool enable_encryption, bool enable_keyspace_encryption)
{
    auto lock = getLock();
    if (shared->file_provider)
        throw Exception("File provider has already been initialized.", ErrorCodes::LOGICAL_ERROR);
    shared->file_provider = std::make_shared<FileProvider>(key_manager, enable_encryption, enable_keyspace_encryption);
}

FileProviderPtr Context::getFileProvider() const
{
    auto lock = getLock();
    return shared->file_provider;
}

void Context::setFileProvider(FileProviderPtr file_provider)
{
    auto lock = getLock();
    shared->file_provider = file_provider;
}

void Context::initializeRateLimiter(
    Poco::Util::AbstractConfiguration & config,
    BackgroundProcessingPool & bg_pool,
    BackgroundProcessingPool & blockable_bg_pool) const
{
    getIORateLimiter().init(config);
    auto tids = bg_pool.getThreadIds();
    auto blockable_tids = blockable_bg_pool.getThreadIds();
    tids.insert(tids.end(), blockable_tids.begin(), blockable_tids.end());
    getIORateLimiter().setBackgroundThreadIds(tids);
}

WriteLimiterPtr Context::getWriteLimiter() const
{
    return getIORateLimiter().getWriteLimiter();
}

IORateLimiter & Context::getIORateLimiter() const
{
    return shared->io_rate_limiter;
}

ReadLimiterPtr Context::getReadLimiter() const
{
    return getIORateLimiter().getReadLimiter();
}


static bool isPageStorageV2Existed(const PathPool & path_pool)
{
    for (const auto & path : path_pool.listKVStorePaths())
    {
        Poco::File dir(path);
        if (!dir.exists())
            continue;

        std::vector<std::string> files;
        dir.list(files);
        if (!files.empty())
        {
            for (const auto & file_name : files)
            {
                const auto & find_index = file_name.find("page");
                if (find_index != std::string::npos)
                {
                    return true;
                }
            }
            // KVStore is not empty, but can't find any of v2 data in it.
        }
    }

    // If not data in KVStore. It means V2 data must not existed.
    return false;
}

static bool isPageStorageV3Existed(const PathPool & path_pool)
{
    const std::vector<String> path_prefixes = {
        PathPool::log_path_prefix,
        PathPool::data_path_prefix,
        PathPool::meta_path_prefix,
        PathPool::kvstore_path_prefix,
    };
    for (const auto & path : path_pool.listGlobalPagePaths())
    {
        for (const auto & path_prefix : path_prefixes)
        {
            Poco::File dir(path + "/" + path_prefix);
            if (dir.exists())
                return true;
        }
    }
    return false;
}

static bool isWriteNodeUniPSExisted(const PathPool & path_pool)
{
    for (const auto & path : path_pool.listGlobalPagePaths())
    {
        Poco::File dir(path + "/" + PathPool::write_uni_path_prefix);
        if (dir.exists())
            return true;
    }
    return false;
}

void Context::initializePageStorageMode(const PathPool & path_pool, UInt64 storage_page_format_version)
{
    auto lock = getLock();

    /**
     * PageFormat::V2 + isPageStorageV3Existed is false + whatever isPageStorageV2Existed true or false = ONLY_V2
     * PageFormat::V2 + isPageStorageV3Existed is true  + whatever isPageStorageV2Existed true or false = ERROR Config
     * PageFormat::V3 + isPageStorageV2Existed is true  + whatever isPageStorageV3Existed true or false = MIX_MODE
     * PageFormat::V3 + isPageStorageV2Existed is false + whatever isPageStorageV3Existed true or false = ONLY_V3
     */

    switch (storage_page_format_version)
    {
    case PageFormat::V1:
    case PageFormat::V2:
    {
        if (isPageStorageV3Existed(path_pool) || isWriteNodeUniPSExisted(path_pool))
        {
            throw Exception(
                "Invalid config `storage.format_version`, newer format page data exist. But using the PageFormat::V2."
                "If you are downgrading the format_version for this TiFlash node, you need to rebuild the data from "
                "scratch.",
                ErrorCodes::LOGICAL_ERROR);
        }
        // not exist newer format page data
        shared->storage_run_mode = PageStorageRunMode::ONLY_V2;
        return;
    }
    case PageFormat::V3:
    {
        if (isWriteNodeUniPSExisted(path_pool))
        {
            throw Exception(
                "Invalid config `storage.format_version`, newer format page data exist. But using the PageFormat::V3."
                "If you are downgrading the format_version for this TiFlash node, you need to rebuild the data from "
                "scratch.",
                ErrorCodes::LOGICAL_ERROR);
        }
        shared->storage_run_mode
            = isPageStorageV2Existed(path_pool) ? PageStorageRunMode::MIX_MODE : PageStorageRunMode::ONLY_V3;
        return;
    }
    case PageFormat::V4:
    {
        if (isPageStorageV2Existed(path_pool) || isPageStorageV3Existed(path_pool))
        {
            throw Exception("Uni PS can only be enabled on a fresh start", ErrorCodes::LOGICAL_ERROR);
        }
        shared->storage_run_mode = PageStorageRunMode::UNI_PS;
        return;
    }
    default:
        throw Exception(
            fmt::format("Can't detect the format version of Page [page_version={}]", storage_page_format_version),
            ErrorCodes::LOGICAL_ERROR);
    }
}

PageStorageRunMode Context::getPageStorageRunMode() const
{
    auto lock = getLock();
    return shared->storage_run_mode;
}

void Context::setPageStorageRunMode(PageStorageRunMode run_mode) const
{
    auto lock = getLock();
    shared->storage_run_mode = run_mode;
}

bool Context::initializeGlobalPageIdAllocator()
{
    auto lock = getLock();
    if (!shared->global_page_id_allocator)
    {
        shared->global_page_id_allocator = std::make_shared<DM::GlobalPageIdAllocator>();
    }
    return true;
}

DM::GlobalPageIdAllocatorPtr Context::getGlobalPageIdAllocator() const
{
    auto lock = getLock();
    return shared->global_page_id_allocator;
}

bool Context::initializeGlobalLocalIndexerScheduler(size_t pool_size, size_t memory_limit)
{
    auto lock = getLock();
    if (!shared->global_local_indexer_scheduler)
    {
        shared->global_local_indexer_scheduler
            = std::make_shared<DM::LocalIndexerScheduler>(DM::LocalIndexerScheduler::Options{
                .pool_size = pool_size,
                .memory_limit = memory_limit,
                .auto_start = true,
            });
    }
    return true;
}

DM::LocalIndexerSchedulerPtr Context::getGlobalLocalIndexerScheduler() const
{
    auto lock = getLock();
    return shared->global_local_indexer_scheduler;
}

bool Context::initializeGlobalStoragePoolIfNeed(const PathPool & path_pool)
{
    auto lock = getLock();
    CurrentMetrics::set(CurrentMetrics::GlobalStorageRunMode, static_cast<UInt8>(shared->storage_run_mode));
    if (shared->storage_run_mode == PageStorageRunMode::MIX_MODE
        || shared->storage_run_mode == PageStorageRunMode::ONLY_V3)
    {
        if (shared->global_storage_pool)
        {
            // GlobalStoragePool may be initialized many times in some test cases for restore.
            LOG_WARNING(shared->log, "GlobalStoragePool has already been initialized.");
            shared->global_storage_pool->shutdown();
        }
        try
        {
            shared->global_storage_pool = std::make_shared<DM::GlobalStoragePool>(path_pool, *this, settings);
            shared->global_storage_pool->restore();
            return true;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }
    else
    {
        shared->global_storage_pool = nullptr;
        return false;
    }
}

DM::GlobalStoragePoolPtr Context::getGlobalStoragePool() const
{
    auto lock = getLock();
    return shared->global_storage_pool;
}


void Context::initializeJointThreadInfoJeallocMap()
{
    auto lock = getLock();
    if (!shared->joint_memory_allocation_map)
    {
        shared->joint_memory_allocation_map = std::make_shared<JointThreadInfoJeallocMap>();
    }
}

JointThreadInfoJeallocMapPtr Context::getJointThreadInfoJeallocMap() const
{
    auto lock = getLock();
    if (!shared->joint_memory_allocation_map)
    {
        shared->joint_memory_allocation_map = std::make_shared<JointThreadInfoJeallocMap>();
    }
    return shared->joint_memory_allocation_map;
}

JointThreadInfoJeallocMapPtr Context::getJointThreadInfoJeallocMap(std::unique_lock<std::recursive_mutex> &) const
{
    if (!shared->joint_memory_allocation_map)
    {
        shared->joint_memory_allocation_map = std::make_shared<JointThreadInfoJeallocMap>();
    }
    return shared->joint_memory_allocation_map;
}

/**
 * This PageStorage is initialized in two cases:
 * 1. Not in disaggregated mode.
 * 2. In disaggregated write mode.
 */
void Context::initializeWriteNodePageStorageIfNeed(const PathPool & path_pool)
{
    auto lock = getLock();
    if (shared->storage_run_mode == PageStorageRunMode::UNI_PS)
    {
        RUNTIME_CHECK(shared->ps_write == nullptr);
        try
        {
            PageStorageConfig config;
            shared->ps_write = UniversalPageStorageService::create( //
                *this,
                "uni_write",
                path_pool.getPSDiskDelegatorGlobalMulti(PathPool::write_uni_path_prefix),
                config);
            LOG_INFO(shared->log, "initialized GlobalUniversalPageStorage(WriteNode)");
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }
    }
    else
    {
        shared->ps_write = nullptr;
    }
}

UniversalPageStoragePtr Context::getWriteNodePageStorage() const
{
    auto lock = getLock();
    if (shared->ps_write)
    {
        return shared->ps_write->getUniversalPageStorage();
    }
    else
    {
        LOG_WARNING(
            shared->log,
            "Calling getWriteNodePageStorage() without initialization, stack={}",
            StackTrace().toString());
        return nullptr;
    }
}

UniversalPageStoragePtr Context::tryGetWriteNodePageStorage() const
{
    auto lock = getLock();
    if (shared->ps_write)
        return shared->ps_write->getUniversalPageStorage();
    return nullptr;
}

bool Context::tryUploadAllDataToRemoteStore() const
{
    auto lock = getLock();
    if (shared->ctx_disagg->isDisaggregatedStorageMode() && shared->ps_write)
    {
        shared->ps_write->setUploadAllData();
        return true;
    }
    return false;
}

// In some unit tests, we may want to reinitialize WriteNodePageStorage multiple times to mock restart.
// And we need to release old one before creating new one.
// And we must do it explicitly. Because if we do it implicitly in `initializeWriteNodePageStorageIfNeed`, there is a potential deadlock here.
// Thread A:
//   Get lock on SharedContext -> call UniversalPageStorageService::shutdown -> remove background tasks -> try get rwlock on the task
// Thread B:
//   Get rwlock on task -> call a method on Context to get some object -> try to get lock on SharedContext
void Context::tryReleaseWriteNodePageStorageForTest()
{
    UniversalPageStorageServicePtr ps_write;
    {
        auto lock = getLock();
        if (shared->ps_write)
        {
            LOG_WARNING(shared->log, "Release GlobalUniversalPageStorage(WriteNode).");
            ps_write = shared->ps_write;
            shared->ps_write = nullptr;
        }
    }
    if (ps_write)
    {
        // call shutdown without lock
        ps_write->shutdown();
        ps_write = nullptr;
    }
}

SharedContextDisaggPtr Context::getSharedContextDisagg() const
{
    RUNTIME_CHECK(shared->ctx_disagg != nullptr); // We always initialize the shared context in createGlobal()
    return shared->ctx_disagg;
}

UInt16 Context::getTCPPort() const
{
    auto lock = getLock();

    auto & config = getConfigRef();
    return config.getInt("tcp_port");
}


void Context::initializeSystemLogs()
{
    auto lock = getLock();
    system_logs = std::make_shared<SystemLogs>();
}


QueryLog * Context::getQueryLog()
{
    auto lock = getLock();

    if (!system_logs)
        return nullptr;

    if (!system_logs->query_log)
    {
        if (shared->shutdown_called)
            throw Exception(
                "Logical error: query log should be destroyed before tables shutdown",
                ErrorCodes::LOGICAL_ERROR);

        if (!global_context)
            throw Exception("Logical error: no global context for query log", ErrorCodes::LOGICAL_ERROR);

        auto & config = getConfigRef();

        String database = config.getString("query_log.database", "system");
        String table = config.getString("query_log.table", "query_log");
        String partition_by = config.getString("query_log.partition_by", "toYYYYMM(event_date)");
        size_t flush_interval_milliseconds
            = config.getUInt64("query_log.flush_interval_milliseconds", DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS);

        String engine = "ENGINE = MergeTree PARTITION BY (" + partition_by
            + ") ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024";

        system_logs->query_log
            = std::make_unique<QueryLog>(*global_context, database, table, engine, flush_interval_milliseconds);
    }

    return system_logs->query_log.get();
}


BlockInputStreamPtr Context::getInputFormat(
    const String & name,
    ReadBuffer & buf,
    const Block & sample,
    size_t max_block_size) const
{
    return shared->format_factory.getInput(name, buf, sample, *this, max_block_size);
}

BlockOutputStreamPtr Context::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return shared->format_factory.getOutput(name, buf, sample, *this);
}


time_t Context::getUptimeSeconds() const
{
    auto lock = getLock();
    return shared->uptime_watch.elapsedSeconds();
}


void Context::setConfigReloadCallback(ConfigReloadCallback && callback)
{
    /// Is initialized at server startup, so lock isn't required. Otherwise use mutex.
    shared->config_reload_callback = std::move(callback);
}

void Context::reloadConfig() const
{
    /// Use mutex if callback may be changed after startup.
    if (!shared->config_reload_callback)
        throw Exception("Can't reload config beacuse config_reload_callback is not set.", ErrorCodes::LOGICAL_ERROR);

    shared->config_reload_callback();
}


void Context::shutdown()
{
    system_logs.reset();
    shared->shutdown();
}


Context::ApplicationType Context::getApplicationType() const
{
    return shared->application_type;
}

void Context::setApplicationType(ApplicationType type)
{
    /// Lock isn't required, you should set it at start
    shared->application_type = type;
}

void Context::setDefaultProfiles(const Poco::Util::AbstractConfiguration & config)
{
    shared->default_profile_name = config.getString("default_profile", "default");
    shared->system_profile_name = config.getString("system_profile", shared->default_profile_name);
    setSetting("profile", shared->system_profile_name);
    is_config_loaded = true;
}

String Context::getDefaultProfileName() const
{
    return shared->default_profile_name;
}

String Context::getSystemProfileName() const
{
    return shared->system_profile_name;
}

String Context::getFormatSchemaPath() const
{
    return shared->format_schema_path;
}

void Context::setFormatSchemaPath(const String & path)
{
    shared->format_schema_path = path;
}

SharedQueriesPtr Context::getSharedQueries()
{
    auto lock = getLock();

    if (!shared->shared_queries)
        shared->shared_queries = std::make_shared<SharedQueries>();
    return shared->shared_queries;
}

const std::shared_ptr<DB::DM::SharedBlockSchemas> & Context::getSharedBlockSchemas() const
{
    return shared->shared_block_schemas;
}

void Context::initializeSharedBlockSchemas(size_t shared_block_schemas_size)
{
    shared->shared_block_schemas = std::make_shared<DB::DM::SharedBlockSchemas>(shared_block_schemas_size);
}

size_t Context::getMaxStreams() const
{
    size_t max_streams = settings.max_threads;
    bool is_cop_request = false;
    if (dag_context != nullptr)
    {
        if (isExecutorTest() || isInterpreterTest())
            max_streams = dag_context->initialize_concurrency;
        else if (dag_context->isCop())
        {
            is_cop_request = true;
            max_streams = 1;
        }
    }
    if (max_streams == 0)
        max_streams = 1;
    if (unlikely(max_streams != 1 && is_cop_request))
        /// for cop request, the max_streams should be 1
        throw Exception("Cop request only support running with max_streams = 1");
    return max_streams;
}

bool Context::isMPPTest() const
{
    return test_mode == mpp_test || test_mode == cancel_test;
}

void Context::setMPPTest()
{
    test_mode = mpp_test;
}

bool Context::isCancelTest() const
{
    return test_mode == cancel_test;
}

void Context::setCancelTest()
{
    test_mode = cancel_test;
}

bool Context::isExecutorTest() const
{
    return test_mode == executor_test;
}

void Context::setExecutorTest()
{
    test_mode = executor_test;
}

bool Context::isInterpreterTest() const
{
    return test_mode == interpreter_test;
}

void Context::setInterpreterTest()
{
    test_mode = interpreter_test;
}

bool Context::isCopTest() const
{
    return test_mode == cop_test;
}

void Context::setCopTest()
{
    test_mode = cop_test;
}

bool Context::isTest() const
{
    return test_mode != non_test;
}

void Context::setMockStorage(MockStorage * mock_storage_)
{
    mock_storage = mock_storage_;
}

MockStorage * Context::mockStorage() const
{
    return mock_storage;
}

MockMPPServerInfo Context::mockMPPServerInfo() const
{
    return mpp_server_info;
}

void Context::setMockMPPServerInfo(MockMPPServerInfo & info)
{
    mpp_server_info = info;
}

const std::unordered_set<uint64_t> * Context::getStoreIdBlockList() const
{
    return &shared->store_id_blocklist;
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
bool Context::initializeStoreIdBlockList(const String & comma_sep_string)
{
#if SERVERLESS_PROXY == 1
    std::istringstream iss(comma_sep_string);
    std::string token;

    while (std::getline(iss, token, ','))
    {
        try
        {
            uint64_t number = std::stoull(token);
            shared->store_id_blocklist.insert(number);
        }
        catch (...)
        {
            // Keep empty
            LOG_INFO(DB::Logger::get(), "Error disagg_blocklist_wn_store_id setting, {}", comma_sep_string);
            shared->store_id_blocklist.clear();
            return false;
        }
    }

    if (!shared->store_id_blocklist.empty())
        LOG_DEBUG(
            DB::Logger::get(),
            "Blocklisted {} stores, which are {}",
            shared->store_id_blocklist.size(),
            comma_sep_string);

    return true;
#else
    UNUSED(comma_sep_string);
    return true;
#endif
}

SessionCleaner::~SessionCleaner()
{
    try
    {
        {
            std::lock_guard lock{mutex};
            quit = true;
        }

        cond.notify_one();

        thread.join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void SessionCleaner::run()
{
    setThreadName("SessionCleaner");

    std::unique_lock lock{mutex};

    while (true)
    {
        auto interval = context.closeSessions();

        if (cond.wait_for(lock, interval, [this]() -> bool { return quit; }))
            break;
    }
}

} // namespace DB
