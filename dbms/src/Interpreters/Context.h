#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <IO/CompressionSettings.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Settings.h>
#include <Interpreters/TimezoneInfo.h>
#include <Storages/PartPathSelector.h>
#include <common/MultiVersion.h>
#include <grpc++/grpc++.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>

namespace pingcap
{
struct ClusterConfig;
}

namespace Poco
{
namespace Net
{
class IPAddress;
}
} // namespace Poco

namespace DB
{
struct ContextShared;
class IRuntimeComponentsFactory;
class QuotaForIntervals;
class EmbeddedDictionaries;
class ExternalDictionaries;
class ExternalModels;
class InterserverIOHandler;
class BackgroundProcessingPool;
class MergeList;
class Cluster;
class Compiler;
class MarkCache;
class UncompressedCache;
class PersistedCache;
class DBGInvoker;
class TMTContext;
using TMTContextPtr = std::shared_ptr<TMTContext>;
class ProcessList;
class ProcessListElement;
class Macros;
struct Progress;
class Clusters;
class QueryLog;
class PartLog;
struct MergeTreeSettings;
class IDatabase;
class DDLGuard;
class IStorage;
class ITableFunction;
using StoragePtr = std::shared_ptr<IStorage>;
using Tables = std::map<String, StoragePtr>;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class IBlockInputStream;
class IBlockOutputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;
class Block;
struct SystemLogs;
using SystemLogsPtr = std::shared_ptr<SystemLogs>;
class SharedQueries;
using SharedQueriesPtr = std::shared_ptr<SharedQueries>;
class TiDBService;
using TiDBServicePtr = std::shared_ptr<TiDBService>;
class SchemaSyncService;
using SchemaSyncServicePtr = std::shared_ptr<SchemaSyncService>;
class PathPool;
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
class KeyManager;
using KeyManagerPtr = std::shared_ptr<KeyManager>;
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;
struct TiFlashRaftConfig;
class DAGContext;
class IORateLimiter;
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;

namespace DM
{
class MinMaxIndexCache;
class DeltaIndexManager;
} // namespace DM

/// (database name, table name)
using DatabaseAndTableName = std::pair<String, String>;

/// Table -> set of table-views that make SELECT from it.
using ViewDependencies = std::map<DatabaseAndTableName, std::set<DatabaseAndTableName>>;
using Dependencies = std::vector<DatabaseAndTableName>;

using TableAndCreateAST = std::pair<StoragePtr, ASTPtr>;
using TableAndCreateASTs = std::map<String, TableAndCreateAST>;

/** A set of known objects that can be used in the query.
  * Consists of a shared part (always common to all sessions and queries)
  *  and copied part (which can be its own for each session or query).
  *
  * Everything is encapsulated for all sorts of checks and locks.
  */
class Context
{
private:
    using Shared = std::shared_ptr<ContextShared>;
    Shared shared;

    std::shared_ptr<IRuntimeComponentsFactory> runtime_components_factory;

    ClientInfo client_info;

    std::shared_ptr<QuotaForIntervals> quota; /// Current quota. By default - empty quota, that have no limits.
    String current_database;
    Settings settings; /// Setting for query execution.
    using ProgressCallback = std::function<void(const Progress & progress)>;
    ProgressCallback progress_callback; /// Callback for tracking progress of query execution.
    ProcessListElement * process_list_elem = nullptr; /// For tracking total resource usage for query.
    /// Format, used when server formats data by itself and if query does not have FORMAT specification.
    /// Thus, used in HTTP interface. If not specified - then some globally default format is used.
    String default_format;
    TableAndCreateASTs external_tables; /// Temporary tables.
    Tables table_function_results; /// Temporary tables obtained by execution of table functions. Keyed by AST tree id.
    Context * query_context = nullptr;
    Context * session_context = nullptr; /// Session context or nullptr. Could be equal to this.
    Context * global_context = nullptr; /// Global context or nullptr. Could be equal to this.
    SystemLogsPtr system_logs; /// Used to log queries and operations on parts

    UInt64 session_close_cycle = 0;
    bool session_is_used = false;

    bool use_l0_opt = true;

    TimezoneInfo timezone_info;

    DAGContext * dag_context = nullptr;

    using DatabasePtr = std::shared_ptr<IDatabase>;
    using Databases = std::map<String, std::shared_ptr<IDatabase>>;

    /// Use copy constructor or createGlobal() instead
    Context();

public:
    /// Create initial Context with ContextShared and etc.
    static Context createGlobal(std::shared_ptr<IRuntimeComponentsFactory> runtime_components_factory);
    static Context createGlobal();

    ~Context();

    String getPath() const;
    String getTemporaryPath() const;
    String getFlagsPath() const;
    String getUserFilesPath() const;
    PathPool & getPathPool() const;

    void setPath(const String & path);
    void setTemporaryPath(const String & path);
    void setFlagsPath(const String & path);
    void setUserFilesPath(const String & path);

    void setPathPool(const Strings & main_data_paths,
                     const Strings & latest_data_paths,
                     const Strings & kvstore_paths,
                     bool enable_raft_compatible_mode,
                     PathCapacityMetricsPtr global_capacity_,
                     FileProviderPtr file_provider);

    using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

    /// Global application configuration settings.
    void setConfig(const ConfigurationPtr & config);
    Poco::Util::AbstractConfiguration & getConfigRef() const;

    /** Take the list of users, quotas and configuration profiles from this config.
      * The list of users is completely replaced.
      * The accumulated quota values are not reset if the quota is not deleted.
      */
    void setUsersConfig(const ConfigurationPtr & config);
    ConfigurationPtr getUsersConfig();

    /// Must be called before getClientInfo.
    void setUser(const String & name, const String & password, const Poco::Net::SocketAddress & address, const String & quota_key);
    /// Compute and set actual user settings, client_info.current_user should be set
    void calculateUserSettings();

    ClientInfo & getClientInfo() { return client_info; };
    const ClientInfo & getClientInfo() const { return client_info; };

    void setQuota(const String & name, const String & quota_key, const String & user_name, const Poco::Net::IPAddress & address);
    QuotaForIntervals & getQuota();

    void addDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where);
    void removeDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where);
    Dependencies getDependencies(const String & database_name, const String & table_name) const;

    /// Checking the existence of the table/database. Database can be empty - in this case the current database is used.
    bool isTableExist(const String & database_name, const String & table_name) const;
    bool isDatabaseExist(const String & database_name) const;
    bool isExternalTableExist(const String & table_name) const;
    void assertTableExists(const String & database_name, const String & table_name) const;

    /** The parameter check_database_access_rights exists to not check the permissions of the database again,
      * when assertTableDoesntExist or assertDatabaseExists is called inside another function that already
      * made this check.
      */
    void assertTableDoesntExist(const String & database_name, const String & table_name, bool check_database_acccess_rights = true) const;
    void assertDatabaseExists(const String & database_name, bool check_database_acccess_rights = true) const;

    void assertDatabaseDoesntExist(const String & database_name) const;
    void checkDatabaseAccessRights(const std::string & database_name) const;

    Tables getExternalTables() const;
    StoragePtr tryGetExternalTable(const String & table_name) const;
    StoragePtr getTable(const String & database_name, const String & table_name) const;
    StoragePtr tryGetTable(const String & database_name, const String & table_name) const;
    void addExternalTable(const String & table_name, const StoragePtr & storage, const ASTPtr & ast = {});
    StoragePtr tryRemoveExternalTable(const String & table_name);

    StoragePtr executeTableFunction(const ASTPtr & table_expression);

    void addDatabase(const String & database_name, const DatabasePtr & database);
    DatabasePtr detachDatabase(const String & database_name);

    /// Get an object that protects the table from concurrently executing multiple DDL operations.
    /// If such an object already exists, an exception is thrown.
    std::unique_ptr<DDLGuard> getDDLGuard(const String & database, const String & table, const String & message) const;
    /// If the table already exists, it returns nullptr, otherwise guard is created.
    std::unique_ptr<DDLGuard> getDDLGuardIfTableDoesntExist(const String & database, const String & table, const String & message) const;

    String getCurrentDatabase() const;
    String getCurrentQueryId() const;
    void setCurrentDatabase(const String & name);
    void setCurrentQueryId(const String & query_id);

    String getDefaultFormat() const; /// If default_format is not specified, some global default format is returned.
    void setDefaultFormat(const String & name);

    MultiVersion<Macros>::Version getMacros() const;
    void setMacros(std::unique_ptr<Macros> && macros);

    Settings getSettings() const;
    void setSettings(const Settings & settings_);

    /// Set a setting by name.
    void setSetting(const String & name, const Field & value);

    /// Set a setting by name. Read the value in text form from a string (for example, from a config, or from a URL parameter).
    void setSetting(const String & name, const std::string & value);

    const EmbeddedDictionaries & getEmbeddedDictionaries() const;
    const ExternalDictionaries & getExternalDictionaries() const;
    const ExternalModels & getExternalModels() const;
    EmbeddedDictionaries & getEmbeddedDictionaries();
    ExternalDictionaries & getExternalDictionaries();
    ExternalModels & getExternalModels();
    void tryCreateEmbeddedDictionaries() const;
    void tryCreateExternalDictionaries() const;
    void tryCreateExternalModels() const;

    /// I/O formats.
    BlockInputStreamPtr getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, size_t max_block_size) const;
    BlockOutputStreamPtr getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample) const;

    InterserverIOHandler & getInterserverIOHandler();

    /// How other servers can access this for downloading replicated data.
    void setInterserverIOAddress(const String & host, UInt16 port);
    std::pair<String, UInt16> getInterserverIOAddress() const;
    /// The port that the server listens for executing SQL queries.
    UInt16 getTCPPort() const;

    /// Get query for the CREATE table.
    ASTPtr getCreateTableQuery(const String & database_name, const String & table_name) const;
    ASTPtr getCreateExternalTableQuery(const String & table_name) const;
    ASTPtr getCreateDatabaseQuery(const String & database_name) const;

    const DatabasePtr getDatabase(const String & database_name) const;
    DatabasePtr getDatabase(const String & database_name);
    const DatabasePtr tryGetDatabase(const String & database_name) const;
    DatabasePtr tryGetDatabase(const String & database_name);

    const Databases getDatabases() const;
    Databases getDatabases();

    std::shared_ptr<Context> acquireSession(
        const String & session_id,
        std::chrono::steady_clock::duration timeout,
        bool session_check) const;
    void releaseSession(const String & session_id, std::chrono::steady_clock::duration timeout);

    /// Close sessions, that has been expired. Returns how long to wait for next session to be expired, if no new sessions will be added.
    std::chrono::steady_clock::duration closeSessions() const;

    /// For methods below you may need to acquire a lock by yourself.
    std::unique_lock<std::recursive_mutex> getLock() const;

    const Context & getQueryContext() const;
    Context & getQueryContext();
    bool hasQueryContext() const { return query_context != nullptr; }
    const Context & getSessionContext() const;
    Context & getSessionContext();
    bool hasSessionContext() const { return session_context != nullptr; }

    const Context & getGlobalContext() const;
    Context & getGlobalContext();
    bool hasGlobalContext() const { return global_context != nullptr; }

    void setQueryContext(Context & context_) { query_context = &context_; }
    void setSessionContext(Context & context_) { session_context = &context_; }
    void setGlobalContext(Context & context_) { global_context = &context_; }
    const Settings & getSettingsRef() const { return settings; };
    Settings & getSettingsRef() { return settings; };


    void setProgressCallback(ProgressCallback callback);
    /// Used in InterpreterSelectQuery to pass it to the IProfilingBlockInputStream.
    ProgressCallback getProgressCallback() const;

    /** Set in executeQuery and InterpreterSelectQuery. Then it is used in IProfilingBlockInputStream,
      *  to update and monitor information about the total number of resources spent for the query.
      */
    void setProcessListElement(ProcessListElement * elem);
    /// Can return nullptr if the query was not inserted into the ProcessList.
    ProcessListElement * getProcessListElement() const;

    void setDAGContext(DAGContext * dag_context);
    DAGContext * getDAGContext() const;

    /// List all queries.
    ProcessList & getProcessList();
    const ProcessList & getProcessList() const;

    MergeList & getMergeList();
    const MergeList & getMergeList() const;

    /// Create a cache of uncompressed blocks of specified size. This can be done only once.
    void setUncompressedCache(size_t max_size_in_bytes);
    std::shared_ptr<UncompressedCache> getUncompressedCache() const;
    void dropUncompressedCache() const;

    /// Create a persisted cache written in fast(er) disk device.
    void setPersistedCache(size_t max_size_in_bytes, const std::string & persisted_path);
    std::shared_ptr<PersistedCache> getPersistedCache() const;

    /// Execute inner functions, debug only.
    DBGInvoker & getDBGInvoker() const;

    TMTContext & getTMTContext() const;

    /// Create a cache of marks of specified size. This can be done only once.
    void setMarkCache(size_t cache_size_in_bytes);
    std::shared_ptr<MarkCache> getMarkCache() const;
    void dropMarkCache() const;

    void setMinMaxIndexCache(size_t cache_size_in_bytes);
    std::shared_ptr<DM::MinMaxIndexCache> getMinMaxIndexCache() const;
    void dropMinMaxIndexCache() const;

    bool isDeltaIndexLimited() const;
    void setDeltaIndexManager(size_t cache_size_in_bytes);
    std::shared_ptr<DM::DeltaIndexManager> getDeltaIndexManager() const;

    /** Clear the caches of the uncompressed blocks and marks.
      * This is usually done when renaming tables, changing the type of columns, deleting a table.
      *  - since caches are linked to file names, and become incorrect.
      *  (when deleting a table - it is necessary, since in its place another can appear)
      * const - because the change in the cache is not considered significant.
      */
    void dropCaches() const;

    void setUseL0Opt(bool use_l0_opt);
    bool useL0Opt() const;

    BackgroundProcessingPool & getBackgroundPool();
    BackgroundProcessingPool & getBlockableBackgroundPool();

    void createTMTContext(const TiFlashRaftConfig & raft_config, pingcap::ClusterConfig && cluster_config);

    void initializeSchemaSyncService();
    SchemaSyncServicePtr & getSchemaSyncService();

    void initializePathCapacityMetric(
        size_t global_capacity_quota,
        const Strings & main_data_paths,
        const std::vector<size_t> & main_capacity_quota,
        const Strings & latest_data_paths,
        const std::vector<size_t> & latest_capacity_quota);
    PathCapacityMetricsPtr getPathCapacity() const;

    void initializePartPathSelector(std::vector<std::string> && all_path, std::vector<std::string> && all_fast_path);
    PartPathSelector & getPartPathSelector();

    void initializeTiFlashMetrics();

    void initializeFileProvider(KeyManagerPtr key_manager, bool enable_encryption);
    FileProviderPtr getFileProvider() const;

    void initializeRateLimiter(Poco::Util::AbstractConfiguration & config);
    WriteLimiterPtr getWriteLimiter() const;
    ReadLimiterPtr getReadLimiter() const;
    IORateLimiter & getIORateLimiter() const;

    Clusters & getClusters() const;
    std::shared_ptr<Cluster> getCluster(const std::string & cluster_name) const;
    std::shared_ptr<Cluster> tryGetCluster(const std::string & cluster_name) const;
    void setClustersConfig(const ConfigurationPtr & config, const String & config_name = "remote_servers");
    /// Sets custom cluster, but doesn't update configuration
    void setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster);
    void reloadClusterConfig();

    Compiler & getCompiler();

    /// Call after initialization before using system logs. Call for global context.
    void initializeSystemLogs();

    /// Nullptr if the query log is not ready for this moment.
    QueryLog * getQueryLog();

    /// Returns an object used to log opertaions with parts if it possible.
    /// Provide table name to make required cheks.
    PartLog * getPartLog(const String & part_database);

    const MergeTreeSettings & getMergeTreeSettings();

    /// Prevents DROP TABLE if its size is greater than max_size (50GB by default, max_size=0 turn off this check)
    void setMaxTableSizeToDrop(size_t max_size);
    void checkTableCanBeDropped(const String & database, const String & table, size_t table_size);

    /// Lets you select the compression settings according to the conditions described in the configuration file.
    CompressionSettings chooseCompressionSettings(size_t part_size, double part_size_ratio) const;

    /// Get the server uptime in seconds.
    time_t getUptimeSeconds() const;

    using ConfigReloadCallback = std::function<void()>;
    void setConfigReloadCallback(ConfigReloadCallback && callback);
    void reloadConfig() const;

    void shutdown();

    enum class ApplicationType
    {
        SERVER, /// The program is run as clickhouse-server daemon (default behavior)
        CLIENT, /// clickhouse-client
        LOCAL /// clickhouse-local
    };

    ApplicationType getApplicationType() const;
    void setApplicationType(ApplicationType type);

    /// Sets default_profile and system_profile, must be called once during the initialization
    void setDefaultProfiles(const Poco::Util::AbstractConfiguration & config);
    String getDefaultProfileName() const;
    String getSystemProfileName() const;

    /// Base path for format schemas
    String getFormatSchemaPath() const;
    void setFormatSchemaPath(const String & path);

    SharedQueriesPtr getSharedQueries();

    const TimezoneInfo & getTimezoneInfo() const { return timezone_info; };
    TimezoneInfo & getTimezoneInfo() { return timezone_info; };

    /// User name and session identifier. Named sessions are local to users.
    using SessionKey = std::pair<String, String>;

    void reloadDeltaTreeConfig(const Poco::Util::AbstractConfiguration & config);

private:
    /** Check if the current client has access to the specified database.
      * If access is denied, throw an exception.
      * NOTE: This method should always be called when the `shared->mutex` mutex is acquired.
      */
    void checkDatabaseAccessRightsImpl(const std::string & database_name) const;

    EmbeddedDictionaries & getEmbeddedDictionariesImpl(bool throw_on_error) const;
    ExternalDictionaries & getExternalDictionariesImpl(bool throw_on_error) const;
    ExternalModels & getExternalModelsImpl(bool throw_on_error) const;

    StoragePtr getTableImpl(const String & database_name, const String & table_name, Exception * exception) const;

    SessionKey getSessionKey(const String & session_id) const;

    /// Session will be closed after specified timeout.
    void scheduleCloseSession(const SessionKey & key, std::chrono::steady_clock::duration timeout);
};


/// Puts an element into the map, erases it in the destructor.
/// If the element already exists in the map, throws an exception containing provided message.
class DDLGuard
{
public:
    /// Element name -> message.
    /// NOTE: using std::map here (and not std::unordered_map) to avoid iterator invalidation on insertion.
    using Map = std::map<String, String>;

    DDLGuard(Map & map_, std::mutex & mutex_, std::unique_lock<std::mutex> && lock, const String & elem, const String & message);
    ~DDLGuard();

private:
    Map & map;
    Map::iterator it;
    std::mutex & mutex;
};


class SessionCleaner
{
public:
    SessionCleaner(Context & context_)
        : context{context_}
    {}
    ~SessionCleaner();

private:
    void run();

    Context & context;

    std::mutex mutex;
    std::condition_variable cond;
    std::atomic<bool> quit{false};
    std::thread thread{&SessionCleaner::run, this};
};

} // namespace DB
