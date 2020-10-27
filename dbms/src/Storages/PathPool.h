#pragma once

#include <Core/Types.h>
#include <Storages/Page/PageDefines.h>

#include <unordered_map>

namespace Poco
{
class Logger;
}

namespace DB
{

class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
class FileProvider;
using FileProviderPtr = std::shared_ptr<FileProvider>;

/// A class to manage global paths.
class PathPool;
/// A class to manage paths for the specified storage.
class StoragePathPool;

/// Delegators to StoragePathPool. They are used for managing the path for storing stable/delta/raft data.
class StableDelegator;
class PSPathDelegator;
using PSPathDelegatorPtr = std::shared_ptr<PSPathDelegator>;
class DeltaDelegator;
class NormalPathDelegator;
// TODO: support multi-paths for RaftDelegator
// using RaftDelegator = NormalPathDelegator;


/// A class to manage global paths.
class PathPool
{
public:
    PathPool() = default;

    // Constructor to be used during initialization
    PathPool(const Strings & main_data_paths, const Strings & latest_data_paths, PathCapacityMetricsPtr global_capacity_,
        FileProviderPtr file_provider_);

    // Constructor to create PathPool for one Storage
    StoragePathPool withTable(const String & database_, const String & table_, bool path_need_database_name_) const;

public:
    /// Methods for the root PathPool ///
    std::vector<String> listPaths() const;

private:
    Strings main_data_paths;
    Strings latest_data_paths;

    PathCapacityMetricsPtr global_capacity;

    FileProviderPtr file_provider;

    Poco::Logger * log;
};

class StableDelegator : private boost::noncopyable
{
public:
    StableDelegator(StoragePathPool & pool_) : pool(pool_) {}

    Strings listPaths() const;

    String choosePath() const;

    String getDTFilePath(UInt64 file_id) const;

    void addDTFile(UInt64 file_id, size_t file_size, std::string_view path);

    void removeDTFile(UInt64 file_id);

private:
    StoragePathPool & pool;
};

class PSPathDelegator : private boost::noncopyable
{
public:
    PSPathDelegator() {}
    virtual ~PSPathDelegator() {}

    virtual size_t numPaths() const = 0;

    virtual String normalPath() const = 0;

    virtual Strings listPaths() const = 0;

    virtual String choosePath(const PageFileIdAndLevel & id_lvl) = 0;

    virtual size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl, size_t size_to_add, const String & pf_parent_path, bool need_insert_location)
        = 0;

    virtual String getPageFilePath(const PageFileIdAndLevel & id_lvl) const = 0;

    virtual void removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size) = 0;
};

class DeltaDelegator : public PSPathDelegator
{
public:
    DeltaDelegator(StoragePathPool & pool_) : pool(pool_) {}

    size_t numPaths() const override;

    String normalPath() const override;

    Strings listPaths() const override;

    String choosePath(const PageFileIdAndLevel & id_lvl) override;

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl, size_t size_to_add, const String & pf_parent_path, bool need_insert_location) override;

    String getPageFilePath(const PageFileIdAndLevel & id_lvl) const override;

    void removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size) override;

private:
    StoragePathPool & pool;
};

class NormalPathDelegator : public PSPathDelegator
{
public:
    NormalPathDelegator(StoragePathPool & pool_, String prefix) : pool(pool_), path_prefix(std::move(prefix)) {}

    size_t numPaths() const override;

    String normalPath() const override;

    Strings listPaths() const override;

    String choosePath(const PageFileIdAndLevel & id_lvl) override;

    size_t addPageFileUsedSize(
        const PageFileIdAndLevel & id_lvl, size_t size_to_add, const String & pf_parent_path, bool need_insert_location) override;

    String getPageFilePath(const PageFileIdAndLevel & id_lvl) const override;

    void removePageFile(const PageFileIdAndLevel & id_lvl, size_t file_size) override;

private:
    StoragePathPool & pool;
    const String path_prefix;
};

/// A class to manage paths for the specified storage.
class StoragePathPool
{
public:
    static constexpr const char * STABLE_FOLDER_NAME = "stable";
    static constexpr const char * DELTA_FOLDER_NAME = "log";

    StoragePathPool(const Strings & main_data_paths, const Strings & latest_data_paths, //
        String database_, String table_, bool path_need_database_name_,                 //
        PathCapacityMetricsPtr global_capacity_, FileProviderPtr file_provider_);

    StoragePathPool(const StoragePathPool & rhs);
    StoragePathPool & operator=(const StoragePathPool & rhs);

    // Generate a lightweight delegator for managing stable data, such as choosing path for DTFile or getting DTFile path by ID and so on.
    // Those paths are generate from `main_path_infos` and `STABLE_FOLDER_NAME`
    StableDelegator getStableDelegate() { return StableDelegator(*this); }

    // Generate a lightweight delegator for managing data in `StoragePool`.
    // Those paths are generate from `latest_path_infos` and `DELTA_FOLDER_NAME`
    PSPathDelegatorPtr getDeltaDelegate() { return std::make_shared<DeltaDelegator>(*this); }

    // Generate a lightweight delegator for managing data in `StoragePool`.
    // Those paths are generate from the first path of `latest_path_infos` and `prefix`
    PSPathDelegatorPtr getNormalDelegate(const String & prefix) { return std::make_shared<NormalPathDelegator>(*this, prefix); }

    void rename(const String & new_database, const String & new_table, bool clean_rename);

    void drop(bool recursive, bool must_success = true);

private:
    String getStorePath(const String & extra_path_root, const String & database_name, const String & table_name);

    void renamePath(const String & old_path, const String & new_path);

private:
    using DMFilePathMap = std::unordered_map<UInt64, UInt32>;
    struct PageFileIdLvlHasher
    {
        std::size_t operator()(const PageFileIdAndLevel & id_lvl) const
        {
            return std::hash<PageFileId>()(id_lvl.first) ^ std::hash<PageFileLevel>()(id_lvl.second);
        }
    };
    using PageFilePathMap = std::unordered_map<PageFileIdAndLevel, UInt32, PageFileIdLvlHasher>;
    struct MainPathInfo
    {
        String path;
        size_t total_size; // total used bytes
        std::unordered_map<UInt64, size_t> file_size_map;
    };
    using MainPathInfos = std::vector<MainPathInfo>;
    struct LatestPathInfo
    {
        String path;
        size_t total_size; // total used bytes
    };
    using LatestPathInfos = std::vector<LatestPathInfo>;

    friend class StableDelegator;
    friend class DeltaDelegator;
    friend class NormalPathDelegator;
    friend class RaftDelegator;

private:
    // Path, size
    MainPathInfos main_path_infos;
    LatestPathInfos latest_path_infos;
    // DMFileID -> path index
    DMFilePathMap dt_file_path_map;
    // PageFileID -> path index
    PageFilePathMap page_path_map;

    String database;
    String table;

    mutable std::mutex mutex;

    bool path_need_database_name = false;

    PathCapacityMetricsPtr global_capacity;

    FileProviderPtr file_provider;

    Poco::Logger * log;
};

} // namespace DB
